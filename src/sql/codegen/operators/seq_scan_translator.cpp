#include "sql/codegen/operators/seq_scan_translator.h"

#include "common/exception.h"
#include "sql/catalog.h"
#include "sql/codegen/codegen.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/consumer_context.h"
#include "sql/codegen/function_builder.h"
#include "sql/codegen/if.h"
#include "sql/codegen/loop.h"
#include "sql/codegen/pipeline.h"
#include "sql/planner/expressions/column_value_expression.h"
#include "sql/planner/expressions/comparison_expression.h"
#include "sql/planner/expressions/conjunction_expression.h"
#include "sql/planner/expressions/expression_util.h"
#include "sql/planner/plannodes/seq_scan_plan_node.h"
#include "sql/table.h"

namespace tpl::sql::codegen {

SeqScanTranslator::SeqScanTranslator(const planner::SeqScanPlanNode &plan,
                                     CompilationContext *compilation_context, Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline), vpi_(codegen_, "vpi") {
  pipeline->RegisterSource(this, Pipeline::Parallelism::Parallel);
  if (HasPredicate()) compilation_context->Prepare(*plan.GetScanPredicate());
}

bool SeqScanTranslator::HasPredicate() const {
  return GetPlanAs<planner::SeqScanPlanNode>().GetScanPredicate() != nullptr;
}

std::string_view SeqScanTranslator::GetTableName() const {
  const auto table_oid = GetPlanAs<planner::SeqScanPlanNode>().GetTableOid();
  return Catalog::Instance()->LookupTableById(table_oid)->GetName();
}

void SeqScanTranslator::GenerateGenericTerm(
    FunctionBuilder *function, const planner::AbstractExpression *term,
    const edsl::Value<ast::x::VectorProjection *> &vector_proj,
    const edsl::Value<ast::x::TupleIdList *> &tid_list) {
  edsl::Variable<ast::x::VectorProjectionIterator> vpi_base(codegen_, "vpi_base");

  function->Append(edsl::Declare(vpi_base));
  function->Append(edsl::Declare(vpi_, vpi_base.Addr()));
  function->Append(vpi_->Init(vector_proj, tid_list));

  Loop vpi_loop(function, nullptr, vpi_->HasNext(), vpi_->Advance());
  {
    PipelineContext pipeline_context(*GetPipeline());
    ConsumerContext context(GetCompilationContext(), pipeline_context);
    auto cond_translator = GetCompilationContext()->LookupTranslator(*term);
    auto match_gen = cond_translator->DeriveValue(&context, this);
    auto match = match_gen.IsSQLType() ? edsl::ForceTruth(match_gen) : edsl::Value<bool>(match_gen);
    function->Append(vpi_->Match(match));
  }
  vpi_loop.EndLoop();
}

void SeqScanTranslator::GenerateFilterClauseFunctions(const planner::AbstractExpression *predicate,
                                                      std::vector<ast::Identifier> *curr_clause,
                                                      bool seen_conjunction) {
  // The top-most disjunctions in the tree form separate clauses in the filter manager.
  if (!seen_conjunction && predicate->Is<planner::ExpressionType::CONJUNCTION>()) {
    auto conj = static_cast<const planner::ConjunctionExpression *>(predicate);
    if (conj->GetKind() == planner::ConjunctionKind::OR) {
      std::vector<ast::Identifier> next_clause;
      GenerateFilterClauseFunctions(predicate->GetChild(0), &next_clause, false);
      filters_.emplace_back(std::move(next_clause));
      GenerateFilterClauseFunctions(predicate->GetChild(1), curr_clause, false);
      return;
    }
  }

  // Consecutive conjunctions are part of the same clause.
  if (predicate->Is<planner::ExpressionType::CONJUNCTION>()) {
    auto conj = static_cast<const planner::ConjunctionExpression *>(predicate);
    if (conj->GetKind() == planner::ConjunctionKind::AND) {
      GenerateFilterClauseFunctions(predicate->GetChild(0), curr_clause, true);
      GenerateFilterClauseFunctions(predicate->GetChild(1), curr_clause, true);
      return;
    }
  }

  // At this point, we create a term.
  // Signature: (vp: *VectorProjection, tids: *TupleIdList, ctx: *uint8) -> nil

  auto fn_name =
      codegen_->MakeFreshIdentifier(GetPipeline()->CreatePipelineFunctionName("FilterClause"));

  std::vector<FunctionBuilder::Param> params = {
      {codegen_->MakeIdentifier("vp"), codegen_->GetType<ast::x::VectorProjection *>()},
      {codegen_->MakeIdentifier("tids"), codegen_->GetType<ast::x::TupleIdList *>()},
      {codegen_->MakeIdentifier("ctx"), codegen_->GetType<uint8_t *>()},
  };

  FunctionBuilder builder(codegen_, fn_name, std::move(params), codegen_->GetType<void>());
  {
    edsl::Value<ast::x::VectorProjection *> vector_proj = builder.GetParameterByPosition(0);
    edsl::Value<ast::x::TupleIdList *> tid_list = builder.GetParameterByPosition(1);
    if (planner::ExpressionUtil::IsColumnCompareWithConst(*predicate) &&
        !planner::ExpressionUtil::IsLikeComparison(*predicate)) {
      auto cmp = static_cast<const planner::ComparisonExpression *>(predicate);
      auto cve = static_cast<const planner::ColumnValueExpression *>(predicate->GetChild(0));
      auto translator = GetCompilationContext()->LookupTranslator(*predicate->GetChild(1));
      auto const_val = translator->DeriveValue(nullptr, nullptr);
      builder.Append(edsl::VPIFilter(vector_proj,          // Vector projection.
                                     cmp->GetKind(),       // Comparison type.
                                     cve->GetColumnOid(),  // Column index.
                                     const_val,            // Constant value.
                                     tid_list));           // TID list.
    } else if (planner::ExpressionUtil::IsConstCompareWithColumn(*predicate)) {
      throw NotImplementedException("const <op> col vector filter comparison not implemented");
    } else {
      // If we ever reach this point, the current node in the expression tree
      // violates strict DNF. Its subtree is treated as a generic,
      // non-vectorized filter.
      GenerateGenericTerm(&builder, predicate, vector_proj, tid_list);
    }
  }
  builder.Finish();
  curr_clause->push_back(fn_name);
}

void SeqScanTranslator::DeclarePipelineState(PipelineContext *pipeline_ctx) {
  if (HasPredicate()) {
    local_filter_ = pipeline_ctx->DeclarePipelineStateEntry(
        "filter_mgr", codegen_->GetType<ast::x::FilterManager>());
  }
}

void SeqScanTranslator::DefinePipelineFunctions(UNUSED const PipelineContext &pipeline_ctx) {
  if (HasPredicate()) {
    std::vector<ast::Identifier> curr_clause;
    auto root_expr = GetPlanAs<planner::SeqScanPlanNode>().GetScanPredicate();
    GenerateFilterClauseFunctions(root_expr, &curr_clause, false);
    filters_.emplace_back(std::move(curr_clause));
  }
}

void SeqScanTranslator::ScanTable(ConsumerContext *context, FunctionBuilder *function,
                                  const edsl::Value<ast::x::TableVectorIterator *> &tvi) const {
  Loop tvi_loop(function, tvi->Advance());
  {
    // var vpi = @tableIterGetVPI()
    function->Append(edsl::Declare(vpi_, tvi->GetVPI()));

    if (HasPredicate()) {
      auto fm = context->GetStateEntryPtr<ast::x::FilterManager>(local_filter_);
      function->Append(fm->RunFilters(vpi_));
    }

    if (context->IsVectorized()) {
      // Push the (potentially filtered) VPI directly to consumer.
      context->Consume(function);
    } else {
      Loop vpi_loop(function, nullptr, vpi_->HasNext(), vpi_->Advance());
      {  // Tuple-at-a-time scan over the VPI.
        context->Consume(function);
      }
      vpi_loop.EndLoop();
    }
  }
  tvi_loop.EndLoop();
}

void SeqScanTranslator::InitializePipelineState(const PipelineContext &pipeline_ctx,
                                                FunctionBuilder *function) const {
  if (HasPredicate()) {
    // @filterManagerInit()
    auto fm = pipeline_ctx.GetStateEntryPtr<ast::x::FilterManager>(local_filter_);
    function->Append(fm->Init());
    // @filterManagerInsert() for each clause.
    for (const auto &clause : filters_) {
      auto fm = pipeline_ctx.GetStateEntryPtr<ast::x::FilterManager>(local_filter_);
      function->Append(fm->Insert(clause));
    }
  }
}

void SeqScanTranslator::TearDownPipelineState(const PipelineContext &pipeline_ctx,
                                              FunctionBuilder *function) const {
  if (HasPredicate()) {
    auto fm = pipeline_ctx.GetStateEntryPtr<ast::x::FilterManager>(local_filter_);
    function->Append(fm->Free());
  }
}

void SeqScanTranslator::Consume(ConsumerContext *context, FunctionBuilder *function) const {
  if (!GetPipeline()->IsParallel() || !GetPipeline()->IsDriver(this)) {
    // A serial pipeline, or one that we aren't driving. Declare TVI.
    edsl::Variable<ast::x::TableVectorIterator> tvi_base(codegen_, "tvi_base");
    edsl::Variable<ast::x::TableVectorIterator *> tvi(codegen_, "tvi");
    function->Append(edsl::Declare(tvi_base));
    function->Append(edsl::Declare(tvi, tvi_base.Addr()));
    function->Append(tvi->Init(GetTableName()));

    // Scan the table.
    ScanTable(context, function, tvi);

    // Close the TVI.
    function->Append(tvi->Close());
    return;
  }

  // This is a parallel pipeline, a TVI is provided as a function argument.
  edsl::Value<ast::x::TableVectorIterator *> tvi(
      function->GetParameterByPosition(function->GetParameterCount() - 1));

  // Scan it.
  ScanTable(context, function, tvi);
}

edsl::ValueVT SeqScanTranslator::GetTableColumn(uint16_t col_oid) const {
  const auto table_oid = GetPlanAs<planner::SeqScanPlanNode>().GetTableOid();
  const auto schema = &Catalog::Instance()->LookupTableById(table_oid)->GetSchema();
  auto type = schema->GetColumnInfo(col_oid)->type.GetPrimitiveTypeId();
  auto nullable = schema->GetColumnInfo(col_oid)->type.IsNullable();
  return vpi_->Get(type, nullable, col_oid);
}

void SeqScanTranslator::DrivePipeline(const PipelineContext &pipeline_ctx) const {
  TPL_ASSERT(pipeline_ctx.IsForPipeline(*GetPipeline()), "Table scan driving unknown pipeline!");
  if (pipeline_ctx.IsParallel()) {
    const auto dispatch = [&](FunctionBuilder *function, ast::Identifier work_func) {
      function->Append(edsl::IterateTableParallel(GetTableName(), GetQueryStatePtr(),
                                                  GetThreadStateContainer(), work_func));
    };
    std::vector<FunctionBuilder::Param> params = {
        {codegen_->MakeFreshIdentifier("tvi"), codegen_->GetType<ast::x::TableVectorIterator *>()},
    };
    GetPipeline()->LaunchParallel(pipeline_ctx, dispatch, std::move(params));
  } else {
    GetPipeline()->LaunchSerial(pipeline_ctx);
  }
}

}  // namespace tpl::sql::codegen
