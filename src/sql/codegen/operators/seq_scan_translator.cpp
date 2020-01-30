#include "sql/codegen/operators/seq_scan_translator.h"

#include "common/exception.h"
#include "sql/catalog.h"
#include "sql/codegen/codegen.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/function_builder.h"
#include "sql/codegen/if.h"
#include "sql/codegen/loop.h"
#include "sql/codegen/pipeline.h"
#include "sql/codegen/work_context.h"
#include "sql/planner/expressions/column_value_expression.h"
#include "sql/planner/expressions/expression_util.h"
#include "sql/planner/plannodes/seq_scan_plan_node.h"
#include "sql/table.h"

namespace tpl::sql::codegen {

SeqScanTranslator::SeqScanTranslator(const planner::SeqScanPlanNode &plan,
                                     CompilationContext *compilation_context, Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline),
      vpi_name_(GetCodeGen()->MakeFreshIdentifier("vpi")) {
  pipeline->RegisterSource(this, Pipeline::Parallelism::Parallel);
  if (plan.GetScanPredicate() != nullptr) {
    compilation_context->Prepare(*plan.GetScanPredicate());
  }
}

bool SeqScanTranslator::HasPredicate() const {
  return GetPlanAs<planner::SeqScanPlanNode>().GetScanPredicate() != nullptr;
}

std::string_view SeqScanTranslator::GetTableName() const {
  const auto table_oid = GetPlanAs<planner::SeqScanPlanNode>().GetTableOid();
  return Catalog::Instance()->LookupTableById(table_oid)->GetName();
}

void SeqScanTranslator::GenerateGenericTerm(FunctionBuilder *func,
                                            const planner::AbstractExpression *term) {
  auto codegen = GetCodeGen();

  auto vp = func->GetParameterByPosition(0);
  auto tids = func->GetParameterByPosition(1);

  // Declare vpi_base: VectorProjectionIterator.
  auto vpi_base = codegen->MakeFreshIdentifier("vpi_base");
  auto vpi_kind = ast::BuiltinType::VectorProjectionIterator;
  func->Append(codegen->DeclareVarNoInit(vpi_base, vpi_kind));

  // Assign vpi = &vpi_base
  auto vpi = codegen->MakeExpr(vpi_name_);
  func->Append(
      codegen->DeclareVarWithInit(vpi_name_, codegen->PointerType(codegen->MakeExpr(vpi_base))));

  auto gen_body = [&](const bool is_filtered) {
    Loop vpi_loop(codegen, codegen->MakeStmt(codegen->VPIInit(vpi, vp, tids)),  // @vpiInit()
                  codegen->VPIHasNext(vpi, is_filtered),                        // @vpiHasNext()
                  codegen->MakeStmt(codegen->VPIAdvance(vpi, is_filtered)));    // @vpiAdvance()
    {
      WorkContext context(GetCompilationContext(), *GetPipeline());
      auto cond_translator = GetCompilationContext()->LookupTranslator(*term);
      auto match = cond_translator->DeriveValue(&context, this);
      func->Append(codegen->VPIMatch(vpi, match));
    }
    vpi_loop.EndLoop();
  };

  If check_filtered(codegen, codegen->VPIIsFiltered(vpi));
  gen_body(true);
  check_filtered.Else();
  gen_body(false);
  check_filtered.EndIf();
}

void SeqScanTranslator::GenerateFilterClauseFunctions(
    util::RegionVector<ast::FunctionDecl *> *top_level_funcs,
    const planner::AbstractExpression *predicate, std::vector<ast::Identifier> *curr_clause,
    bool seen_conjunction) {
  // The top-most disjunctions in the tree form separate clauses in the filter manager.
  if (!seen_conjunction &&
      predicate->GetExpressionType() == planner::ExpressionType::CONJUNCTION_OR) {
    std::vector<ast::Identifier> next_clause;
    GenerateFilterClauseFunctions(top_level_funcs, predicate->GetChild(0), &next_clause, false);
    filters_.emplace_back(std::move(next_clause));
    GenerateFilterClauseFunctions(top_level_funcs, predicate->GetChild(1), curr_clause, false);
    return;
  }

  // Consecutive conjunctions are part of the same clause.
  if (predicate->GetExpressionType() == planner::ExpressionType::CONJUNCTION_AND) {
    GenerateFilterClauseFunctions(top_level_funcs, predicate->GetChild(0), curr_clause, true);
    GenerateFilterClauseFunctions(top_level_funcs, predicate->GetChild(1), curr_clause, true);
    return;
  }

  // At this point, we create a term.
  // Signature: (vp: *VectorProjection, tids: *TupleIdList) -> nil
  auto codegen = GetCodeGen();
  auto fn_name = codegen->MakeFreshIdentifier("filterClause");
  util::RegionVector<ast::FieldDecl *> params = codegen->MakeFieldList({
      codegen->MakeField(codegen->MakeIdentifier("vp"),
                         codegen->PointerType(ast::BuiltinType::VectorProjection)),
      codegen->MakeField(codegen->MakeIdentifier("tids"),
                         codegen->PointerType(ast::BuiltinType::TupleIdList)),
  });
  FunctionBuilder builder(codegen, fn_name, std::move(params), codegen->Nil());
  {
    ast::Expr *vp = builder.GetParameterByPosition(0);
    ast::Expr *tids = builder.GetParameterByPosition(1);
    if (planner::ExpressionUtil::IsColumnCompareWithConst(*predicate)) {
      auto cve = static_cast<const planner::ColumnValueExpression *>(predicate->GetChild(0));
      auto translator = GetCompilationContext()->LookupTranslator(*predicate->GetChild(1));
      auto const_val = translator->DeriveValue(nullptr, nullptr);
      builder.Append(codegen->VPIFilter(vp,                              // The vector projection
                                        predicate->GetExpressionType(),  // Comparison type
                                        cve->GetColumnOid(),             // Column index
                                        const_val,                       // Constant value
                                        tids));                          // TID list
    } else if (planner::ExpressionUtil::IsConstCompareWithColumn(*predicate)) {
      throw NotImplementedException("const <op> col vector filter comparison not implemented");
    } else {
      // If we ever reach this point, the current node in the expression tree violates strict DNF.
      // Its subtree is treated as a generic, non-vectorized filter.
      GenerateGenericTerm(&builder, predicate);
    }
  }
  curr_clause->push_back(fn_name);
  top_level_funcs->push_back(builder.Finish());
}

void SeqScanTranslator::DefineHelperFunctions(
    util::RegionVector<ast::FunctionDecl *> *top_level_funcs) {
  if (HasPredicate()) {
    std::vector<ast::Identifier> curr_clause;
    auto root_expr = GetPlanAs<planner::SeqScanPlanNode>().GetScanPredicate();
    GenerateFilterClauseFunctions(top_level_funcs, root_expr, &curr_clause, false);
    filters_.emplace_back(std::move(curr_clause));
  }
}

void SeqScanTranslator::ScanVPI(WorkContext *ctx, ast::Expr *vpi) const {
  auto codegen = GetCodeGen();
  Loop vpi_loop(codegen, nullptr, codegen->VPIHasNext(vpi, false),
                codegen->MakeStmt(codegen->VPIAdvance(vpi, false)));
  {
    // Push to parent.
    ctx->Push();
  }
  vpi_loop.EndLoop();
}

void SeqScanTranslator::ScanTable(WorkContext *ctx, ast::Expr *tvi, bool close_iter) const {
  auto codegen = GetCodeGen();
  auto func = codegen->CurrentFunction();

  Loop tvi_loop(codegen, codegen->TableIterAdvance(tvi));
  {
    auto vpi = codegen->MakeExpr(vpi_name_);
    func->Append(codegen->DeclareVarWithInit(vpi_name_, codegen->TableIterGetVPI(tvi)));

    if (HasPredicate()) {
      auto filter_manager = ctx->GetThreadStateEntryPtr(codegen, fm_slot_);
      func->Append(codegen->FilterManagerRunFilters(filter_manager, vpi));
    }

    if (ctx->GetPipeline().IsVectorized()) {
    } else {
      ScanVPI(ctx, vpi);
    }
  }
  tvi_loop.EndLoop();

  if (close_iter) {
    func->Append(codegen->TableIterClose(tvi));
  }
}

void SeqScanTranslator::DeclarePipelineState(PipelineContext *pipeline_context) {
  if (HasPredicate()) {
    auto fm_type = GetCodeGen()->BuiltinType(ast::BuiltinType::FilterManager);
    fm_slot_ = pipeline_context->DeclareStateEntry(GetCodeGen(), "filter", fm_type);
  }
}

void SeqScanTranslator::InitializePipelineState(const PipelineContext &pipeline_context) const {
  if (HasPredicate()) {
    auto codegen = GetCodeGen();
    auto func = codegen->CurrentFunction();
    auto fm = pipeline_context.GetThreadStateEntryPtr(codegen, fm_slot_);
    func->Append(codegen->FilterManagerInit(fm));

    for (const auto &clause : filters_) {
      func->Append(codegen->FilterManagerInsert(fm, clause));
    }

    func->Append(codegen->FilterManagerFinalize(fm));
  }
}

void SeqScanTranslator::TearDownPipelineState(const PipelineContext &pipeline_context) const {
  if (HasPredicate()) {
    auto codegen = GetCodeGen();
    auto fm = pipeline_context.GetThreadStateEntryPtr(codegen, fm_slot_);
    codegen->CurrentFunction()->Append(codegen->FilterManagerFree(fm));
  }
}

void SeqScanTranslator::PerformPipelineWork(WorkContext *work_context) const {
  auto codegen = GetCodeGen();
  auto func = codegen->CurrentFunction();

  if (work_context->GetPipeline().IsParallel()) {
    ScanTable(work_context, func->GetParameterByPosition(2), false);
  } else {
    auto tvi_name = codegen->MakeFreshIdentifier("tvi");
    auto tvi = codegen->AddressOf(codegen->MakeExpr(tvi_name));
    func->Append(codegen->DeclareVarNoInit(tvi_name, ast::BuiltinType::TableVectorIterator));
    func->Append(codegen->TableIterInit(tvi, GetTableName()));
    ScanTable(work_context, tvi, true);
  }
}

util::RegionVector<ast::FieldDecl *> SeqScanTranslator::GetWorkerParams() const {
  auto codegen = GetCodeGen();
  auto tvi_type = codegen->PointerType(ast::BuiltinType::TableVectorIterator);
  return codegen->MakeFieldList({codegen->MakeField(codegen->MakeIdentifier("tvi"), tvi_type)});
}

void SeqScanTranslator::LaunchWork(ast::Identifier work_func_name) const {
  // @iterateTableParallel(table_name, query_state, thread_state_container, worker)
  auto codegen = GetCodeGen();
  auto func = codegen->CurrentFunction();
  func->Append(codegen->IterateTableParallel(GetTableName(), GetQueryStatePtr(),
                                             GetThreadStateContainer(), work_func_name));
}

ast::Expr *SeqScanTranslator::GetTableColumn(uint16_t col_oid) const {
  const auto table_oid = GetPlanAs<planner::SeqScanPlanNode>().GetTableOid();
  const auto schema = &Catalog::Instance()->LookupTableById(table_oid)->GetSchema();
  auto type = schema->GetColumnInfo(col_oid)->sql_type.GetPrimitiveTypeId();
  auto nullable = schema->GetColumnInfo(col_oid)->sql_type.IsNullable();
  return GetCodeGen()->VPIGet(GetCodeGen()->MakeExpr(vpi_name_), type, nullable, col_oid);
}

}  // namespace tpl::sql::codegen
