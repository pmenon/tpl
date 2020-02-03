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
      tvi_var_(GetCodeGen()->MakeFreshIdentifier("tvi")),
      vpi_var_(GetCodeGen()->MakeFreshIdentifier("vpi")) {
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
                                            const planner::AbstractExpression *term,
                                            ast::Expr *vector_proj, ast::Expr *tid_list) {
  auto codegen = GetCodeGen();

  // var vpiBase: VectorProjectionIterator
  // var vpi = &vpiBase
  auto vpi_base = codegen->MakeFreshIdentifier("vpiBase");
  func->Append(codegen->DeclareVarNoInit(vpi_base, ast::BuiltinType::VectorProjectionIterator));
  func->Append(
      codegen->DeclareVarWithInit(vpi_var_, codegen->AddressOf(codegen->MakeExpr(vpi_base))));

  auto vpi = codegen->MakeExpr(vpi_var_);
  auto gen_body = [&](const bool is_filtered) {
    Loop vpi_loop(codegen,
                  codegen->MakeStmt(codegen->VPIInit(vpi, vector_proj, tid_list)),  // @vpiInit()
                  codegen->VPIHasNext(vpi, is_filtered),                            // @vpiHasNext()
                  codegen->MakeStmt(codegen->VPIAdvance(vpi, is_filtered)));        // @vpiAdvance()
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
    util::RegionVector<ast::FunctionDecl *> *decls, const planner::AbstractExpression *predicate,
    std::vector<ast::Identifier> *curr_clause, bool seen_conjunction) {
  // The top-most disjunctions in the tree form separate clauses in the filter manager.
  if (!seen_conjunction &&
      predicate->GetExpressionType() == planner::ExpressionType::CONJUNCTION_OR) {
    std::vector<ast::Identifier> next_clause;
    GenerateFilterClauseFunctions(decls, predicate->GetChild(0), &next_clause, false);
    filters_.emplace_back(std::move(next_clause));
    GenerateFilterClauseFunctions(decls, predicate->GetChild(1), curr_clause, false);
    return;
  }

  // Consecutive conjunctions are part of the same clause.
  if (predicate->GetExpressionType() == planner::ExpressionType::CONJUNCTION_AND) {
    GenerateFilterClauseFunctions(decls, predicate->GetChild(0), curr_clause, true);
    GenerateFilterClauseFunctions(decls, predicate->GetChild(1), curr_clause, true);
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
    ast::Expr *vector_proj = builder.GetParameterByPosition(0);
    ast::Expr *tid_list = builder.GetParameterByPosition(1);
    if (planner::ExpressionUtil::IsColumnCompareWithConst(*predicate)) {
      auto cve = static_cast<const planner::ColumnValueExpression *>(predicate->GetChild(0));
      auto translator = GetCompilationContext()->LookupTranslator(*predicate->GetChild(1));
      auto const_val = translator->DeriveValue(nullptr, nullptr);
      builder.Append(codegen->VPIFilter(vector_proj,                     // The vector projection
                                        predicate->GetExpressionType(),  // Comparison type
                                        cve->GetColumnOid(),             // Column index
                                        const_val,                       // Constant value
                                        tid_list));                      // TID list
    } else if (planner::ExpressionUtil::IsConstCompareWithColumn(*predicate)) {
      throw NotImplementedException("const <op> col vector filter comparison not implemented");
    } else {
      // If we ever reach this point, the current node in the expression tree violates strict DNF.
      // Its subtree is treated as a generic, non-vectorized filter.
      GenerateGenericTerm(&builder, predicate, vector_proj, tid_list);
    }
  }
  curr_clause->push_back(fn_name);
  decls->push_back(builder.Finish());
}

void SeqScanTranslator::DefineHelperFunctions(util::RegionVector<ast::FunctionDecl *> *decls) {
  if (HasPredicate()) {
    std::vector<ast::Identifier> curr_clause;
    auto root_expr = GetPlanAs<planner::SeqScanPlanNode>().GetScanPredicate();
    GenerateFilterClauseFunctions(decls, root_expr, &curr_clause, false);
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

void SeqScanTranslator::ScanTable(WorkContext *ctx) const {
  auto codegen = GetCodeGen();
  auto func = codegen->CurrentFunction();
  auto tvi = codegen->MakeExpr(tvi_var_);

  Loop tvi_loop(codegen, codegen->TableIterAdvance(tvi));
  {
    auto vpi = codegen->MakeExpr(vpi_var_);
    func->Append(codegen->DeclareVarWithInit(vpi_var_, codegen->TableIterGetVPI(tvi)));

    if (HasPredicate()) {
      auto filter_manager = ctx->GetThreadStateEntryPtr(codegen, local_filter_manager_slot_);
      func->Append(codegen->FilterManagerRunFilters(filter_manager, vpi));
    }

    if (!ctx->GetPipeline().IsVectorized()) {
      ScanVPI(ctx, vpi);
    }
  }
  tvi_loop.EndLoop();

  if (!ctx->GetPipeline().IsParallel()) {
    func->Append(codegen->TableIterClose(tvi));
  }
}

void SeqScanTranslator::DeclarePipelineState(PipelineContext *pipeline_context) {
  if (HasPredicate()) {
    auto fm_type = GetCodeGen()->BuiltinType(ast::BuiltinType::FilterManager);
    local_filter_manager_slot_ =
        pipeline_context->DeclareStateEntry(GetCodeGen(), "filter", fm_type);
  }
}

void SeqScanTranslator::InitializePipelineState(const PipelineContext &pipeline_context) const {
  if (HasPredicate()) {
    auto codegen = GetCodeGen();
    auto func = codegen->CurrentFunction();
    auto fm = pipeline_context.GetThreadStateEntryPtr(codegen, local_filter_manager_slot_);
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
    auto fm = pipeline_context.GetThreadStateEntryPtr(codegen, local_filter_manager_slot_);
    codegen->CurrentFunction()->Append(codegen->FilterManagerFree(fm));
  }
}

void SeqScanTranslator::PerformPipelineWork(WorkContext *work_context) const {
  auto codegen = GetCodeGen();
  auto func = codegen->CurrentFunction();

  if (!work_context->GetPipeline().IsParallel()) {
    // var tviBase: TableVectorIterator
    // var tvi = &tviBase
    auto tvi_base = codegen->MakeFreshIdentifier("tviBase");
    func->Append(codegen->DeclareVarNoInit(tvi_base, ast::BuiltinType::TableVectorIterator));
    func->Append(
        codegen->DeclareVarWithInit(tvi_var_, codegen->AddressOf(codegen->MakeExpr(tvi_base))));
    func->Append(codegen->TableIterInit(codegen->MakeExpr(tvi_var_), GetTableName()));
  }

  ScanTable(work_context);
}

util::RegionVector<ast::FieldDecl *> SeqScanTranslator::GetWorkerParams() const {
  auto codegen = GetCodeGen();
  auto tvi_type = codegen->PointerType(ast::BuiltinType::TableVectorIterator);
  return codegen->MakeFieldList({codegen->MakeField(tvi_var_, tvi_type)});
}

void SeqScanTranslator::LaunchWork(ast::Identifier work_func_name) const {
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
  return GetCodeGen()->VPIGet(GetCodeGen()->MakeExpr(vpi_var_), type, nullable, col_oid);
}

}  // namespace tpl::sql::codegen
