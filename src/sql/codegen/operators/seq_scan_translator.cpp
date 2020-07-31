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
  // If there's a predicate, prepare the expression and register a filter manager.
  if (HasPredicate()) {
    compilation_context->Prepare(*plan.GetScanPredicate());

    ast::Expr *fm_type = GetCodeGen()->BuiltinType(ast::BuiltinType::FilterManager);
    local_filter_manager_ = pipeline->DeclarePipelineStateEntry("filter_manager", fm_type);
  }
}

bool SeqScanTranslator::HasPredicate() const {
  return GetPlanAs<planner::SeqScanPlanNode>().GetScanPredicate() != nullptr;
}

std::string_view SeqScanTranslator::GetTableName() const {
  const auto table_oid = GetPlanAs<planner::SeqScanPlanNode>().GetTableOid();
  return Catalog::Instance()->LookupTableById(table_oid)->GetName();
}

void SeqScanTranslator::GenerateGenericTerm(FunctionBuilder *function,
                                            const planner::AbstractExpression *term,
                                            ast::Expr *vector_proj, ast::Expr *tid_list) {
  auto codegen = GetCodeGen();

  // var vpiBase: VectorProjectionIterator
  // var vpi = &vpiBase
  auto vpi_base = codegen->MakeFreshIdentifier("vpi_base");
  function->Append(codegen->DeclareVarNoInit(vpi_base, ast::BuiltinType::VectorProjectionIterator));
  function->Append(
      codegen->DeclareVarWithInit(vpi_var_, codegen->AddressOf(codegen->MakeExpr(vpi_base))));

  // @vpiInit()
  auto vpi = codegen->MakeExpr(vpi_var_);
  function->Append(codegen->VPIInit(vpi, vector_proj, tid_list));

  Loop vpi_loop(function, nullptr, codegen->VPIHasNext(vpi),
                codegen->MakeStmt(codegen->VPIAdvance(vpi)));
  {
    ConsumerContext context(GetCompilationContext(), *GetPipeline());
    auto cond_translator = GetCompilationContext()->LookupTranslator(*term);
    auto match = cond_translator->DeriveValue(&context, this);
    function->Append(codegen->VPIMatch(vpi, match));
  }
  vpi_loop.EndLoop();
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
  // Signature: (vp: *VectorProjection, tids: *TupleIdList, ctx: *uint8) -> nil
  auto codegen = GetCodeGen();
  auto fn_name =
      codegen->MakeFreshIdentifier(GetPipeline()->CreatePipelineFunctionName("FilterClause"));
  util::RegionVector<ast::FieldDecl *> params = codegen->MakeFieldList({
      codegen->MakeField(codegen->MakeIdentifier("vp"),
                         codegen->PointerType(ast::BuiltinType::VectorProjection)),
      codegen->MakeField(codegen->MakeIdentifier("tids"),
                         codegen->PointerType(ast::BuiltinType::TupleIdList)),
      codegen->MakeField(codegen->MakeIdentifier("context"),
                         codegen->PointerType(ast::BuiltinType::Uint8)),
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
      // If we ever reach this point, the current node in the expression tree
      // violates strict DNF. Its subtree is treated as a generic,
      // non-vectorized filter.
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

void SeqScanTranslator::ScanVPI(ConsumerContext *ctx, FunctionBuilder *function,
                                ast::Expr *vpi) const {
  CodeGen *codegen = GetCodeGen();

  Loop vpi_loop(function, nullptr, codegen->VPIHasNext(vpi),
                codegen->MakeStmt(codegen->VPIAdvance(vpi)));
  {
    // Push to parent.
    ctx->Consume(function);
  }
  vpi_loop.EndLoop();
}

void SeqScanTranslator::ScanTable(ConsumerContext *ctx, FunctionBuilder *function) const {
  CodeGen *codegen = GetCodeGen();
  Loop tvi_loop(function, codegen->TableIterAdvance(codegen->MakeExpr(tvi_var_)));
  {
    // var vpi = @tableIterGetVPI()
    auto vpi = codegen->MakeExpr(vpi_var_);
    function->Append(codegen->DeclareVarWithInit(
        vpi_var_, codegen->TableIterGetVPI(codegen->MakeExpr(tvi_var_))));

    if (HasPredicate()) {
      auto filter_manager = local_filter_manager_.GetPtr(codegen);
      function->Append(codegen->FilterManagerRunFilters(filter_manager, vpi));
    }

    if (!ctx->GetPipeline().IsVectorized()) {
      ScanVPI(ctx, function, vpi);
    }
  }
  tvi_loop.EndLoop();
}

void SeqScanTranslator::InitializePipelineState(const Pipeline &pipeline,
                                                FunctionBuilder *function) const {
  if (HasPredicate()) {
    CodeGen *codegen = GetCodeGen();
    function->Append(codegen->FilterManagerInit(local_filter_manager_.GetPtr(codegen)));
    for (const auto &clause : filters_) {
      function->Append(codegen->FilterManagerInsert(local_filter_manager_.GetPtr(codegen), clause));
    }
  }
}

void SeqScanTranslator::TearDownPipelineState(const Pipeline &pipeline,
                                              FunctionBuilder *function) const {
  if (HasPredicate()) {
    auto filter_manager = local_filter_manager_.GetPtr(GetCodeGen());
    function->Append(GetCodeGen()->FilterManagerFree(filter_manager));
  }
}

void SeqScanTranslator::Consume(ConsumerContext *context, FunctionBuilder *function) const {
  CodeGen *codegen = GetCodeGen();

  const bool declare_local_tvi = !GetPipeline()->IsParallel() || !GetPipeline()->IsDriver(this);
  if (declare_local_tvi) {
    // var tviBase: TableVectorIterator
    // var tvi = &tviBase
    auto tvi_base = codegen->MakeFreshIdentifier("tvi_base");
    function->Append(codegen->DeclareVarNoInit(tvi_base, ast::BuiltinType::TableVectorIterator));
    function->Append(codegen->DeclareVarWithInit(tvi_var_, codegen->AddressOf(tvi_base)));
    function->Append(codegen->TableIterInit(codegen->MakeExpr(tvi_var_), GetTableName()));
  }

  // Scan it.
  ScanTable(context, function);

  // Close TVI, if need be.
  if (declare_local_tvi) {
    function->Append(codegen->TableIterClose(codegen->MakeExpr(tvi_var_)));
  }
}

util::RegionVector<ast::FieldDecl *> SeqScanTranslator::GetWorkerParams() const {
  auto codegen = GetCodeGen();
  auto tvi_type = codegen->PointerType(ast::BuiltinType::TableVectorIterator);
  return codegen->MakeFieldList({codegen->MakeField(tvi_var_, tvi_type)});
}

void SeqScanTranslator::LaunchWork(FunctionBuilder *function, ast::Identifier work_func) const {
  function->Append(GetCodeGen()->IterateTableParallel(GetTableName(), GetQueryStatePtr(),
                                                      GetThreadStateContainer(), work_func));
}

ast::Expr *SeqScanTranslator::GetTableColumn(uint16_t col_oid) const {
  const auto table_oid = GetPlanAs<planner::SeqScanPlanNode>().GetTableOid();
  const auto schema = &Catalog::Instance()->LookupTableById(table_oid)->GetSchema();
  auto type = schema->GetColumnInfo(col_oid)->sql_type.GetPrimitiveTypeId();
  auto nullable = schema->GetColumnInfo(col_oid)->sql_type.IsNullable();
  return GetCodeGen()->VPIGet(GetCodeGen()->MakeExpr(vpi_var_), type, nullable, col_oid);
}

}  // namespace tpl::sql::codegen
