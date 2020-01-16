#include "sql/codegen/operators/seq_scan_translator.h"

#include "common/exception.h"
#include "sql/catalog.h"
#include "sql/codegen/codegen.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/consumer_context.h"
#include "sql/codegen/function_builder.h"
#include "sql/codegen/loop.h"
#include "sql/codegen/pipeline.h"
#include "sql/codegen/top_level_declarations.h"
#include "sql/planner/expressions/column_value_expression.h"
#include "sql/planner/expressions/expression_util.h"
#include "sql/planner/plannodes/seq_scan_plan_node.h"
#include "sql/table.h"

namespace tpl::sql::codegen {

SeqScanTranslator::SeqScanTranslator(const planner::SeqScanPlanNode &plan,
                                     CompilationContext *compilation_context, Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline) {
  // Register as a parallel scan.
  pipeline->RegisterSource(this, Pipeline::Parallelism::Parallel);
  // Prepare the scan predicate.
  if (HasPredicate()) {
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

class SeqScanTranslator::TableColumnAccess : public ConsumerContext::ValueProvider {
 public:
  TableColumnAccess() : schema_(nullptr), codegen_(nullptr), vpi_(nullptr), col_idx_(0) {}

  TableColumnAccess(const Schema *schema, CodeGen *codegen, ast::Expr *vpi, uint32_t col_idx)
      : schema_(schema), codegen_(codegen), vpi_(vpi), col_idx_(col_idx) {}

  // Call @vpiGet[Type](vpi, index)
  ast::Expr *GetValue(ConsumerContext *ctx) const override {
    auto type = schema_->GetColumnInfo(col_idx_)->sql_type.GetPrimitiveTypeId();
    auto nullable = schema_->GetColumnInfo(col_idx_)->sql_type.nullable();
    return codegen_->VPIGet(vpi_, type, nullable, col_idx_);
  }

 private:
  // The table schema.
  const Schema *schema_;
  // The code generator instance.
  CodeGen *codegen_;
  // The VPI pointer.
  ast::Expr *vpi_;
  // The column to read in the VPI.
  uint32_t col_idx_;
};

void SeqScanTranslator::PopulateContextWithVPIAttributes(
    ConsumerContext *consumer_context, ast::Expr *vpi,
    std::vector<SeqScanTranslator::TableColumnAccess> *attrs) const {
  const auto table_oid = GetPlanAs<planner::SeqScanPlanNode>().GetTableOid();
  const auto schema = &Catalog::Instance()->LookupTableById(table_oid)->GetSchema();
  attrs->resize(schema->GetColumnCount());

  CodeGen *codegen = GetCodeGen();
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    (*attrs)[i] = TableColumnAccess(schema, codegen, vpi, i);
    consumer_context->RegisterColumnValueProvider(i, &(*attrs)[i]);
  }
}

void SeqScanTranslator::GenerateGenericTerm(FunctionBuilder *func,
                                            const planner::AbstractExpression *term) {
  CodeGen *codegen = GetCodeGen();

  ast::Expr *vp = func->GetParameterByPosition(0);
  ast::Expr *tids = func->GetParameterByPosition(1);

  // Declare vpi_base: VectorProjectionIterator.
  ast::Identifier vpi_base = codegen->MakeFreshIdentifier("vpi_base");
  ast::BuiltinType::Kind vpi_kind = ast::BuiltinType::Kind::VectorProjectionIterator;
  func->Append(codegen->DeclareVarNoInit(vpi_base, vpi_kind));

  // Assign vpi = &vpi_base
  ast::Identifier vpi_name = codegen->MakeFreshIdentifier("vpi");
  ast::Expr *vpi = codegen->MakeExpr(vpi_name);
  func->Append(
      codegen->DeclareVarWithInit(vpi_name, codegen->PointerType(codegen->MakeExpr(vpi_base))));

  Loop vpi_loop(codegen, codegen->MakeStmt(codegen->VPIInit(vpi, vp, tids)),  // @vpiInit()
                codegen->VPIHasNext(vpi, true),                               // @vpiHasNext()
                codegen->MakeStmt(codegen->VPIAdvance(vpi, true)));           // @vpiAdvance()
  {
    // Create evaluation context.
    std::vector<TableColumnAccess> attrs;
    ConsumerContext ctx(GetCompilationContext(), *GetPipeline());
    PopulateContextWithVPIAttributes(&ctx, vpi, &attrs);

    // Call @vpiMatch() with the result of the comparison.
    ExpressionTranslator *cond_translator = GetCompilationContext()->LookupTranslator(*term);
    ast::Expr *cond = cond_translator->DeriveValue(&ctx);
    func->Append(codegen->VPIMatch(vpi, cond));
  }
  vpi_loop.EndLoop();
}

void SeqScanTranslator::GenerateFilterClauseFunctions(TopLevelDeclarations *top_level_declarations,
                                                      const planner::AbstractExpression *predicate,
                                                      std::vector<ast::Identifier> *curr_clause,
                                                      bool seen_conjunction) {
  CodeGen *codegen = GetCodeGen();

  // The top-most disjunctions in the tree form separate clauses in the filter manager.
  if (!seen_conjunction &&
      predicate->GetExpressionType() == planner::ExpressionType::CONJUNCTION_OR) {
    std::vector<ast::Identifier> next_clause;
    GenerateFilterClauseFunctions(top_level_declarations, predicate->GetChild(0), &next_clause,
                                  false);
    filters_.emplace_back(std::move(next_clause));
    GenerateFilterClauseFunctions(top_level_declarations, predicate->GetChild(1), curr_clause,
                                  false);
    return;
  }

  // Consecutive conjunctions are part of the same clause.
  if (predicate->GetExpressionType() == planner::ExpressionType::CONJUNCTION_AND) {
    GenerateFilterClauseFunctions(top_level_declarations, predicate->GetChild(0), curr_clause,
                                  true);
    GenerateFilterClauseFunctions(top_level_declarations, predicate->GetChild(1), curr_clause,
                                  true);
    return;
  }

  // At this point, we create a term.
  // Signature: (vp: *VectorProjection, tids: *TupleIdList) -> nil
  // First make vp: *VectorProjection
  ast::Identifier vp_name = codegen->MakeIdentifier("vp");
  ast::FieldDecl *param1 =
      codegen->MakeField(vp_name, codegen->PointerType(ast::BuiltinType::Kind::VectorProjection));

  // Then make tids: *TupleIdList.
  ast::Identifier tids_name = codegen->MakeIdentifier("tids");
  ast::FieldDecl *param2 =
      codegen->MakeField(tids_name, codegen->PointerType(ast::BuiltinType::Kind::TupleIdList));

  // Now create the function.
  ast::Identifier fn_name = codegen->MakeFreshIdentifier("filterClause");
  util::RegionVector<ast::FieldDecl *> params = codegen->MakeFieldList({param1, param2});
  FunctionBuilder builder(codegen, fn_name, std::move(params), codegen->Nil());
  {
    ast::Expr *vp = builder.GetParameterByPosition(0);
    ast::Expr *tids = builder.GetParameterByPosition(1);
    if (planner::ExpressionUtil::IsColumnCompareWithConst(*predicate)) {
      auto cve = static_cast<const planner::ColumnValueExpression *>(predicate->GetChild(0));
      auto translator = GetCompilationContext()->LookupTranslator(*predicate->GetChild(1));
      auto const_val = translator->DeriveValue(nullptr);
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
  // Register function.
  top_level_declarations->RegisterFunction(builder.Finish());
  // Add to list of filters.
  curr_clause->emplace_back(fn_name);
}

void SeqScanTranslator::DefineHelperFunctions(TopLevelDeclarations *top_level_decls) {
  if (HasPredicate()) {
    std::vector<ast::Identifier> curr_clause;
    auto root_expr = GetPlanAs<planner::SeqScanPlanNode>().GetScanPredicate();
    GenerateFilterClauseFunctions(top_level_decls, root_expr, &curr_clause, false);
    filters_.emplace_back(std::move(curr_clause));
  }
}

void SeqScanTranslator::GenerateVPIScan(ConsumerContext *consumer_context, ast::Expr *vpi) const {
  TPL_ASSERT(!consumer_context->GetPipeline().IsVectorized(),
             "Should not generate VPI scan for vectorized pipeline");

  CodeGen *codegen = GetCodeGen();
  Loop vpi_loop(codegen, nullptr, codegen->VPIHasNext(vpi, false),
                codegen->MakeStmt(codegen->VPIAdvance(vpi, false)));
  {
    // Populate context with VPI columns.
    std::vector<TableColumnAccess> attrs;
    PopulateContextWithVPIAttributes(consumer_context, vpi, &attrs);
    // Push to consumer.
    consumer_context->Push();
  }
  vpi_loop.EndLoop();
}

void SeqScanTranslator::DoScanTable(ConsumerContext *ctx, ast::Expr *tvi, bool close_iter) const {
  CodeGen *codegen = GetCodeGen();
  FunctionBuilder *func = codegen->CurrentFunction();

  // Loop while iterator has data.
  Loop tvi_loop(codegen, codegen->TableIterAdvance(tvi));
  {
    // Pull out VPI from TVI.
    ast::Identifier vpi_var = codegen->MakeFreshIdentifier("vpi");
    ast::Expr *vpi = codegen->MakeExpr(vpi_var);
    func->Append(codegen->DeclareVarWithInit(vpi_var, codegen->TableIterGetVPI(tvi)));

    // Generate filter.
    if (HasPredicate()) {
      ast::Expr *fm = ctx->GetPipelineContext()->GetThreadStateEntryPtr(codegen, fm_slot_);
      func->Append(codegen->FilterManagerRunFilters(fm, vpi));
    }

    // If the pipeline is vectorized, just pass along the VPI. Otherwise,
    // generate a scan over the VPI.
    if (ctx->GetPipeline().IsVectorized()) {
    } else {
      GenerateVPIScan(ctx, vpi);
    }
  }
  tvi_loop.EndLoop();

  if (close_iter) {
    func->Append(codegen->TableIterClose(tvi));
  }
}

void SeqScanTranslator::DeclarePipelineState(PipelineContext *pipeline_context) {
  if (HasPredicate()) {
    // Declare the filter manager.
    CodeGen *codegen = GetCodeGen();
    ast::Expr *filter_manager_type = codegen->BuiltinType(ast::BuiltinType::Kind::FilterManager);
    fm_slot_ = pipeline_context->DeclareStateEntry(codegen, "filter", filter_manager_type);
  }
}

void SeqScanTranslator::InitializePipelineState(const PipelineContext &pipeline_context) const {
  if (HasPredicate()) {
    // Initialize the filter manager.
    CodeGen *codegen = GetCodeGen();
    FunctionBuilder *func = codegen->CurrentFunction();
    ast::Expr *fm = pipeline_context.GetThreadStateEntryPtr(codegen, fm_slot_);
    // Initialize filter manager.
    func->Append(codegen->FilterManagerInit(fm));
    // Insert clauses.
    for (const auto &clause : filters_) {
      func->Append(codegen->FilterManagerInsert(fm, clause));
    }
    // Finalize filter manager.
    func->Append(codegen->FilterManagerFinalize(fm));
  }
}

void SeqScanTranslator::TearDownPipelineState(const PipelineContext &pipeline_context) const {
  if (HasPredicate()) {
    // Tear-down filter manager.
    CodeGen *codegen = GetCodeGen();
    ast::Expr *fm = pipeline_context.GetThreadStateEntryPtr(codegen, fm_slot_);
    codegen->CurrentFunction()->Append(codegen->FilterManagerFree(fm));
  }
}

void SeqScanTranslator::DoPipelineWork(ConsumerContext *consumer_context) const {
  CodeGen *codegen = GetCodeGen();
  FunctionBuilder *func = codegen->CurrentFunction();

  if (consumer_context->GetPipeline().IsParallel()) {
    // Parallel scan, the TVI is the last argument the function.
    DoScanTable(consumer_context, func->GetParameterByPosition(2), false);
  } else {
    // Declare new TVI.
    ast::Identifier tvi_var = codegen->MakeFreshIdentifier("tvi");
    ast::Expr *tvi = codegen->AddressOf(codegen->MakeExpr(tvi_var));
    func->Append(codegen->DeclareVarNoInit(tvi_var, ast::BuiltinType::Kind::TableVectorIterator));
    // @tableIterInit().
    func->Append(codegen->TableIterInit(tvi, GetTableName()));
    // Scan it.
    DoScanTable(consumer_context, tvi, true);
  }
}

util::RegionVector<ast::FieldDecl *> SeqScanTranslator::GetWorkerParams() const {
  CodeGen *codegen = GetCodeGen();
  ast::Identifier tvi_var = codegen->MakeIdentifier("tvi");
  ast::Expr *tvi_type = codegen->PointerType(ast::BuiltinType::Kind::TableVectorIterator);
  return codegen->MakeFieldList({codegen->MakeField(tvi_var, tvi_type)});
}

void SeqScanTranslator::LaunchWork(ast::Identifier work_func_name) const {
  // @iterateTableParallel(table_name, query_state, thread_state_container, worker)
  CodeGen *codegen = GetCodeGen();
  FunctionBuilder *func = codegen->CurrentFunction();
  ast::Expr *state_ptr = GetQueryState().GetStatePointer(codegen);
  ast::Expr *tls_ptr = GetThreadStateContainer();
  func->Append(codegen->IterateTableParallel(GetTableName(), state_ptr, tls_ptr, work_func_name));
}

}  // namespace tpl::sql::codegen
