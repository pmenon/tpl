#include "sql/codegen/operators/seq_scan_translator.h"

#include "sql/catalog.h"
#include "sql/codegen/codegen.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/consumer_context.h"
#include "sql/codegen/function_builder.h"
#include "sql/codegen/loop.h"
#include "sql/codegen/pipeline.h"
#include "sql/planner/plannodes/seq_scan_plan_node.h"
#include "sql/table.h"

namespace tpl::sql::codegen {

SeqScanTranslator::SeqScanTranslator(const planner::SeqScanPlanNode &plan,
                                     CompilationContext *compilation_context, Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline) {
  // Register as a parallel scan.
  pipeline->RegisterSource(this, Pipeline::Parallelism::Parallel);
  // Prepare the scan predicate.
  if (plan.GetScanPredicate() != nullptr) {
    compilation_context->Prepare(*plan.GetScanPredicate());
  }
}

std::string_view SeqScanTranslator::GetTableName() const {
  const auto table_oid = GetPlanAs<planner::SeqScanPlanNode>().GetTableOid();
  return Catalog::Instance()->LookupTableById(table_oid)->GetName();
}

void SeqScanTranslator::DoScanTable(ConsumerContext *ctx, ast::Expr *tvi, bool close_iter) const {
  auto codegen = GetCodeGen();
  auto func = codegen->CurrentFunction();

  // Loop while iterator has data.
  Loop tvi_loop(codegen, codegen->TableIterAdvance(tvi));
  {
    // Stuff
    auto vpi_name = codegen->MakeFreshIdentifier("vpi");
    UNUSED auto vpi = codegen->MakeExpr(vpi_name);
    func->Append(codegen->DeclareVarWithInit(vpi_name, codegen->TableIterGetVPI(tvi)));
  }
  tvi_loop.EndLoop();

  if (close_iter) {
    func->Append(codegen->TableIterClose(tvi));
  }
}

void SeqScanTranslator::DoPipelineWork(ConsumerContext *ctx) const {
  auto codegen = GetCodeGen();
  auto func = codegen->CurrentFunction();

  ast::Expr *tvi = nullptr;
  if (ctx->GetPipeline().IsParallel()) {
    DoScanTable(ctx, func->GetParameterByPosition(2), false);
  } else {
    auto tvi_name = codegen->MakeFreshIdentifier("tvi");
    tvi = codegen->AddressOf(codegen->MakeExpr(tvi_name));
    func->Append(codegen->DeclareVarNoInit(tvi_name, ast::BuiltinType::Kind::TableVectorIterator));
    func->Append(codegen->TableIterInit(tvi, GetTableName()));
    DoScanTable(ctx, tvi, true);
  }
}

util::RegionVector<ast::FieldDecl *> SeqScanTranslator::GetWorkerParams() const {
  auto codegen = GetCodeGen();
  auto tvi_var = codegen->MakeIdentifier("tvi");
  auto tvi_type = codegen->PointerType(ast::BuiltinType::Kind::TableVectorIterator);
  return codegen->MakeFieldList({codegen->MakeField(tvi_var, tvi_type)});
}

void SeqScanTranslator::LaunchWork(ast::Identifier work_func_name) const {
  // @iterateTableParallel(table_name, query_state, thread_state_container, worker)
  auto codegen = GetCodeGen();
  auto func = codegen->CurrentFunction();
  auto state_ptr = GetQueryState().GetStatePointer(codegen);
  auto tls_ptr = GetThreadStateContainer();
  func->Append(codegen->IterateTableParallel(GetTableName(), state_ptr, tls_ptr, work_func_name));
}

}  // namespace tpl::sql::codegen
