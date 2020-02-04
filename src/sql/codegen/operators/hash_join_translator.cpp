#include "sql/codegen/operators/hash_join_translator.h"

#include "ast/type.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/function_builder.h"
#include "sql/codegen/if.h"
#include "sql/codegen/loop.h"
#include "sql/codegen/work_context.h"
#include "sql/planner/plannodes/hash_join_plan_node.h"

namespace tpl::sql::codegen {

namespace {
const char *kBuildRowAttrPrefix = "attr";
}  // namespace

HashJoinTranslator::HashJoinTranslator(const planner::HashJoinPlanNode &plan,
                                       CompilationContext *compilation_context, Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline),
      build_row_var_(GetCodeGen()->MakeFreshIdentifier("buildRow")),
      build_row_type_(GetCodeGen()->MakeFreshIdentifier("BuildRow")),
      left_pipeline_(this, Pipeline::Parallelism::Parallel) {
  pipeline->RegisterStep(this, Pipeline::Parallelism::Parallel);
  pipeline->LinkSourcePipeline(LeftPipeline());

  compilation_context->Prepare(*plan.GetChild(0), LeftPipeline());
  compilation_context->Prepare(*plan.GetChild(1), RightPipeline());

  compilation_context->Prepare(*plan.GetJoinPredicate());
  for (const auto left_hash_key : plan.GetLeftHashKeys()) {
    compilation_context->Prepare(*left_hash_key);
  }
  for (const auto right_hash_key : plan.GetRightHashKeys()) {
    compilation_context->Prepare(*right_hash_key);
  }

  CodeGen *codegen = GetCodeGen();
  ast::Expr *join_ht_type = codegen->BuiltinType(ast::BuiltinType::JoinHashTable);
  global_join_ht_ = compilation_context->GetQueryState()->DeclareStateEntry(
      codegen, "joinHashTable", join_ht_type);

  if (left_pipeline_.IsParallel()) {
    local_join_ht_ = left_pipeline_.GetPipelineState()->DeclareStateEntry(codegen, "joinHashTable",
                                                                          join_ht_type);
  }
}

void HashJoinTranslator::DefineHelperStructs(util::RegionVector<ast::StructDecl *> *decls) {
  auto codegen = GetCodeGen();
  auto fields = codegen->MakeEmptyFieldList();
  GetAllChildOutputFields(0, kBuildRowAttrPrefix, &fields);
  if (GetPlanAs<planner::HashJoinPlanNode>().RequiresLeftMark()) {
    fields.push_back(codegen->MakeField(codegen->MakeFreshIdentifier("mark"), codegen->BoolType()));
  }
  decls->push_back(codegen->DeclareStruct(build_row_type_, std::move(fields)));
}

void HashJoinTranslator::InitializeJoinHashTable(FunctionBuilder *function,
                                                 ast::Expr *jht_ptr) const {
  function->Append(GetCodeGen()->JoinHashTableInit(jht_ptr, GetMemoryPool(), build_row_type_));
}

void HashJoinTranslator::TearDownJoinHashTable(FunctionBuilder *function,
                                               ast::Expr *jht_ptr) const {
  function->Append(GetCodeGen()->JoinHashTableFree(jht_ptr));
}

void HashJoinTranslator::InitializeQueryState(FunctionBuilder *function) const {
  InitializeJoinHashTable(function, global_join_ht_.GetPtr(GetCodeGen()));
}

void HashJoinTranslator::TearDownQueryState(FunctionBuilder *function) const {
  TearDownJoinHashTable(function, global_join_ht_.GetPtr(GetCodeGen()));
}

void HashJoinTranslator::InitializePipelineState(const Pipeline &pipeline,
                                                 FunctionBuilder *function) const {
  if (IsLeftPipeline(pipeline) && LeftPipeline().IsParallel()) {
    InitializeJoinHashTable(function, local_join_ht_.GetPtr(GetCodeGen()));
  }
}

void HashJoinTranslator::TearDownPipelineState(const Pipeline &pipeline,
                                               FunctionBuilder *function) const {
  if (IsLeftPipeline(pipeline) && LeftPipeline().IsParallel()) {
    TearDownJoinHashTable(function, local_join_ht_.GetPtr(GetCodeGen()));
  }
}

ast::Expr *HashJoinTranslator::HashKeys(
    WorkContext *ctx, FunctionBuilder *function,
    const std::vector<const planner::AbstractExpression *> &hash_keys) const {
  auto codegen = GetCodeGen();

  std::vector<ast::Expr *> key_values;
  key_values.reserve(hash_keys.size());
  for (const auto hash_key : hash_keys) {
    key_values.push_back(ctx->DeriveValue(*hash_key, this));
  }

  ast::Identifier hash_val_name = codegen->MakeFreshIdentifier("hashVal");
  function->Append(codegen->DeclareVarWithInit(hash_val_name, codegen->Hash(key_values)));

  return codegen->MakeExpr(hash_val_name);
}

ast::Expr *HashJoinTranslator::GetBuildRowAttribute(ast::Expr *build_row, uint32_t attr_idx) const {
  auto codegen = GetCodeGen();
  auto attr_name = codegen->MakeIdentifier(kBuildRowAttrPrefix + std::to_string(attr_idx));
  return codegen->AccessStructMember(build_row, attr_name);
}

void HashJoinTranslator::FillBuildRow(WorkContext *ctx, FunctionBuilder *function,
                                      ast::Expr *build_row) const {
  CodeGen *codegen = GetCodeGen();
  const auto child_schema = GetPlan().GetChild(0)->GetOutputSchema();
  for (uint32_t attr_idx = 0; attr_idx < child_schema->GetColumns().size(); attr_idx++) {
    ast::Expr *lhs = GetBuildRowAttribute(build_row, attr_idx);
    ast::Expr *rhs = GetChildOutput(ctx, 0, attr_idx);
    function->Append(codegen->Assign(lhs, rhs));
  }
}

void HashJoinTranslator::InsertIntoJoinHashTable(WorkContext *ctx,
                                                 FunctionBuilder *function) const {
  CodeGen *codegen = GetCodeGen();

  const auto join_ht = ctx->GetPipeline().IsParallel() ? local_join_ht_ : global_join_ht_;

  // var hashVal = @hash(...)
  auto hash_val = HashKeys(ctx, function, GetPlanAs<planner::HashJoinPlanNode>().GetLeftHashKeys());

  // var buildRow = @joinHTInsert(...)
  function->Append(codegen->DeclareVarWithInit(
      build_row_var_,
      codegen->JoinHashTableInsert(join_ht.GetPtr(codegen), hash_val, build_row_type_)));

  // Fill row.
  FillBuildRow(ctx, function, codegen->MakeExpr(build_row_var_));
}

void HashJoinTranslator::ProbeJoinHashTable(WorkContext *ctx, FunctionBuilder *function) const {
  auto codegen = GetCodeGen();

  // var entryIterBase: HashTableEntryIterator
  auto iter_name_base = codegen->MakeFreshIdentifier("entryIterBase");
  function->Append(
      codegen->DeclareVarNoInit(iter_name_base, ast::BuiltinType::HashTableEntryIterator));

  // var entryIter = &entryIterBase
  auto iter_name = codegen->MakeFreshIdentifier("entryIter");
  function->Append(codegen->DeclareVarWithInit(
      iter_name, codegen->AddressOf(codegen->MakeExpr(iter_name_base))));

  auto entry_iter = codegen->MakeExpr(iter_name);
  auto hash_val =
      HashKeys(ctx, function, GetPlanAs<planner::HashJoinPlanNode>().GetRightHashKeys());

  // Loop over matches.
  Loop entry_loop(function,
                  codegen->MakeStmt(codegen->JoinHashTableLookup(global_join_ht_.GetPtr(codegen),
                                                                 entry_iter, hash_val)),
                  codegen->HTEntryIterHasNext(entry_iter), nullptr);
  {
    // var buildRow = @ptrCast(*BuildRow, @htEntryIterGetRow())
    function->Append(codegen->DeclareVarWithInit(
        build_row_var_, codegen->HTEntryIterGetRow(entry_iter, build_row_type_)));

    // Check predicate
    if (const auto join_predicate = GetPlanAs<planner::HashJoinPlanNode>().GetJoinPredicate()) {
      auto cond = ctx->DeriveValue(*join_predicate, this);
      If check_condition(function, cond);
      ctx->Push(function);
    } else {
      ctx->Push(function);
    }
  }
  entry_loop.EndLoop();
}

void HashJoinTranslator::PerformPipelineWork(WorkContext *ctx, FunctionBuilder *function) const {
  if (IsLeftPipeline(ctx->GetPipeline())) {
    InsertIntoJoinHashTable(ctx, function);
  } else {
    TPL_ASSERT(IsRightPipeline(ctx->GetPipeline()), "Pipeline is unknown to join translator");
    ProbeJoinHashTable(ctx, function);
  }
}

void HashJoinTranslator::FinishPipelineWork(const Pipeline &pipeline,
                                            FunctionBuilder *function) const {
  if (IsLeftPipeline(pipeline)) {
    auto codegen = GetCodeGen();
    auto jht = global_join_ht_.GetPtr(codegen);
    if (LeftPipeline().IsParallel()) {
      auto tls = GetThreadStateContainer();
      auto offset = local_join_ht_.OffsetFromState(codegen);
      function->Append(codegen->JoinHashTableBuildParallel(jht, tls, offset));
    } else {
      function->Append(codegen->JoinHashTableBuild(jht));
    }
  }
}

ast::Expr *HashJoinTranslator::GetChildOutput(WorkContext *work_context, uint32_t child_idx,
                                              uint32_t attr_idx) const {
  if (IsRightPipeline(work_context->GetPipeline()) && child_idx == 0) {
    return GetBuildRowAttribute(GetCodeGen()->MakeExpr(build_row_var_), attr_idx);
  }

  const auto child = GetPlan().GetChild(child_idx);
  const auto child_translator = GetCompilationContext()->LookupTranslator(*child);
  return child_translator->GetOutput(work_context, attr_idx);
}

}  // namespace tpl::sql::codegen
