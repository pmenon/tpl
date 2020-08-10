#include "sql/codegen/operators/hash_join_translator.h"

#include "ast/type.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/consumer_context.h"
#include "sql/codegen/function_builder.h"
#include "sql/codegen/if.h"
#include "sql/codegen/loop.h"
#include "sql/planner/plannodes/hash_join_plan_node.h"

namespace tpl::sql::codegen {

namespace {
const char *kBuildRowAttrPrefix = "attr";
}  // namespace

HashJoinTranslator::HashJoinTranslator(const planner::HashJoinPlanNode &plan,
                                       CompilationContext *compilation_context, Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline),
      build_row_var_(codegen_->MakeFreshIdentifier("build_row")),
      build_row_type_(codegen_->MakeFreshIdentifier("BuildRow")),
      build_mark_(codegen_->MakeFreshIdentifier("mark")),
      left_pipeline_(this, pipeline->GetPipelineGraph(), Pipeline::Parallelism::Parallel) {
  TPL_ASSERT(!plan.GetLeftHashKeys().empty(), "Hash-join must have join keys from left input");
  TPL_ASSERT(!plan.GetRightHashKeys().empty(), "Hash-join must have join keys from right input");
  TPL_ASSERT(plan.GetJoinPredicate() != nullptr, "Hash-join must have a join predicate!");
  // Register left and right child in their appropriate pipelines.
  compilation_context->Prepare(*plan.GetChild(0), &left_pipeline_);
  compilation_context->Prepare(*plan.GetChild(1), pipeline);

  // Prepare join predicate, left, and right hash keys.
  compilation_context->Prepare(*plan.GetJoinPredicate());
  for (const auto left_hash_key : plan.GetLeftHashKeys()) {
    compilation_context->Prepare(*left_hash_key);
  }
  for (const auto right_hash_key : plan.GetRightHashKeys()) {
    compilation_context->Prepare(*right_hash_key);
  }

  // Declare global state.
  ast::Expr *join_ht_type = codegen_->BuiltinType(ast::BuiltinType::JoinHashTable);
  global_join_ht_ = compilation_context->GetQueryState()->DeclareStateEntry(
      codegen_, "join_hash_table", join_ht_type);
}

void HashJoinTranslator::DeclarePipelineDependencies() const {
  GetPipeline()->AddDependency(left_pipeline_);
}

void HashJoinTranslator::DefineStructsAndFunctions() {
  auto fields = codegen_->MakeEmptyFieldList();
  GetAllChildOutputFields(0, kBuildRowAttrPrefix, &fields);
  if (GetPlanAs<planner::HashJoinPlanNode>().RequiresLeftMark()) {
    fields.push_back(codegen_->MakeField(build_mark_, codegen_->BoolType()));
  }
  codegen_->DeclareStruct(build_row_type_, std::move(fields));
}

void HashJoinTranslator::InitializeJoinHashTable(FunctionBuilder *function,
                                                 ast::Expr *jht_ptr) const {
  function->Append(codegen_->JoinHashTableInit(jht_ptr, GetMemoryPool(), build_row_type_));
}

void HashJoinTranslator::TearDownJoinHashTable(FunctionBuilder *function,
                                               ast::Expr *jht_ptr) const {
  function->Append(codegen_->JoinHashTableFree(jht_ptr));
}

void HashJoinTranslator::InitializeQueryState(FunctionBuilder *function) const {
  ast::Expr *global_hash_table = GetQueryStateEntryPtr(global_join_ht_);
  InitializeJoinHashTable(function, global_hash_table);
}

void HashJoinTranslator::TearDownQueryState(FunctionBuilder *function) const {
  ast::Expr *global_hash_table = GetQueryStateEntryPtr(global_join_ht_);
  TearDownJoinHashTable(function, global_hash_table);
}

void HashJoinTranslator::DeclarePipelineState(PipelineContext *pipeline_ctx) {
  if (pipeline_ctx->IsForPipeline(left_pipeline_) && pipeline_ctx->IsParallel()) {
    ast::Expr *join_ht_type = codegen_->BuiltinType(ast::BuiltinType::JoinHashTable);
    local_join_ht_ = pipeline_ctx->DeclarePipelineStateEntry("join_hash_table", join_ht_type);
  }
}

void HashJoinTranslator::InitializePipelineState(const PipelineContext &pipeline_ctx,
                                                 FunctionBuilder *function) const {
  if (pipeline_ctx.IsForPipeline(left_pipeline_) && pipeline_ctx.IsParallel()) {
    ast::Expr *local_hash_table = pipeline_ctx.GetStateEntryPtr(local_join_ht_);
    InitializeJoinHashTable(function, local_hash_table);
  }
}

void HashJoinTranslator::TearDownPipelineState(const PipelineContext &pipeline_ctx,
                                               FunctionBuilder *function) const {
  if (pipeline_ctx.IsForPipeline(left_pipeline_) && pipeline_ctx.IsParallel()) {
    ast::Expr *local_hash_table = pipeline_ctx.GetStateEntryPtr(local_join_ht_);
    TearDownJoinHashTable(function, local_hash_table);
  }
}

ast::Expr *HashJoinTranslator::HashKeys(
    ConsumerContext *ctx, FunctionBuilder *function,
    const std::vector<const planner::AbstractExpression *> &hash_keys) const {
  std::vector<ast::Expr *> key_values;
  key_values.reserve(hash_keys.size());
  for (const auto hash_key : hash_keys) {
    key_values.push_back(ctx->DeriveValue(*hash_key, this));
  }

  ast::Identifier hash_val_name = codegen_->MakeFreshIdentifier("hash_val");
  function->Append(codegen_->DeclareVarWithInit(hash_val_name, codegen_->Hash(key_values)));

  return codegen_->MakeExpr(hash_val_name);
}

ast::Expr *HashJoinTranslator::GetBuildRowAttribute(ast::Expr *build_row, uint32_t attr_idx) const {
  auto attr_name = codegen_->MakeIdentifier(kBuildRowAttrPrefix + std::to_string(attr_idx));
  return codegen_->AccessStructMember(build_row, attr_name);
}

void HashJoinTranslator::FillBuildRow(ConsumerContext *ctx, FunctionBuilder *function,
                                      ast::Expr *build_row) const {
  const auto child_schema = GetPlan().GetChild(0)->GetOutputSchema();
  for (uint32_t attr_idx = 0; attr_idx < child_schema->GetColumns().size(); attr_idx++) {
    ast::Expr *lhs = GetBuildRowAttribute(build_row, attr_idx);
    ast::Expr *rhs = GetChildOutput(ctx, 0, attr_idx);
    function->Append(codegen_->Assign(lhs, rhs));
  }
  const auto &join_plan = GetPlanAs<planner::HashJoinPlanNode>();
  if (join_plan.RequiresLeftMark()) {
    ast::Expr *lhs = codegen_->AccessStructMember(build_row, build_mark_);
    ast::Expr *rhs = codegen_->ConstBool(true);
    function->Append(codegen_->Assign(lhs, rhs));
  }
}

void HashJoinTranslator::InsertIntoJoinHashTable(ConsumerContext *context,
                                                 FunctionBuilder *function) const {
  ast::Expr *hash_table = context->IsParallel() ? context->GetStateEntryPtr(local_join_ht_)
                                                : GetQueryStateEntryPtr(global_join_ht_);

  // var hashVal = @hash(...)
  const auto &left_keys = GetPlanAs<planner::HashJoinPlanNode>().GetLeftHashKeys();
  ast::Expr *hash_val = HashKeys(context, function, left_keys);

  // var buildRow = @joinHTInsert(...)
  ast::Expr *insert_call = codegen_->JoinHashTableInsert(hash_table, hash_val, build_row_type_);
  function->Append(codegen_->DeclareVarWithInit(build_row_var_, insert_call));

  // Fill row.
  FillBuildRow(context, function, codegen_->MakeExpr(build_row_var_));
}

void HashJoinTranslator::ProbeJoinHashTable(ConsumerContext *ctx, FunctionBuilder *function) const {
  const auto &join_plan = GetPlanAs<planner::HashJoinPlanNode>();

  // var entryIterBase: HashTableEntryIterator
  ast::Identifier iter_name_base = codegen_->MakeFreshIdentifier("entry_iter_base");
  function->Append(
      codegen_->DeclareVarNoInit(iter_name_base, ast::BuiltinType::HashTableEntryIterator));

  // var entryIter = &entryIterBase
  ast::Identifier iter_name = codegen_->MakeFreshIdentifier("entry_iter");
  function->Append(codegen_->DeclareVarWithInit(
      iter_name, codegen_->AddressOf(codegen_->MakeExpr(iter_name_base))));

  const auto entry_iter = [&]() { return codegen_->MakeExpr(iter_name); };
  ast::Expr *hash_val = HashKeys(ctx, function, join_plan.GetRightHashKeys());

  // Probe matches.

  auto lookup_call = codegen_->MakeStmt(codegen_->JoinHashTableLookup(
      GetQueryStateEntryPtr(global_join_ht_), entry_iter(), hash_val));
  auto has_next_call = codegen_->HTEntryIterHasNext(entry_iter());

  // The probe depends on the join type
  if (join_plan.RequiresRightMark()) {
    // First declare the right mark.
    ast::Identifier right_mark_var = codegen_->MakeFreshIdentifier("right_mark");
    ast::Expr *right_mark = codegen_->MakeExpr(right_mark_var);
    function->Append(codegen_->DeclareVarWithInit(right_mark_var, codegen_->ConstBool(true)));

    // Probe hash table and check for a match. Loop condition becomes false as
    // soon as a match is found.
    auto loop_cond = codegen_->BinaryOp(parsing::Token::Type::AND, right_mark, has_next_call);
    Loop entry_loop(function, lookup_call, loop_cond, nullptr);
    {
      // var buildRow = @ptrCast(*BuildRow, @htEntryIterGetRow())
      ast::Expr *build_row = codegen_->HTEntryIterGetRow(entry_iter(), build_row_type_);
      function->Append(codegen_->DeclareVarWithInit(build_row_var_, build_row));
      CheckRightMark(ctx, function, right_mark_var);
    }
    entry_loop.EndLoop();

    // The next step depends on the join type.
    if (join_plan.GetLogicalJoinType() == planner::LogicalJoinType::RIGHT_ANTI) {
      // If the right mark is true, then we can perform the anti join.
      // if (right_mark)
      If right_anti_check(function, codegen_->MakeExpr(right_mark_var));
      ctx->Consume(function);
      right_anti_check.EndIf();
    } else if (join_plan.GetLogicalJoinType() == planner::LogicalJoinType::RIGHT_SEMI) {
      // If the right mark is unset, then there is at least one match.
      // if (!right_mark)
      auto cond = codegen_->UnaryOp(parsing::Token::Type::BANG, codegen_->MakeExpr(right_mark_var));
      If right_semi_check(function, cond);
      ctx->Consume(function);
      right_semi_check.EndIf();
    }
  } else {
    // For regular joins: while (has_next)
    Loop entry_loop(function, lookup_call, has_next_call, nullptr);
    {
      // var buildRow = @ptrCast(*BuildRow, @htEntryIterGetRow())
      ast::Expr *build_row = codegen_->HTEntryIterGetRow(entry_iter(), build_row_type_);
      function->Append(codegen_->DeclareVarWithInit(build_row_var_, build_row));
      CheckJoinPredicate(ctx, function);
    }
    entry_loop.EndLoop();
  }
}

void HashJoinTranslator::CheckJoinPredicate(ConsumerContext *ctx, FunctionBuilder *function) const {
  const auto &join_plan = GetPlanAs<planner::HashJoinPlanNode>();

  auto cond = ctx->DeriveValue(*join_plan.GetJoinPredicate(), this);
  if (join_plan.RequiresLeftMark()) {
    // For left-semi joins, we also need to make sure the build-side tuple
    // has not already found an earlier join partner. We enforce the check
    // by modifying the join predicate.
    auto left_mark = codegen_->AccessStructMember(codegen_->MakeExpr(build_row_var_), build_mark_);
    cond = codegen_->BinaryOp(parsing::Token::Type::AND, left_mark, cond);
  }

  If check_condition(function, cond);
  {
    if (join_plan.RequiresLeftMark()) {
      // Mark this tuple as accessed.
      auto left_mark =
          codegen_->AccessStructMember(codegen_->MakeExpr(build_row_var_), build_mark_);
      function->Append(codegen_->Assign(left_mark, codegen_->ConstBool(false)));
    }
    // Move along.
    ctx->Consume(function);
  }
  check_condition.EndIf();
}

void HashJoinTranslator::CheckRightMark(ConsumerContext *ctx, FunctionBuilder *function,
                                        ast::Identifier right_mark) const {
  // Generate the join condition.
  const auto join_predicate = GetPlanAs<planner::HashJoinPlanNode>().GetJoinPredicate();
  auto cond = ctx->DeriveValue(*join_predicate, this);

  If check_condition(function, cond);
  {
    // If there is a match, unset the right mark now.
    function->Append(codegen_->Assign(codegen_->MakeExpr(right_mark), codegen_->ConstBool(false)));
  }
  check_condition.EndIf();
}

void HashJoinTranslator::Consume(ConsumerContext *context, FunctionBuilder *function) const {
  if (context->IsForPipeline(left_pipeline_)) {
    InsertIntoJoinHashTable(context, function);
  } else {
    TPL_ASSERT(context->IsForPipeline(*GetPipeline()), "Pipeline is unknown to join translator");
    ProbeJoinHashTable(context, function);
  }
}

void HashJoinTranslator::FinishPipelineWork(const PipelineContext &pipeline_ctx,
                                            FunctionBuilder *function) const {
  if (pipeline_ctx.IsForPipeline(left_pipeline_)) {
    ast::Expr *global_hash_table = GetQueryStateEntryPtr(global_join_ht_);
    if (left_pipeline_.IsParallel()) {
      ast::Expr *tls = GetThreadStateContainer();
      ast::Expr *offset = pipeline_ctx.GetStateEntryByteOffset(local_join_ht_);
      function->Append(codegen_->JoinHashTableBuildParallel(global_hash_table, tls, offset));
    } else {
      function->Append(codegen_->JoinHashTableBuild(global_hash_table));
    }
  }
}

ast::Expr *HashJoinTranslator::GetChildOutput(ConsumerContext *context, uint32_t child_idx,
                                              uint32_t attr_idx) const {
  // If the request is in the probe pipeline and for an attribute in the left
  // child, we read it from the probe/materialized build row. Otherwise, we
  // propagate to the appropriate child.
  if (context->IsForPipeline(*GetPipeline()) && child_idx == 0) {
    return GetBuildRowAttribute(codegen_->MakeExpr(build_row_var_), attr_idx);
  }
  return OperatorTranslator::GetChildOutput(context, child_idx, attr_idx);
}

}  // namespace tpl::sql::codegen
