#include "sql/codegen/operators/hash_join_translator.h"

#include <algorithm>

#include "ast/type.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/consumer_context.h"
#include "sql/codegen/function_builder.h"
#include "sql/codegen/if.h"
#include "sql/codegen/loop.h"
#include "sql/planner/plannodes/hash_join_plan_node.h"

namespace tpl::sql::codegen {

HashJoinTranslator::HashJoinTranslator(const planner::HashJoinPlanNode &plan,
                                       CompilationContext *compilation_context, Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline),
      storage_(codegen_, "BuildRow"),
      build_row_var_(codegen_->MakeFreshIdentifier("row")),
      build_mark_index_(plan.GetChild(0)->GetOutputSchema()->NumColumns()),
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

  // Setup compact storage.
  std::vector<TypeId> types;
  types.reserve(GetChildOutputSchema(0)->NumColumns());
  for (const auto &col : GetChildOutputSchema(0)->GetColumns()) {
    types.push_back(col.GetType());
  }
  if (plan.RequiresLeftMark()) {
    types.push_back(TypeId::Boolean);
  }
  storage_.Setup(types);

  // Declare global hash table.
  ast::Expression *join_ht_type = codegen_->BuiltinType(ast::BuiltinType::JoinHashTable);
  global_join_ht_ = compilation_context->GetQueryState()->DeclareStateEntry(
      codegen_, "join_hash_table", join_ht_type);
}

void HashJoinTranslator::DeclarePipelineDependencies() const {
  GetPipeline()->AddDependency(left_pipeline_);
}

void HashJoinTranslator::InitializeJoinHashTable(FunctionBuilder *function,
                                                 ast::Expression *jht_ptr) const {
  function->Append(codegen_->JoinHashTableInit(jht_ptr, GetMemoryPool(), storage_.GetTypeName()));
}

void HashJoinTranslator::TearDownJoinHashTable(FunctionBuilder *function,
                                               ast::Expression *jht_ptr) const {
  function->Append(codegen_->JoinHashTableFree(jht_ptr));
}

void HashJoinTranslator::InitializeQueryState(FunctionBuilder *function) const {
  InitializeJoinHashTable(function, GetQueryStateEntryPtr(global_join_ht_));
}

void HashJoinTranslator::TearDownQueryState(FunctionBuilder *function) const {
  TearDownJoinHashTable(function, GetQueryStateEntryPtr(global_join_ht_));
}

void HashJoinTranslator::DeclarePipelineState(PipelineContext *pipeline_ctx) {
  if (pipeline_ctx->IsForPipeline(left_pipeline_) && pipeline_ctx->IsParallel()) {
    ast::Expression *join_ht_type = codegen_->BuiltinType(ast::BuiltinType::JoinHashTable);
    local_join_ht_ = pipeline_ctx->DeclarePipelineStateEntry("join_hash_table", join_ht_type);
  }
}

void HashJoinTranslator::InitializePipelineState(const PipelineContext &pipeline_ctx,
                                                 FunctionBuilder *function) const {
  if (pipeline_ctx.IsForPipeline(left_pipeline_) && pipeline_ctx.IsParallel()) {
    ast::Expression *local_hash_table = pipeline_ctx.GetStateEntryPtr(local_join_ht_);
    InitializeJoinHashTable(function, local_hash_table);
  }
}

void HashJoinTranslator::TearDownPipelineState(const PipelineContext &pipeline_ctx,
                                               FunctionBuilder *function) const {
  if (pipeline_ctx.IsForPipeline(left_pipeline_) && pipeline_ctx.IsParallel()) {
    ast::Expression *local_hash_table = pipeline_ctx.GetStateEntryPtr(local_join_ht_);
    TearDownJoinHashTable(function, local_hash_table);
  }
}

ast::Identifier HashJoinTranslator::HashKeys(
    ConsumerContext *ctx, FunctionBuilder *function,
    const std::vector<const planner::AbstractExpression *> &hash_keys) const {
  std::vector<ast::Expression *> key_values;
  key_values.reserve(hash_keys.size());
  for (const auto hash_key : hash_keys) {
    key_values.push_back(ctx->DeriveValue(*hash_key, this));
  }

  ast::Identifier hash_val_name = codegen_->MakeFreshIdentifier("hash_val");
  function->Append(codegen_->DeclareVarWithInit(hash_val_name, codegen_->Hash(key_values)));

  return hash_val_name;
}

ast::Expression *HashJoinTranslator::GetBuildRowAttribute(uint32_t attr_idx) const {
  return storage_.ReadSQL(codegen_->MakeExpr(build_row_var_), attr_idx);
}

void HashJoinTranslator::WriteBuildRow(ConsumerContext *context, FunctionBuilder *function) const {
  // @csWrite() for each attribute.
  const auto child_schema = GetPlan().GetChild(0)->GetOutputSchema();
  for (uint32_t attr_idx = 0; attr_idx < child_schema->NumColumns(); attr_idx++) {
    ast::Expression *val = GetChildOutput(context, 0, attr_idx);
    storage_.WriteSQL(codegen_->MakeExpr(build_row_var_), attr_idx, val);
  }

  // @csWrite() the mark, if needed.
  if (GetJoinPlan().RequiresLeftMark()) {
    storage_.WriteSQL(codegen_->MakeExpr(build_row_var_), build_mark_index_,
                      codegen_->BoolToSql(true));
  }
}

void HashJoinTranslator::InsertIntoJoinHashTable(ConsumerContext *context,
                                                 FunctionBuilder *function) const {
  ast::Expression *hash_table = context->IsParallel() ? context->GetStateEntryPtr(local_join_ht_)
                                                      : GetQueryStateEntryPtr(global_join_ht_);

  // var hashVal = @hash(...)
  const auto &left_keys = GetPlanAs<planner::HashJoinPlanNode>().GetLeftHashKeys();
  ast::Identifier hash_val = HashKeys(context, function, left_keys);

  // var buildRow = @joinHTInsert(...)
  ast::Expression *insert_call = codegen_->JoinHashTableInsert(
      hash_table, codegen_->MakeExpr(hash_val), storage_.GetTypeName());
  function->Append(codegen_->DeclareVarWithInit(build_row_var_, insert_call));

  // Fill row.
  WriteBuildRow(context, function);
}

bool HashJoinTranslator::ShouldValidateHashOnProbe() const {
  // Validate hash value on probe if:
  // 1. There is more than one probe key that cannot be packed.
  // 2. One of the keys is considered "complex".
  // For now, only string keys are the only complex type.
  // Modify 'is_complex()' as more complex types are supported.
  const auto &keys = GetJoinPlan().GetRightHashKeys();
  const auto is_complex = [](auto key) { return key->GetReturnValueType() == TypeId::Varchar; };
  return keys.size() > 1 || std::ranges::any_of(keys, is_complex);
}

void HashJoinTranslator::ProbeJoinHashTable(ConsumerContext *ctx, FunctionBuilder *function) const {
  // Compute hash.
  ast::Identifier hash_val = HashKeys(ctx, function, GetJoinPlan().GetRightHashKeys());

  // var entry = @jhtLookup(hash)
  ast::Identifier entry_var = codegen_->MakeFreshIdentifier("entry");
  const auto entry = [&]() { return codegen_->MakeExpr(entry_var); };
  auto lookup_call = codegen_->MakeStatement(codegen_->DeclareVarWithInit(
      entry_var, codegen_->JoinHashTableLookup(GetQueryStateEntryPtr(global_join_ht_),
                                               codegen_->MakeExpr(hash_val))));

  // entry != null
  auto valid_entry = codegen_->Compare(parsing::Token::Type::BANG_EQUAL, entry(), codegen_->Nil());

  // entry = @htEntryGetNext(entry)
  auto next_call = codegen_->Assign(entry(), codegen_->HTEntryGetNext(entry()));

  // TODO(pmenon): Only check/validate hash collisions if using complex keys.
  //               Use HashJoinTranslator::ShouldValidateHashOnProbe()

  // The probe depends on the join type
  if (GetJoinPlan().RequiresRightMark()) {
    // First declare the right mark.
    ast::Identifier right_mark_var = codegen_->MakeFreshIdentifier("right_mark");
    ast::Expression *right_mark = codegen_->MakeExpr(right_mark_var);
    function->Append(codegen_->DeclareVarWithInit(right_mark_var, codegen_->ConstBool(true)));

    // Probe hash table and check for a match. Loop condition becomes false as
    // soon as a match is found.
    auto loop_cond = codegen_->BinaryOp(parsing::Token::Type::AND, right_mark, valid_entry);
    Loop entry_loop(function, lookup_call, loop_cond, next_call);
    {
      // if (entry->hash == hash) {
      //   var buildRow = @ptrCast(*BuildRow, @htEntryIterGetRow())
      //   ...
      // }
      If check_hash(function, codegen_->CompareEq(codegen_->MakeExpr(hash_val),
                                                  codegen_->HTEntryGetHash(entry())));
      {
        ast::Expression *build_row = codegen_->HTEntryGetRow(entry(), storage_.GetTypeName());
        function->Append(codegen_->DeclareVarWithInit(build_row_var_, build_row));
        CheckRightMark(ctx, function, right_mark_var);
      }
    }
    entry_loop.EndLoop();

    if (GetJoinPlan().GetLogicalJoinType() == planner::LogicalJoinType::RIGHT_ANTI) {
      // If the right mark is true, the row didn't find a matches.
      // if (right_mark)
      If right_anti_check(function, codegen_->MakeExpr(right_mark_var));
      ctx->Consume(function);
    } else if (GetJoinPlan().GetLogicalJoinType() == planner::LogicalJoinType::RIGHT_SEMI) {
      // If the right mark is unset, then there is at least one match.
      // if (!right_mark)
      auto cond = codegen_->UnaryOp(parsing::Token::Type::BANG, codegen_->MakeExpr(right_mark_var));
      If right_semi_check(function, cond);
      ctx->Consume(function);
    }
  } else {
    // For regular joins:
    // for (var entry = @jhtLookup(); entry != nil; entry = @htEntryNext(entry)) { }
    Loop entry_loop(function, lookup_call, valid_entry, next_call);
    {
      // if (entry->hash == hash) {
      //   var buildRow = @ptrCast(*BuildRow, @htEntryIterGetRow())
      //   ...
      // }
      If check_hash(function, codegen_->CompareEq(codegen_->MakeExpr(hash_val),
                                                  codegen_->HTEntryGetHash(entry())));
      {
        ast::Expression *build_row = codegen_->HTEntryGetRow(entry(), storage_.GetTypeName());
        function->Append(codegen_->DeclareVarWithInit(build_row_var_, build_row));
        CheckJoinPredicate(ctx, function);
      }
    }
    entry_loop.EndLoop();
  }
}

void HashJoinTranslator::CheckJoinPredicate(ConsumerContext *ctx, FunctionBuilder *function) const {
  auto cond = ctx->DeriveValue(*GetJoinPlan().GetJoinPredicate(), this);
  if (GetJoinPlan().GetLogicalJoinType() == planner::LogicalJoinType::LEFT_SEMI) {
    // For left-semi joins, we also need to make sure the build-side tuple
    // has not already found an earlier join partner. We enforce the check
    // by modifying the join predicate.
    auto left_mark = storage_.ReadSQL(codegen_->MakeExpr(build_row_var_), build_mark_index_);
    cond = codegen_->BinaryOp(parsing::Token::Type::AND, left_mark, cond);
  }

  If check_condition(function, cond);
  {
    if (GetJoinPlan().RequiresLeftMark()) {
      // Mark this tuple as accessed.
      auto val = codegen_->BoolToSql(false);
      storage_.WriteSQL(codegen_->MakeExpr(build_row_var_), build_mark_index_, val);
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
    ast::Expression *global_hash_table = GetQueryStateEntryPtr(global_join_ht_);
    if (left_pipeline_.IsParallel()) {
      ast::Expression *tls = GetThreadStateContainer();
      ast::Expression *offset = pipeline_ctx.GetStateEntryByteOffset(local_join_ht_);
      function->Append(codegen_->JoinHashTableBuildParallel(global_hash_table, tls, offset));
    } else {
      function->Append(codegen_->JoinHashTableBuild(global_hash_table));
    }
  }
}

ast::Expression *HashJoinTranslator::GetChildOutput(ConsumerContext *context, uint32_t child_idx,
                                                    uint32_t attr_idx) const {
  // If the request is in the probe pipeline and for an attribute in the left
  // child, we read it from the probe/materialized build row. Otherwise, we
  // propagate to the appropriate child.
  if (context->IsForPipeline(*GetPipeline()) && child_idx == 0) {
    return GetBuildRowAttribute(attr_idx);
  }
  return OperatorTranslator::GetChildOutput(context, child_idx, attr_idx);
}

void HashJoinTranslator::AnalyzeHashTable(FunctionBuilder *function) {
  // Declare the attributes.
  const auto build_schema = GetChildOutputSchema(0);
  std::vector<ast::Identifier> attributes(build_schema->NumColumns());
  for (uint32_t attr_idx = 0; attr_idx < build_schema->NumColumns(); attr_idx++) {
    attributes[attr_idx] = codegen_->MakeFreshIdentifier("bits_m");
    auto type = codegen_->PrimitiveTplType(build_schema->GetColumn(attr_idx).GetType());
    function->Append(codegen_->DeclareVarNoInit(attributes[attr_idx], type));
  }

  // for (var i = 0; i < n; i++)
  ast::Identifier i_var = codegen_->MakeIdentifier("i"), row_var = codegen_->MakeIdentifier("row");
  const auto i = [&, name = codegen_->MakeIdentifier("i")]() { return codegen_->MakeExpr(name); };
  Loop loop(function,
            codegen_->MakeStatement(
                codegen_->DeclareVar(i_var, codegen_->UInt32Type(), codegen_->Const32(0))),
            codegen_->CompareLt(i(), function->GetParameterByPosition(0)),
            codegen_->Assign(i(), codegen_->Add(i(), codegen_->Const32(1))));
  {
    // var row = rows[i]
    ast::Expression *rows = function->GetParameterByPosition(1);
    function->Append(codegen_->DeclareVarWithInit(row_var, codegen_->ArrayAccess(rows, i())));

    // Read all columns.
    for (uint32_t attr_idx = 0; attr_idx < GetChildOutputSchema(0)->NumColumns(); attr_idx++) {
      function->Append(codegen_->Assign(
          codegen_->MakeExpr(attributes[attr_idx]),
          codegen_->BitOr(codegen_->MakeExpr(attributes[attr_idx]),
                          storage_.ReadPrimitive(codegen_->MakeExpr(row_var), attr_idx))));
    }
  }
  loop.EndLoop();

  for (uint32_t attr_idx = 0; attr_idx < build_schema->NumColumns(); attr_idx++) {
    auto uval = codegen_->CallBuiltin(ast::Builtin::IntCast,
                                      {codegen_->BuiltinType(ast::BuiltinType::UInt64),
                                       codegen_->MakeExpr(attributes[attr_idx])});
    ast::Identifier room = codegen_->MakeFreshIdentifier("room");
    function->Append(
        codegen_->DeclareVarWithInit(room, codegen_->CallBuiltin(ast::Builtin::Ctlz, {uval})));
  }
}

void HashJoinTranslator::GenerateHashTableAnalysisFunction() {
  auto args = codegen_->MakeEmptyFieldList();
  args.resize(3);
  args[0] = codegen_->MakeField("n", codegen_->UInt32Type());
  args[1] = codegen_->MakeField(
      "rows",
      codegen_->ArrayType(0, codegen_->PointerType(codegen_->MakeExpr(storage_.GetTypeName()))));
  args[2] = codegen_->MakeField("stats", codegen_->PointerType(codegen_->Int8Type()));

  auto name = codegen_->MakeFreshIdentifier(GetPipeline()->CreatePipelineFunctionName("AnalyzeHT"));
  FunctionBuilder function(codegen_, name, std::move(args), codegen_->Nil());
  {
    // Analyze hash table.
    AnalyzeHashTable(&function);
  }
  function.Finish();
}

void HashJoinTranslator::GenerateHashTableCompressionFunction() {}

void HashJoinTranslator::DefineStructsAndFunctions() {
  GenerateHashTableAnalysisFunction();
  GenerateHashTableCompressionFunction();
}

}  // namespace tpl::sql::codegen
