#include "sql/codegen/operators/hash_join_translator.h"

#include <algorithm>

#include "ast/type.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/consumer_context.h"
#include "sql/codegen/edsl/boolean_ops.h"
#include "sql/codegen/edsl/comparison_ops.h"
#include "sql/codegen/edsl/ops.h"
#include "sql/codegen/edsl/value.h"
#include "sql/codegen/edsl/value_vt.h"
#include "sql/codegen/function_builder.h"
#include "sql/codegen/if.h"
#include "sql/codegen/loop.h"
#include "sql/planner/plannodes/hash_join_plan_node.h"

namespace tpl::sql::codegen {

HashJoinTranslator::HashJoinTranslator(const planner::HashJoinPlanNode &plan,
                                       CompilationContext *compilation_context, Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline),
      storage_(codegen_, "BuildRow"),
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
  global_join_ht_ = compilation_context->GetQueryState()->DeclareStateEntry(
      "join_ht", codegen_->GetType<ast::x::JoinHashTable>());

  // Make the variable we use to hold rows.
  build_row_ = std::make_unique<edsl::VariableVT>(codegen_, "row", storage_.GetPtrToType());
}

void HashJoinTranslator::DeclarePipelineDependencies() const {
  GetPipeline()->AddDependency(left_pipeline_);
}

void HashJoinTranslator::InitializeQueryState(FunctionBuilder *function) const {
  auto jht = GetQueryStateEntryPtr<ast::x::JoinHashTable>(global_join_ht_);
  function->Append(jht->Init(GetMemoryPool(), storage_.GetTypeSize()));
}

void HashJoinTranslator::TearDownQueryState(FunctionBuilder *function) const {
  auto jht = GetQueryStateEntryPtr<ast::x::JoinHashTable>(global_join_ht_);
  function->Append(jht->Free());
}

void HashJoinTranslator::DeclarePipelineState(PipelineContext *pipeline_ctx) {
  if (pipeline_ctx->IsForPipeline(left_pipeline_) && pipeline_ctx->IsParallel()) {
    local_join_ht_ = pipeline_ctx->DeclarePipelineStateEntry(
        "join_ht", codegen_->GetType<ast::x::JoinHashTable>());
  }
}

void HashJoinTranslator::InitializePipelineState(const PipelineContext &pipeline_ctx,
                                                 FunctionBuilder *function) const {
  if (pipeline_ctx.IsForPipeline(left_pipeline_) && pipeline_ctx.IsParallel()) {
    auto jht = pipeline_ctx.GetStateEntryPtr<ast::x::JoinHashTable>(local_join_ht_);
    function->Append(jht->Init(GetMemoryPool(), storage_.GetTypeSize()));
  }
}

void HashJoinTranslator::TearDownPipelineState(const PipelineContext &pipeline_ctx,
                                               FunctionBuilder *function) const {
  if (pipeline_ctx.IsForPipeline(left_pipeline_) && pipeline_ctx.IsParallel()) {
    auto jht = pipeline_ctx.GetStateEntryPtr<ast::x::JoinHashTable>(local_join_ht_);
    function->Append(jht->Free());
  }
}

edsl::Variable<hash_t> HashJoinTranslator::HashKeys(ConsumerContext *ctx, FunctionBuilder *function,
                                                    bool left) const {
  const auto &keys = left ? GetJoinPlan().GetLeftHashKeys() : GetJoinPlan().GetRightHashKeys();

  std::vector<edsl::ValueVT> key_values;
  key_values.reserve(keys.size());
  for (const auto hash_key : keys) {
    key_values.push_back(ctx->DeriveValue(*hash_key, this));
  }

  edsl::Variable<hash_t> hash_val(codegen_, "hash_val");
  function->Append(edsl::Declare(hash_val, edsl::Hash(key_values)));
  return hash_val;
}

edsl::ValueVT HashJoinTranslator::GetBuildRowAttribute(uint32_t attr_idx) const {
  return storage_.ReadSQL(*build_row_, attr_idx);
}

void HashJoinTranslator::WriteBuildRow(ConsumerContext *context, FunctionBuilder *function) const {
  // @csWrite() for each attribute.
  const auto child_schema = GetPlan().GetChild(0)->GetOutputSchema();
  for (uint32_t attr_idx = 0; attr_idx < child_schema->NumColumns(); attr_idx++) {
    storage_.WriteSQL(*build_row_, attr_idx, GetChildOutput(context, 0, attr_idx));
  }

  // @csWrite() the mark, if needed.
  if (GetJoinPlan().RequiresLeftMark()) {
    storage_.WritePrimitive(*build_row_, build_mark_index_, edsl::Literal<bool>(codegen_, true));
  }
}

void HashJoinTranslator::InsertIntoJoinHashTable(ConsumerContext *context,
                                                 FunctionBuilder *function) const {
  auto hash_table = context->IsParallel()
                        ? context->GetStateEntryPtr<ast::x::JoinHashTable>(local_join_ht_)
                        : GetQueryStateEntryPtr<ast::x::JoinHashTable>(global_join_ht_);

  // var hashVal = @hash(...)
  edsl::Variable<hash_t> hash_val = HashKeys(context, function, true);

  // var buildRow = @joinHTInsert(...)
  function->Append(edsl::Declare(
      *build_row_, edsl::PtrCast(storage_.GetPtrToType(), hash_table->Insert(hash_val))));

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
  // The hash table.
  auto hash_table = GetQueryStateEntryPtr<ast::x::JoinHashTable>(global_join_ht_);

  edsl::Variable<ast::x::HashTableEntry *> entry(codegen_, "entry");
  edsl::Variable<hash_t> hash_val = HashKeys(ctx, function, false);

  if (GetJoinPlan().RequiresRightMark()) {
    edsl::Variable<bool> right_mark(codegen_, "right_mark");
    function->Append(edsl::Declare(right_mark, true));

    // Probe hash table and check for a match. Loop condition becomes false as
    // soon as a match is found.
    Loop entry_loop(function, edsl::Declare(entry, hash_table->Lookup(hash_val)),
                    entry != nullptr && right_mark, edsl::Assign(entry, entry->Next()));
    {
      If check_hash(function, hash_val == entry->GetHash());
      {
        function->Append(
            edsl::Declare(*build_row_, edsl::PtrCast(storage_.GetPtrToType(), entry->GetRow())));
        CheckRightMark(ctx, function, right_mark);
      }
    }
    entry_loop.EndLoop();

    if (GetJoinPlan().GetLogicalJoinType() == planner::LogicalJoinType::RIGHT_ANTI) {
      // If the right mark is true, the row didn't find a matches.
      If right_anti_check(function, right_mark);
      ctx->Consume(function);
    } else if (GetJoinPlan().GetLogicalJoinType() == planner::LogicalJoinType::RIGHT_SEMI) {
      // If the right mark is unset, then there is at least one match.
      If right_semi_check(function, !right_mark);
      ctx->Consume(function);
    }
  } else {
    Loop entry_loop(function, edsl::Declare(entry, hash_table->Lookup(hash_val)), entry != nullptr,
                    edsl::Assign(entry, entry->Next()));
    {
      If check_hash(function, hash_val == entry->GetHash());
      {
        function->Append(
            edsl::Declare(*build_row_, edsl::PtrCast(storage_.GetPtrToType(), entry->GetRow())));
        CheckJoinPredicate(ctx, function);
      }
    }
    entry_loop.EndLoop();
  }
}

void HashJoinTranslator::CheckJoinPredicate(ConsumerContext *ctx, FunctionBuilder *function) const {
  const auto process = [&](const edsl::Value<bool> &condition) {
    If check_condition(function, condition);
    {  // Valid tuple, pass to next pipeline operator.
      if (GetJoinPlan().RequiresLeftMark()) {
        storage_.WritePrimitive(*build_row_, build_mark_index_, edsl::Literal<bool>(codegen_, false));
      }
      ctx->Consume(function);
    }
  };

  // Derive the value of the join condition.
  edsl::Value<bool> join_condition = ctx->DeriveValue(*GetJoinPlan().GetJoinPredicate(), this);

  if (GetJoinPlan().GetLogicalJoinType() == planner::LogicalJoinType::LEFT_SEMI) {
    // For left-semi joins, we also need to make sure the build-side tuple
    // has not already found an earlier join partner. We enforce the check
    // by modifying the join predicate.
    edsl::Value<bool> left_mark = storage_.ReadPrimitive(*build_row_, build_mark_index_);
    process(join_condition && left_mark);
  } else {
    process(join_condition);
  }
}

void HashJoinTranslator::CheckRightMark(ConsumerContext *ctx, FunctionBuilder *function,
                                        const edsl::Variable<bool> &right_mark) const {
  // Derive the value of the join condition.
  edsl::Value<bool> join_condition(ctx->DeriveValue(*GetJoinPlan().GetJoinPredicate(), this));

  If check_condition(function, join_condition);
  {  // Set the mark if valid tuple pair.
    function->Append(edsl::Assign(right_mark, false));
  }
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
    auto hash_table = GetQueryStateEntryPtr<ast::x::JoinHashTable>(global_join_ht_);
    if (left_pipeline_.IsParallel()) {
      auto offset = pipeline_ctx.GetStateEntryByteOffset(local_join_ht_);
      function->Append(hash_table->BuildParallel(GetThreadStateContainer(), offset));
    } else {
      function->Append(hash_table->Build());
    }
  }
}

edsl::ValueVT HashJoinTranslator::GetChildOutput(ConsumerContext *context, uint32_t child_idx,
                                                 uint32_t attr_idx) const {
  // If the request is in the probe pipeline and for an attribute in the left
  // child, we read it from the probe/materialized build row. Otherwise, we
  // propagate to the appropriate child.
  if (context->IsForPipeline(*GetPipeline()) && child_idx == 0) {
    return GetBuildRowAttribute(attr_idx);
  }
  return OperatorTranslator::GetChildOutput(context, child_idx, attr_idx);
}

}  // namespace tpl::sql::codegen
