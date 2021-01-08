#include "sql/codegen/operators/hash_aggregation_translator.h"

#include <string_view>

// For string formatting.
#include "spdlog/fmt/fmt.h"

#include "sql/codegen/codegen.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/edsl/comparison_ops.h"
#include "sql/codegen/edsl/ops.h"
#include "sql/codegen/function_builder.h"
#include "sql/codegen/if.h"
#include "sql/codegen/loop.h"
#include "sql/planner/plannodes/aggregate_plan_node.h"

namespace tpl::sql::codegen {

namespace {
constexpr std::string_view kGroupByKeyPrefix = "gb_key";
constexpr std::string_view kAggregateTermPrefix = "agg";
}  // namespace

HashAggregationTranslator::HashAggregationTranslator(const planner::AggregatePlanNode &plan,
                                                     CompilationContext *compilation_context,
                                                     Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline),
      agg_payload_(codegen_, "AggPayload", true),
      merge_partitions_fn_(
          codegen_->MakeFreshIdentifier(pipeline->CreatePipelineFunctionName("MergePartitions"))),
      build_pipeline_(this, pipeline->GetPipelineGraph(), Pipeline::Parallelism::Parallel) {
  TPL_ASSERT(!plan.GetGroupByTerms().empty(), "Hash aggregation should have grouping keys");
  TPL_ASSERT(plan.GetAggregateStrategyType() == planner::AggregateStrategyType::HASH,
             "Expected hash-based aggregation plan node");
  TPL_ASSERT(plan.GetChildrenSize() == 1, "Hash aggregations should only have one child");
  // Prepare the child.
  compilation_context->Prepare(*plan.GetChild(0), &build_pipeline_);

  // If the build-side is parallel, the produce side is parallel.
  pipeline->RegisterSource(this, build_pipeline_.IsParallel() ? Pipeline::Parallelism::Parallel
                                                              : Pipeline::Parallelism::Serial);

  // Prepare all grouping and aggregate expressions.
  for (const auto group_by_term : plan.GetGroupByTerms()) {
    compilation_context->Prepare(*group_by_term);
  }
  for (const auto agg_term : plan.GetAggregateTerms()) {
    compilation_context->Prepare(*agg_term->GetChild(0));
  }

  // If there's a having clause, prepare it, too.
  if (const auto having_clause = plan.GetHavingClausePredicate(); having_clause != nullptr) {
    compilation_context->Prepare(*having_clause);
  }

  // Declare the global hash table.
  global_agg_ht_ = GetQueryState()->DeclareStateEntry<ast::x::AggregationHashTable>("agg_ht");
}

void HashAggregationTranslator::DeclarePipelineDependencies() const {
  GetPipeline()->AddDependency(build_pipeline_);
}

void HashAggregationTranslator::GeneratePayloadStruct() {
  // Create a field for every group by term.
  for (uint32_t idx = 0; const auto term : GetAggPlan().GetGroupByTerms()) {
    auto name = fmt::format("{}{}", kGroupByKeyPrefix, idx++);
    auto type = codegen_->GetTPLType(term->GetReturnValueType().GetTypeId());
    agg_payload_.AddMember(name, type);
  }

  // Create a field for every aggregate term.
  for (uint32_t idx = 0; const auto term : GetAggPlan().GetAggregateTerms()) {
    auto name = fmt::format("{}{}", kAggregateTermPrefix, idx++);
    auto sql_type = term->GetReturnValueType();
    auto type = codegen_->AggregateType(term->GetKind(), sql_type.GetPrimitiveTypeId());
    agg_payload_.AddMember(name, type);
  }

  agg_payload_.Seal();
}

std::size_t HashAggregationTranslator::KeyId(std::size_t id) const { return id; }

std::size_t HashAggregationTranslator::AggId(std::size_t id) const {
  return id + GetAggPlan().NumGroupByTerms();
}

void HashAggregationTranslator::GenerateMergeOverflowPartitionsFunction() {
  // The partition merge fn has the following signature:
  // (*QueryState, *AggregationHashTable, *AHTOverflowPartitionIterator) -> nil

  auto params = GetCompilationContext()->QueryParams();

  // Then the aggregation hash table and the overflow partition iterator.
  params.emplace_back(codegen_->MakeIdentifier("aht"),
                      codegen_->GetType<ast::x::AggregationHashTable *>());
  params.emplace_back(codegen_->MakeIdentifier("aht_ovf_iter"),
                      codegen_->GetType<ast::x::AHTOverflowPartitionIterator *>());

  FunctionBuilder fn(codegen_, merge_partitions_fn_, std::move(params), codegen_->NilType());
  {
    auto hash_table = fn.GetParameterByPosition(1).As<ast::x::AggregationHashTable *>();
    auto iter = fn.GetParameterByPosition(2).As<ast::x::AHTOverflowPartitionIterator *>();
    auto hash_val = edsl::Variable<hash_t>(codegen_, "hash_val");
    auto partial = edsl::VariableVT(codegen_, "partial", agg_payload_.GetPtrToType());
    Loop loop(&fn, nullptr, iter->HasNext(), iter->Next());
    {
      fn.Append(edsl::Declare(hash_val, iter->GetHash()));
      fn.Append(edsl::Declare(partial, edsl::PtrCast(agg_payload_.GetPtrToType(), iter->GetRow())));

      std::vector<edsl::ReferenceVT> keys, vals;
      keys.reserve(GetAggPlan().NumGroupByTerms());
      vals.reserve(GetAggPlan().NumAggregateTerms());
      for (uint32_t i = 0; i < GetAggPlan().NumGroupByTerms(); i++) {
        keys.emplace_back(agg_payload_.GetMember(partial, KeyId(i)));
      }
      for (uint32_t i = 0; i < GetAggPlan().NumAggregateTerms(); i++) {
        vals.emplace_back(agg_payload_.GetMember(partial, AggId(i)));
      }

      // Lookup.
      PerformLookup(&fn, hash_table, hash_val, keys);
      If check_found(&fn, edsl::IsNilPtr(*agg_row_));
      {  // Entry doesn't exist in table, link it in directly.
        fn.Append(hash_table->LinkEntry(iter->GetRowEntry()));
      }
      check_found.Else();
      {  // Entry exists, merge the partial aggregate.
        for (uint32_t i = 0; i < GetAggPlan().NumAggregateTerms(); i++) {
          auto agg = agg_payload_.GetMemberPtr(*agg_row_, AggId(i));
          fn.Append(edsl::AggregatorMerge(agg, vals[i].Addr()));
        }
      }
      check_found.EndIf();
    }
  }
  fn.Finish();
}

void HashAggregationTranslator::DefineStructsAndFunctions() {
  GeneratePayloadStruct();
  agg_row_ = std::make_unique<edsl::VariableVT>(codegen_, "agg_row", agg_payload_.GetPtrToType());
}

void HashAggregationTranslator::DefinePipelineFunctions(const PipelineContext &pipeline_ctx) {
  if (pipeline_ctx.IsForPipeline(build_pipeline_) && pipeline_ctx.IsParallel()) {
    GenerateMergeOverflowPartitionsFunction();
  }
}

void HashAggregationTranslator::InitializeQueryState(FunctionBuilder *function) const {
  auto hash_table = GetQueryStateEntryPtr(global_agg_ht_);
  function->Append(hash_table->Init(GetMemoryPool(), agg_payload_.GetSize()));
}

void HashAggregationTranslator::TearDownQueryState(FunctionBuilder *function) const {
  auto hash_table = GetQueryStateEntryPtr(global_agg_ht_);
  function->Append(hash_table->Free());
}

void HashAggregationTranslator::DeclarePipelineState(PipelineContext *pipeline_ctx) {
  if (pipeline_ctx->IsForPipeline(build_pipeline_) && pipeline_ctx->IsParallel()) {
    local_agg_ht_ = pipeline_ctx->DeclarePipelineStateEntry<ast::x::AggregationHashTable>("agg_ht");
  }
}

void HashAggregationTranslator::InitializePipelineState(const PipelineContext &pipeline_ctx,
                                                        FunctionBuilder *function) const {
  if (pipeline_ctx.IsForPipeline(build_pipeline_) && pipeline_ctx.IsParallel()) {
    auto hash_table = pipeline_ctx.GetStateEntryPtr(local_agg_ht_);
    function->Append(hash_table->Init(GetMemoryPool(), agg_payload_.GetSize()));
  }
}

void HashAggregationTranslator::TearDownPipelineState(const PipelineContext &pipeline_ctx,
                                                      FunctionBuilder *function) const {
  if (pipeline_ctx.IsForPipeline(build_pipeline_) && pipeline_ctx.IsParallel()) {
    auto hash_table = pipeline_ctx.GetStateEntryPtr(local_agg_ht_);
    function->Append(hash_table->Free());
  }
}

template <typename T>
void HashAggregationTranslator::PerformLookup(
    FunctionBuilder *function, const edsl::Value<ast::x::AggregationHashTable *> &hash_table,
    const edsl::Value<hash_t> &hash_val, const std::vector<T> &keys) const {
  auto entry = edsl::Variable<ast::x::HashTableEntry *>(codegen_, "entry");
  auto temp = edsl::VariableVT(codegen_, "temp", agg_payload_.GetPtrToType());

  function->Append(edsl::Declare(*agg_row_, edsl::Nil(codegen_, agg_payload_.GetPtrToType())));
  Loop entry_loop(function, edsl::Declare(entry, hash_table->Lookup(hash_val)),
                  edsl::ComparisonOp(parsing::Token::Type::EQUAL_EQUAL, *agg_row_,
                                     edsl::Nil(codegen_, agg_payload_.GetPtrToType())) &&
                      entry != nullptr,
                  edsl::Assign(entry, entry->Next()));
  {
    If check_hash(function, hash_val == entry->GetHash());
    {  // Hash match. Check key.
      function->Append(edsl::Declare(temp, edsl::PtrCast(temp.GetType(), entry->GetRow())));
      std::vector<edsl::Value<bool>> cmps;
      for (uint32_t i = 0; i < GetAggPlan().NumGroupByTerms(); i++) {
        auto lhs = agg_payload_.GetMember(temp, KeyId(i));
        auto result = edsl::ComparisonOp(parsing::Token::Type::EQUAL_EQUAL, lhs, keys[i]);
        if (cmps.empty()) {
          cmps.emplace_back(result);
        } else {
          cmps.emplace_back(cmps.back() && result);
        }
      }
      If check_key(function, cmps.back());
      {  // Found match!
        function->Append(edsl::Assign(*agg_row_, temp));
      }
    }
  }
  entry_loop.EndLoop();
}

template <typename T>
void HashAggregationTranslator::ConstructNewAggregate(
    FunctionBuilder *function, const edsl::Value<ast::x::AggregationHashTable *> &hash_table,
    const edsl::Value<hash_t> &hash_val, const std::vector<T> &keys) const {
  const bool partitioned = build_pipeline_.IsParallel();
  const auto bytes = hash_table->Insert(hash_val, edsl::Literal<bool>(codegen_, partitioned));
  function->Append(edsl::Assign(*agg_row_, edsl::PtrCast(agg_payload_.GetPtrToType(), bytes)));

  // Copy the grouping keys.
  for (uint32_t i = 0; i < GetAggPlan().NumGroupByTerms(); i++) {
    auto key = agg_payload_.GetMember(*agg_row_, KeyId(i));
    function->Append(edsl::Assign(key, keys[i]));
  }

  // Initialize all aggregate terms.
  for (uint32_t i = 0; i < GetAggPlan().NumAggregateTerms(); i++) {
    auto agg = agg_payload_.GetMemberPtr(*agg_row_, AggId(i));
    function->Append(edsl::AggregatorInit(agg));
  }
}

template <typename T>
void HashAggregationTranslator::AdvanceAggregate(FunctionBuilder *function,
                                                 const std::vector<T> &vals) const {
  for (uint32_t i = 0; i < GetAggPlan().NumAggregateTerms(); i++) {
    auto agg = agg_payload_.GetMemberPtr(*agg_row_, AggId(i));
    function->Append(edsl::AggregatorAdvance(agg, vals[i]));
  }
}

void HashAggregationTranslator::UpdateAggregates(
    ConsumerContext *context, FunctionBuilder *function,
    const edsl::Variable<ast::x::AggregationHashTable *> &hash_table) const {
  // Construct variables for all keys and values.
  std::vector<edsl::VariableVT> keys, vals;
  keys.reserve(GetAggPlan().NumGroupByTerms());
  vals.reserve(GetAggPlan().NumAggregateTerms());
  for (uint32_t idx = 0; const auto &term : GetAggPlan().GetGroupByTerms()) {
    auto key_name = fmt::format("{}{}", kGroupByKeyPrefix, idx++);
    auto key_type = codegen_->GetTPLType(term->GetReturnValueType().GetTypeId());
    keys.emplace_back(codegen_, key_name, key_type);
  }
  for (uint32_t idx = 0; const auto &term : GetAggPlan().GetAggregateTerms()) {
    auto val_name = fmt::format("{}{}", kAggregateTermPrefix, idx++);
    auto val_type = codegen_->GetTPLType(term->GetReturnValueType().GetTypeId());
    vals.emplace_back(codegen_, val_name, val_type);
  }

  // Assign keys and values to locals.
  for (uint32_t idx = 0; const auto &term : GetAggPlan().GetGroupByTerms()) {
    function->Append(edsl::Declare(keys[idx++], context->DeriveValue(*term, this)));
  }
  for (uint32_t idx = 0; const auto &term : GetAggPlan().GetAggregateTerms()) {
    function->Append(edsl::Declare(vals[idx++], context->DeriveValue(*term->GetChild(0), this)));
  }

  // Hash the keys.
  auto hash_val = edsl::Variable<hash_t>(codegen_, "hash_val");
  function->Append(edsl::Declare(hash_val, edsl::Hash(keys)));

  // Lookup.
  PerformLookup(function, hash_table, hash_val, keys);
  If check_new_agg(function, edsl::IsNilPtr(*agg_row_));
  {  // New aggregate.
    ConstructNewAggregate(function, hash_table, hash_val, keys);
  }
  check_new_agg.EndIf();

  // Advance aggregate.
  AdvanceAggregate(function, vals);
}

void HashAggregationTranslator::ScanAggregationHashTable(
    ConsumerContext *context, FunctionBuilder *function,
    const edsl::Variable<ast::x::AggregationHashTable *> &hash_table) const {
  auto iter = edsl::Variable<ast::x::AHTIterator *>(codegen_, "iter");
  auto iter_base = edsl::Variable<ast::x::AHTIterator>(codegen_, "iter_base");

  function->Append(edsl::Declare(iter_base));
  function->Append(edsl::Declare(iter, iter_base.Addr()));

  Loop loop(function, iter->Init(hash_table), iter->HasNext(), iter->Next());
  {
    // var agg_row = @ahtIterGetRow()
    function->Append(
        edsl::Declare(*agg_row_, edsl::PtrCast(agg_payload_.GetPtrToType(), iter->GetRow())));

    if (const auto having = GetAggPlan().GetHavingClausePredicate(); having != nullptr) {
      If check_having(function, context->DeriveValue(*having, this).As<bool>());
      context->Consume(function);
    } else {
      context->Consume(function);
    }
  }
  loop.EndLoop();

  function->Append(iter->Close());
}

void HashAggregationTranslator::Consume(ConsumerContext *context, FunctionBuilder *function) const {
  if (context->IsForPipeline(build_pipeline_)) {
    auto hash_table = edsl::Variable<ast::x::AggregationHashTable *>(codegen_, "agg_ht");
    if (context->IsParallel()) {
      function->Append(edsl::Declare(hash_table, context->GetStateEntryPtr(local_agg_ht_)));
      UpdateAggregates(context, function, hash_table);
    } else {
      function->Append(edsl::Declare(hash_table, GetQueryStateEntryPtr(global_agg_ht_)));
      UpdateAggregates(context, function, hash_table);
    }
  } else {
    TPL_ASSERT(context->IsForPipeline(*GetPipeline()),
               "Pipeline is unknown to hash aggregation translator");
    auto hash_table = edsl::Variable<ast::x::AggregationHashTable *>(codegen_, "agg_ht");
    if (context->IsParallel()) {
      auto param = function->GetParameterByPosition(2);
      function->Append(edsl::Declare(hash_table, param.As<ast::x::AggregationHashTable *>()));
    } else {
      function->Append(edsl::Declare(hash_table, GetQueryStateEntryPtr(global_agg_ht_)));
    }
    ScanAggregationHashTable(context, function, hash_table);
  }
}

void HashAggregationTranslator::FinishPipelineWork(const PipelineContext &pipeline_ctx,
                                                   FunctionBuilder *function) const {
  if (pipeline_ctx.IsForPipeline(build_pipeline_) && pipeline_ctx.IsParallel()) {
    auto global_ht = GetQueryStateEntryPtr(global_agg_ht_);
    auto tls_container = GetThreadStateContainer();
    auto ht_offset = pipeline_ctx.GetStateEntryByteOffset(local_agg_ht_);
    function->Append(global_ht->MovePartitions(tls_container, ht_offset, merge_partitions_fn_));
  }
}

edsl::ValueVT HashAggregationTranslator::GetChildOutput(ConsumerContext *context,
                                                        uint32_t child_idx,
                                                        uint32_t attr_idx) const {
  if (context->IsForPipeline(*GetPipeline())) {
    if (child_idx == 0) return agg_payload_.GetMember(*agg_row_, attr_idx);
    return edsl::AggregatorResult(agg_payload_.GetMemberPtr(*agg_row_, AggId(attr_idx)));
  }
  return OperatorTranslator::GetChildOutput(context, child_idx, KeyId(attr_idx));
}

void HashAggregationTranslator::DrivePipeline(const PipelineContext &pipeline_ctx) const {
  TPL_ASSERT(pipeline_ctx.IsForPipeline(*GetPipeline()), "Aggregation driving unknown pipeline!");
  if (pipeline_ctx.IsParallel()) {
    const auto dispatch = [&](FunctionBuilder *function, ast::Identifier work_func) {
      auto hash_table = GetQueryStateEntryPtr(global_agg_ht_);
      auto query_state = GetQueryStatePtr();
      auto tls_container = GetThreadStateContainer();
      function->Append(hash_table->ParallelScan(query_state, tls_container, work_func));
    };
    std::vector<FunctionBuilder::Param> params = {
        {codegen_->MakeIdentifier("agg_ht"), codegen_->GetType<ast::x::AggregationHashTable *>()}};
    GetPipeline()->LaunchParallel(pipeline_ctx, dispatch, std::move(params));
  } else {
    GetPipeline()->LaunchSerial(pipeline_ctx);
  }
}

}  // namespace tpl::sql::codegen
