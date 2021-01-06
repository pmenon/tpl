#include "sql/codegen/operators/hash_aggregation_translator.h"

#include <string_view>

// For string formatting.
#include "spdlog/fmt/fmt.h"

#include "sql/codegen/codegen.h"
#include "sql/codegen/compilation_context.h"
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
      agg_values_(codegen_, "AggValues", true),
      key_check_fn_(
          codegen_->MakeFreshIdentifier(pipeline->CreatePipelineFunctionName("KeyCheck"))),
      key_check_partial_fn_(
          codegen_->MakeFreshIdentifier(pipeline->CreatePipelineFunctionName("KeyCheckPartial"))),
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
  uint32_t term_idx = 0;
  for (const auto &term : GetAggPlan().GetGroupByTerms()) {
    auto name = fmt::format("{}{}", kGroupByKeyPrefix, term_idx++);
    auto type = codegen_->GetTPLType(term->GetReturnValueType().GetPrimitiveTypeId());
    agg_payload_.AddMember(name, type);
  }

  // Create a field for every aggregate term.
  term_idx = 0;
  for (const auto &term : GetAggPlan().GetAggregateTerms()) {
    auto name = fmt::format("{}{}", kAggregateTermPrefix, term_idx++);
    auto type =
        codegen_->AggregateType(term->GetKind(), term->GetReturnValueType().GetPrimitiveTypeId());
    agg_payload_.AddMember(name, type);
  }

  agg_payload_.Seal();
}

void HashAggregationTranslator::GenerateInputValuesStruct() {
  // Create a field for every group by term.
  uint32_t term_idx = 0;
  for (const auto &term : GetAggPlan().GetGroupByTerms()) {
    auto name = fmt::format("{}{}", kGroupByKeyPrefix, term_idx++);
    auto type = codegen_->GetTPLType(term->GetReturnValueType().GetPrimitiveTypeId());
    agg_values_.AddMember(name, type);
  }

  // Create a field for every aggregate term.
  term_idx = 0;
  for (const auto &term : GetAggPlan().GetAggregateTerms()) {
    auto name = fmt::format("{}{}", kAggregateTermPrefix, term_idx++);
    auto type = codegen_->GetTPLType(term->GetChild(0)->GetReturnValueType().GetPrimitiveTypeId());
    agg_values_.AddMember(name, type);
  }

  agg_values_.Seal();
}

std::size_t HashAggregationTranslator::KeyId(std::size_t id) const { return id; }

std::size_t HashAggregationTranslator::AggId(std::size_t id) const {
  return id + GetAggPlan().NumGroupByTerms();
}

void HashAggregationTranslator::GeneratePartialKeyCheckFunction() {
  std::vector<FunctionBuilder::Param> params = {
      {codegen_->MakeIdentifier("lhs"), agg_payload_.GetPtrToType()},
      {codegen_->MakeIdentifier("rhs"), agg_payload_.GetPtrToType()},
  };
  FunctionBuilder fn(codegen_, key_check_partial_fn_, std::move(params), codegen_->GetType<bool>());
  {
    for (uint32_t i = 0; i < GetAggPlan().NumGroupByTerms(); i++) {
      auto lhs = agg_payload_.GetMember(fn.GetParameterByPosition(0), KeyId(i));
      auto rhs = agg_payload_.GetMember(fn.GetParameterByPosition(1), KeyId(i));
      If check_match(&fn, edsl::ComparisonOp(parsing::Token::Type::BANG_EQUAL, lhs, rhs));
      fn.Append(edsl::Return(codegen_, false));
    }
    fn.Append(edsl::Return(codegen_, true));
  }
  fn.Finish();
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
    ast::Type *payload_type = agg_payload_.GetPtrToType();
    auto hash_table = fn.GetParameterByPosition(1).As<ast::x::AggregationHashTable *>();
    auto iter = fn.GetParameterByPosition(2).As<ast::x::AHTOverflowPartitionIterator *>();
    edsl::Variable<hash_t> hash_val(codegen_, "hash_val");
    edsl::VariableVT partial_row(codegen_, "partial_row", payload_type);
    edsl::VariableVT lookup_result(codegen_, "lookup_result", payload_type);
    Loop loop(&fn, nullptr, iter->HasNext(), iter->Next());
    {
      fn.Append(edsl::Declare(hash_val, iter->GetHash()));
      fn.Append(edsl::Declare(partial_row, edsl::PtrCast(payload_type, iter->GetRow())));
      fn.Append(edsl::Declare(
          lookup_result,
          edsl::PtrCast(payload_type,
                        hash_table->Lookup(hash_val, key_check_partial_fn_, partial_row))));

      If check_found(&fn, edsl::IsNilPtr(lookup_result));
      {  // Entry doesn't exist in table, link in this entry.
        fn.Append(hash_table->LinkEntry(iter->GetRowEntry()));
      }
      check_found.Else();
      {  // Entry exists, merge the partial aggregate.
        for (uint32_t i = 0; i < GetAggPlan().NumAggregateTerms(); i++) {
          auto lhs = agg_payload_.GetMemberPtr(lookup_result, AggId(i));
          auto rhs = agg_payload_.GetMemberPtr(partial_row, AggId(i));
          fn.Append(edsl::AggregatorMerge(lhs, rhs));
        }
      }
      check_found.EndIf();
    }
  }
  fn.Finish();
}

void HashAggregationTranslator::GenerateKeyCheckFunction() {
  // Key-check function:
  // (candidate: *AggPayload, probe_tuple: *AggValues) -> bool

  std::vector<FunctionBuilder::Param> params = {
      {codegen_->MakeIdentifier("agg_payload"), agg_payload_.GetType()->PointerTo()},
      {codegen_->MakeIdentifier("agg_values"), agg_values_.GetType()->PointerTo()},
  };
  FunctionBuilder fn(codegen_, key_check_fn_, std::move(params), codegen_->BoolType());
  {
    for (uint32_t i = 0; i < GetAggPlan().NumGroupByTerms(); i++) {
      auto lhs = agg_payload_.GetMember(fn.GetParameterByPosition(0), KeyId(i));
      auto rhs = agg_values_.GetMember(fn.GetParameterByPosition(1), KeyId(i));
      If check_match(&fn, edsl::ComparisonOp(parsing::Token::Type::BANG_EQUAL, lhs, rhs));
      fn.Append(edsl::Return(codegen_, false));
    }
    fn.Append(edsl::Return(codegen_, true));
  }
  fn.Finish();
}

void HashAggregationTranslator::DefineStructsAndFunctions() {
  GeneratePayloadStruct();
  GenerateInputValuesStruct();
  agg_row_ = std::make_unique<edsl::VariableVT>(codegen_, "agg_row", agg_payload_.GetPtrToType());
}

void HashAggregationTranslator::DefinePipelineFunctions(const PipelineContext &pipeline_ctx) {
  if (pipeline_ctx.IsForPipeline(build_pipeline_)) {
    GenerateKeyCheckFunction();
    if (pipeline_ctx.IsParallel()) {
      GeneratePartialKeyCheckFunction();
      GenerateMergeOverflowPartitionsFunction();
    }
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

edsl::VariableVT HashAggregationTranslator::FillInputValues(FunctionBuilder *function,
                                                            ConsumerContext *context) const {
  // var input_values : AggValues
  edsl::VariableVT input_values(codegen_, "input_values", agg_values_.GetType());
  function->Append(edsl::Declare(input_values));

  // Populate the grouping terms.
  uint32_t term_idx = 0;
  for (const auto &term : GetAggPlan().GetGroupByTerms()) {
    auto lhs = agg_values_.GetMember(input_values, KeyId(term_idx++));
    auto rhs = context->DeriveValue(*term, this);
    function->Append(edsl::Assign(lhs, rhs));
  }

  // Populate the raw aggregate values.
  term_idx = 0;
  for (const auto &term : GetAggPlan().GetAggregateTerms()) {
    auto lhs = agg_values_.GetMember(input_values, AggId(term_idx++));
    auto rhs = context->DeriveValue(*term->GetChild(0), this);
    function->Append(edsl::Assign(lhs, rhs));
  }

  return input_values;
}

edsl::Variable<hash_t> HashAggregationTranslator::HashInputKeys(
    FunctionBuilder *function, const edsl::ValueVT &input_values) const {
  std::vector<edsl::ValueVT> keys;
  for (uint32_t i = 0; i < GetAggPlan().NumGroupByTerms(); i++) {
    keys.emplace_back(agg_values_.GetMember(input_values, KeyId(i)));
  }

  // var hashVal = @hash(...)
  edsl::Variable<hash_t> hash_val(codegen_, "hash_val");
  function->Append(edsl::Declare(hash_val, edsl::Hash(keys)));
  return hash_val;
}

edsl::VariableVT HashAggregationTranslator::PerformLookup(
    FunctionBuilder *function, const edsl::Value<ast::x::AggregationHashTable *> &hash_table,
    const edsl::Value<hash_t> &hash_val, const edsl::ReferenceVT &agg_values) const {
  // var aggPayload = @ptrCast(*AggPayload, @aggHTLookup())
  edsl::VariableVT result(codegen_, "lookup_result", agg_payload_.GetPtrToType());
  auto raw_lookup = hash_table->Lookup(hash_val, key_check_fn_, agg_values.Addr());
  function->Append(edsl::Declare(result, edsl::PtrCast(result.GetType(), raw_lookup)));
  return result;
}

void HashAggregationTranslator::ConstructNewAggregate(
    FunctionBuilder *function, const edsl::Value<ast::x::AggregationHashTable *> &hash_table,
    const edsl::ReferenceVT &agg_payload, const edsl::ValueVT &input_values,
    const edsl::Value<hash_t> &hash_val) const {
  const bool partitioned = build_pipeline_.IsParallel();
  const auto bytes = hash_table->Insert(hash_val, edsl::Literal<bool>(codegen_, partitioned));
  function->Append(edsl::Assign(agg_payload, edsl::PtrCast(agg_payload_.GetPtrToType(), bytes)));

  // Copy the grouping keys.
  for (uint32_t i = 0; i < GetAggPlan().NumGroupByTerms(); i++) {
    auto lhs = agg_payload_.GetMember(agg_payload, KeyId(i));
    auto rhs = agg_values_.GetMember(input_values, KeyId(i));
    function->Append(edsl::Assign(lhs, rhs));
  }

  // Initialize all aggregate terms.
  for (uint32_t i = 0; i < GetAggPlan().NumAggregateTerms(); i++) {
    auto agg = agg_payload_.GetMemberPtr(agg_payload, AggId(i));
    function->Append(edsl::AggregatorInit(agg));
  }
}

void HashAggregationTranslator::AdvanceAggregate(FunctionBuilder *function,
                                                 const edsl::ReferenceVT &agg_payload,
                                                 const edsl::ReferenceVT &input_values) const {
  for (uint32_t i = 0; i < GetAggPlan().NumAggregateTerms(); i++) {
    auto agg = agg_payload_.GetMemberPtr(agg_payload, AggId(i));
    auto val = agg_values_.GetMemberPtr(input_values, AggId(i));
    function->Append(edsl::AggregatorAdvance(agg, val));
  }
}

void HashAggregationTranslator::UpdateAggregates(
    ConsumerContext *context, FunctionBuilder *function,
    const edsl::Variable<ast::x::AggregationHashTable *> &hash_table) const {
  auto input_values = FillInputValues(function, context);
  auto hash_val = HashInputKeys(function, input_values);
  auto agg_payload = PerformLookup(function, hash_table, hash_val, input_values);

  If check_new_agg(function, edsl::IsNilPtr(agg_payload));
  {  // New aggregate.
    ConstructNewAggregate(function, hash_table, agg_payload, input_values, hash_val);
  }
  check_new_agg.EndIf();

  // Advance aggregate.
  AdvanceAggregate(function, agg_payload, input_values);
}

void HashAggregationTranslator::ScanAggregationHashTable(
    ConsumerContext *context, FunctionBuilder *function,
    const edsl::Variable<ast::x::AggregationHashTable *> &hash_table) const {
  edsl::Variable<ast::x::AHTIterator> iter_base(codegen_, "iter_base");
  edsl::Variable<ast::x::AHTIterator *> iter(codegen_, "iter");

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
    edsl::Variable<ast::x::AggregationHashTable *> hash_table(codegen_, "agg_ht");
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
    edsl::Variable<ast::x::AggregationHashTable *> hash_table(codegen_, "agg_ht");
    if (context->IsParallel()) {
      function->Append(edsl::Declare(
          hash_table, function->GetParameterByPosition(2).As<ast::x::AggregationHashTable *>()));
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
