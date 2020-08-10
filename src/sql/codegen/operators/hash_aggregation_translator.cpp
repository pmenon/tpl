#include "sql/codegen/operators/hash_aggregation_translator.h"

#include "sql/codegen/codegen.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/function_builder.h"
#include "sql/codegen/if.h"
#include "sql/codegen/loop.h"
#include "sql/planner/plannodes/aggregate_plan_node.h"

namespace tpl::sql::codegen {

namespace {
constexpr char kGroupByKeyPrefix[] = "gb_key";
constexpr char kAggregateTermPrefix[] = "agg";
}  // namespace

HashAggregationTranslator::HashAggregationTranslator(const planner::AggregatePlanNode &plan,
                                                     CompilationContext *compilation_context,
                                                     Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline),
      agg_row_var_(codegen_->MakeFreshIdentifier("agg_row")),
      agg_payload_type_(codegen_->MakeFreshIdentifier("AggPayload")),
      agg_values_type_(codegen_->MakeFreshIdentifier("AggValues")),
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

  // Prepare all grouping and aggregte expressions.
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
  ast::Expr *agg_ht_type = codegen_->BuiltinType(ast::BuiltinType::AggregationHashTable);
  global_agg_ht_ = compilation_context->GetQueryState()->DeclareStateEntry(
      codegen_, "agg_hash_table", agg_ht_type);
}

void HashAggregationTranslator::DeclarePipelineDependencies() const {
  GetPipeline()->AddDependency(build_pipeline_);
}

ast::StructDecl *HashAggregationTranslator::GeneratePayloadStruct() {
  auto fields = codegen_->MakeEmptyFieldList();
  fields.reserve(GetAggPlan().GetGroupByTerms().size() + GetAggPlan().GetAggregateTerms().size());

  // Create a field for every group by term.
  uint32_t term_idx = 0;
  for (const auto &term : GetAggPlan().GetGroupByTerms()) {
    auto field_name = codegen_->MakeIdentifier(kGroupByKeyPrefix + std::to_string(term_idx));
    auto type = codegen_->TplType(term->GetReturnValueType());
    fields.push_back(codegen_->MakeField(field_name, type));
    term_idx++;
  }

  // Create a field for every aggregate term.
  term_idx = 0;
  for (const auto &term : GetAggPlan().GetAggregateTerms()) {
    auto field_name = codegen_->MakeIdentifier(kAggregateTermPrefix + std::to_string(term_idx));
    auto type = codegen_->AggregateType(term->GetExpressionType(), term->GetReturnValueType());
    fields.push_back(codegen_->MakeField(field_name, type));
    term_idx++;
  }

  return codegen_->DeclareStruct(agg_payload_type_, std::move(fields));
}

ast::StructDecl *HashAggregationTranslator::GenerateInputValuesStruct() {
  auto fields = codegen_->MakeEmptyFieldList();
  fields.reserve(GetAggPlan().GetGroupByTerms().size() + GetAggPlan().GetAggregateTerms().size());

  // Create a field for every group by term.
  uint32_t term_idx = 0;
  for (const auto &term : GetAggPlan().GetGroupByTerms()) {
    auto field_name = codegen_->MakeIdentifier(kGroupByKeyPrefix + std::to_string(term_idx));
    auto type = codegen_->TplType(term->GetReturnValueType());
    fields.push_back(codegen_->MakeField(field_name, type));
    term_idx++;
  }

  // Create a field for every aggregate term.
  term_idx = 0;
  for (const auto &term : GetAggPlan().GetAggregateTerms()) {
    auto field_name = codegen_->MakeIdentifier(kAggregateTermPrefix + std::to_string(term_idx));
    auto type = codegen_->TplType(term->GetChild(0)->GetReturnValueType());
    fields.push_back(codegen_->MakeField(field_name, type));
    term_idx++;
  }

  return codegen_->DeclareStruct(agg_values_type_, std::move(fields));
}

ast::FunctionDecl *HashAggregationTranslator::GeneratePartialKeyCheckFunction() {
  auto lhs_arg = codegen_->MakeIdentifier("lhs");
  auto rhs_arg = codegen_->MakeIdentifier("rhs");
  auto params = codegen_->MakeFieldList({
      codegen_->MakeField(lhs_arg, codegen_->PointerType(agg_payload_type_)),
      codegen_->MakeField(rhs_arg, codegen_->PointerType(agg_payload_type_)),
  });
  auto ret_type = codegen_->BuiltinType(ast::BuiltinType::Kind::Bool);
  FunctionBuilder builder(codegen_, key_check_partial_fn_, std::move(params), ret_type);
  {
    for (uint32_t term_idx = 0; term_idx < GetAggPlan().GetGroupByTerms().size(); term_idx++) {
      auto lhs = GetGroupByTerm(lhs_arg, term_idx);
      auto rhs = GetGroupByTerm(rhs_arg, term_idx);
      If check_match(&builder, codegen_->Compare(parsing::Token::Type::BANG_EQUAL, lhs, rhs));
      builder.Append(codegen_->Return(codegen_->ConstBool(false)));
    }
    builder.Append(codegen_->Return(codegen_->ConstBool(true)));
  }
  return builder.Finish();
}

template <typename F1, typename F2>
void HashAggregationTranslator::MergeOverflowPartitions(FunctionBuilder *function,
                                                        F1 hash_table_provider, F2 iter_provider) {
  Loop loop(function, nullptr, codegen_->AggPartitionIteratorHasNext(iter_provider()),
            codegen_->MakeStmt(codegen_->AggPartitionIteratorNext(iter_provider())));
  {
    // Get hash from overflow entry.
    ast::Identifier hash_val = codegen_->MakeFreshIdentifier("hash_val");
    ast::Expr *get_hash_call = codegen_->AggPartitionIteratorGetHash(iter_provider());
    function->Append(codegen_->DeclareVarWithInit(hash_val, get_hash_call));

    // Get the partial aggregate row from the overflow entry.
    ast::Identifier partial_row = codegen_->MakeFreshIdentifier("partial_row");
    ast::Expr *get_row_call =
        codegen_->AggPartitionIteratorGetRow(iter_provider(), agg_payload_type_);
    function->Append(codegen_->DeclareVarWithInit(partial_row, get_row_call));

    // Perform lookup.
    auto lookup_result = codegen_->MakeFreshIdentifier("agg_payload");
    function->Append(codegen_->DeclareVarWithInit(
        lookup_result,
        codegen_->AggHashTableLookup(hash_table_provider(), codegen_->MakeExpr(hash_val),
                                     key_check_partial_fn_, codegen_->MakeExpr(partial_row),
                                     agg_payload_type_)));

    If check_found(function, codegen_->IsNilPointer(codegen_->MakeExpr(lookup_result)));
    {
      // Link entry.
      ast::Expr *entry = codegen_->AggPartitionIteratorGetRowEntry(iter_provider());
      function->Append(codegen_->AggHashTableLinkEntry(hash_table_provider(), entry));
    }
    check_found.Else();
    {
      // Merge partial aggregate.
      const auto &agg_terms = GetAggPlan().GetAggregateTerms();
      for (uint32_t term_idx = 0; term_idx < agg_terms.size(); term_idx++) {
        ast::Expr *lhs = GetAggregateTermPtr(lookup_result, term_idx);
        ast::Expr *rhs = GetAggregateTermPtr(partial_row, term_idx);
        function->Append(codegen_->AggregatorMerge(lhs, rhs));
      }
    }
    check_found.EndIf();
  }
}

ast::FunctionDecl *HashAggregationTranslator::GenerateMergeOverflowPartitionsFunction() {
  // The partition merge function has the following signature:
  // (*QueryState, *AggregationHashTable, *AHTOverflowPartitionIterator) -> nil

  auto params = GetCompilationContext()->QueryParams();

  // Then the aggregation hash table and the overflow partition iterator.
  auto agg_ht = codegen_->MakeIdentifier("aht");
  auto overflow_iter = codegen_->MakeIdentifier("aht_ovf_iter");
  params.push_back(
      codegen_->MakeField(agg_ht, codegen_->PointerType(ast::BuiltinType::AggregationHashTable)));
  params.push_back(codegen_->MakeField(
      overflow_iter, codegen_->PointerType(ast::BuiltinType::AHTOverflowPartitionIterator)));

  auto ret_type = codegen_->BuiltinType(ast::BuiltinType::Kind::Nil);
  FunctionBuilder builder(codegen_, merge_partitions_fn_, std::move(params), ret_type);
  {
    // Main merging logic.
    const auto hash_table_provider = [&]() { return codegen_->MakeExpr(agg_ht); };
    const auto iter_provider = [&]() { return codegen_->MakeExpr(overflow_iter); };
    MergeOverflowPartitions(&builder, hash_table_provider, iter_provider);
  }
  return builder.Finish();
}

ast::FunctionDecl *HashAggregationTranslator::GenerateKeyCheckFunction() {
  ast::Identifier agg_payload = codegen_->MakeIdentifier("agg_payload");
  ast::Identifier agg_values = codegen_->MakeIdentifier("agg_values");
  auto params = codegen_->MakeFieldList({
      codegen_->MakeField(agg_payload, codegen_->PointerType(agg_payload_type_)),
      codegen_->MakeField(agg_values, codegen_->PointerType(agg_values_type_)),
  });
  ast::Expr *ret_type = codegen_->BuiltinType(ast::BuiltinType::Kind::Bool);
  FunctionBuilder builder(codegen_, key_check_fn_, std::move(params), ret_type);
  {
    for (uint32_t term_idx = 0; term_idx < GetAggPlan().GetGroupByTerms().size(); term_idx++) {
      ast::Expr *lhs = GetGroupByTerm(agg_payload, term_idx);
      ast::Expr *rhs = GetGroupByTerm(agg_values, term_idx);
      If check_match(&builder, codegen_->Compare(parsing::Token::Type::BANG_EQUAL, lhs, rhs));
      builder.Append(codegen_->Return(codegen_->ConstBool(false)));
    }
    builder.Append(codegen_->Return(codegen_->ConstBool(true)));
  }
  return builder.Finish();
}

void HashAggregationTranslator::DefineStructsAndFunctions() {
  GeneratePayloadStruct();
  GenerateInputValuesStruct();
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

void HashAggregationTranslator::InitializeAggregationHashTable(FunctionBuilder *function,
                                                               ast::Expr *agg_ht) const {
  function->Append(codegen_->AggHashTableInit(agg_ht, GetMemoryPool(), agg_payload_type_));
}

void HashAggregationTranslator::TearDownAggregationHashTable(FunctionBuilder *function,
                                                             ast::Expr *agg_ht) const {
  function->Append(codegen_->AggHashTableFree(agg_ht));
}

void HashAggregationTranslator::InitializeQueryState(FunctionBuilder *function) const {
  ast::Expr *global_hash_table = GetQueryStateEntryPtr(global_agg_ht_);
  InitializeAggregationHashTable(function, global_hash_table);
}

void HashAggregationTranslator::TearDownQueryState(FunctionBuilder *function) const {
  ast::Expr *global_hash_table = GetQueryStateEntryPtr(global_agg_ht_);
  TearDownAggregationHashTable(function, global_hash_table);
}

void HashAggregationTranslator::DeclarePipelineState(PipelineContext *pipeline_ctx) {
  if (pipeline_ctx->IsForPipeline(build_pipeline_) && pipeline_ctx->IsParallel()) {
    ast::Expr *agg_ht_type = codegen_->BuiltinType(ast::BuiltinType::AggregationHashTable);
    local_agg_ht_ = pipeline_ctx->DeclarePipelineStateEntry("agg_hash_table", agg_ht_type);
  }
}

void HashAggregationTranslator::InitializePipelineState(const PipelineContext &pipeline_ctx,
                                                        FunctionBuilder *function) const {
  if (pipeline_ctx.IsForPipeline(build_pipeline_) && pipeline_ctx.IsParallel()) {
    ast::Expr *local_hash_table = pipeline_ctx.GetStateEntryPtr(local_agg_ht_);
    InitializeAggregationHashTable(function, local_hash_table);
  }
}

void HashAggregationTranslator::TearDownPipelineState(const PipelineContext &pipeline_ctx,
                                                      FunctionBuilder *function) const {
  if (pipeline_ctx.IsForPipeline(build_pipeline_) && pipeline_ctx.IsParallel()) {
    ast::Expr *local_hash_table = pipeline_ctx.GetStateEntryPtr(local_agg_ht_);
    TearDownAggregationHashTable(function, local_hash_table);
  }
}

ast::Expr *HashAggregationTranslator::GetGroupByTerm(ast::Identifier agg_row,
                                                     uint32_t attr_idx) const {
  auto member = codegen_->MakeIdentifier(kGroupByKeyPrefix + std::to_string(attr_idx));
  return codegen_->AccessStructMember(codegen_->MakeExpr(agg_row), member);
}

ast::Expr *HashAggregationTranslator::GetAggregateTerm(ast::Identifier agg_row,
                                                       uint32_t attr_idx) const {
  auto member = codegen_->MakeIdentifier(kAggregateTermPrefix + std::to_string(attr_idx));
  return codegen_->AccessStructMember(codegen_->MakeExpr(agg_row), member);
}

ast::Expr *HashAggregationTranslator::GetAggregateTermPtr(ast::Identifier agg_row,
                                                          uint32_t attr_idx) const {
  return codegen_->AddressOf(GetAggregateTerm(agg_row, attr_idx));
}

ast::Identifier HashAggregationTranslator::FillInputValues(FunctionBuilder *function,
                                                           ConsumerContext *context) const {
  // var agg_values : AggValues
  ast::Identifier agg_values = codegen_->MakeFreshIdentifier("agg_values");
  function->Append(codegen_->DeclareVarNoInit(agg_values, codegen_->MakeExpr(agg_values_type_)));

  // Populate the grouping terms.
  uint32_t term_idx = 0;
  for (const auto &term : GetAggPlan().GetGroupByTerms()) {
    ast::Expr *lhs = GetGroupByTerm(agg_values, term_idx);
    ast::Expr *rhs = context->DeriveValue(*term, this);
    function->Append(codegen_->Assign(lhs, rhs));
    term_idx++;
  }

  // Populate the raw aggregate values.
  term_idx = 0;
  for (const auto &term : GetAggPlan().GetAggregateTerms()) {
    ast::Expr *lhs = GetAggregateTerm(agg_values, term_idx);
    ast::Expr *rhs = context->DeriveValue(*term->GetChild(0), this);
    function->Append(codegen_->Assign(lhs, rhs));
    term_idx++;
  }

  return agg_values;
}

ast::Identifier HashAggregationTranslator::HashInputKeys(FunctionBuilder *function,
                                                         ast::Identifier agg_values) const {
  std::vector<ast::Expr *> keys;
  for (uint32_t term_idx = 0; term_idx < GetAggPlan().GetGroupByTerms().size(); term_idx++) {
    keys.push_back(GetGroupByTerm(agg_values, term_idx));
  }

  // var hashVal = @hash(...)
  auto hash_val = codegen_->MakeFreshIdentifier("hash_val");
  function->Append(codegen_->DeclareVarWithInit(hash_val, codegen_->Hash(keys)));
  return hash_val;
}

template <typename F>
ast::Identifier HashAggregationTranslator::PerformLookup(FunctionBuilder *function,
                                                         F agg_ht_provider,
                                                         ast::Identifier hash_val,
                                                         ast::Identifier agg_values) const {
  // var aggPayload = @ptrCast(*AggPayload, @aggHTLookup())
  auto lookup_call = codegen_->AggHashTableLookup(
      agg_ht_provider(), codegen_->MakeExpr(hash_val), key_check_fn_,
      codegen_->AddressOf(codegen_->MakeExpr(agg_values)), agg_payload_type_);
  auto agg_payload = codegen_->MakeFreshIdentifier("agg_payload");
  function->Append(codegen_->DeclareVarWithInit(agg_payload, lookup_call));
  return agg_payload;
}

template <typename F>
void HashAggregationTranslator::ConstructNewAggregate(FunctionBuilder *function, F agg_ht_provider,
                                                      ast::Identifier agg_payload,
                                                      ast::Identifier agg_values,
                                                      ast::Identifier hash_val) const {
  // aggRow = @ptrCast(*AggPayload, @aggHTInsert(&state.agg_table, agg_hash_val))
  bool partitioned = build_pipeline_.IsParallel();
  auto insert_call = codegen_->AggHashTableInsert(agg_ht_provider(), codegen_->MakeExpr(hash_val),
                                                  partitioned, agg_payload_type_);
  function->Append(codegen_->Assign(codegen_->MakeExpr(agg_payload), insert_call));

  // Copy the grouping keys.
  for (uint32_t term_idx = 0; term_idx < GetAggPlan().GetGroupByTerms().size(); term_idx++) {
    auto lhs = GetGroupByTerm(agg_payload, term_idx);
    auto rhs = GetGroupByTerm(agg_values, term_idx);
    function->Append(codegen_->Assign(lhs, rhs));
  }

  // Initialize all aggregate terms.
  for (uint32_t term_idx = 0; term_idx < GetAggPlan().GetAggregateTerms().size(); term_idx++) {
    auto agg_term = GetAggregateTermPtr(agg_payload, term_idx);
    function->Append(codegen_->AggregatorInit(agg_term));
  }
}

void HashAggregationTranslator::AdvanceAggregate(FunctionBuilder *function,
                                                 ast::Identifier agg_payload,
                                                 ast::Identifier agg_values) const {
  for (uint32_t term_idx = 0; term_idx < GetAggPlan().GetAggregateTerms().size(); term_idx++) {
    auto agg = GetAggregateTermPtr(agg_payload, term_idx);
    auto val = GetAggregateTermPtr(agg_values, term_idx);
    function->Append(codegen_->AggregatorAdvance(agg, val));
  }
}

template <typename F>
void HashAggregationTranslator::UpdateAggregates(ConsumerContext *context,
                                                 FunctionBuilder *function,
                                                 F agg_ht_provider) const {
  auto agg_values = FillInputValues(function, context);
  auto hash_val = HashInputKeys(function, agg_values);
  auto agg_payload = PerformLookup(function, agg_ht_provider, hash_val, agg_values);

  If check_new_agg(function, codegen_->IsNilPointer(codegen_->MakeExpr(agg_payload)));
  ConstructNewAggregate(function, agg_ht_provider, agg_payload, agg_values, hash_val);
  check_new_agg.EndIf();

  // Advance aggregate.
  AdvanceAggregate(function, agg_payload, agg_values);
}

template <typename F>
void HashAggregationTranslator::ScanAggregationHashTable(ConsumerContext *context,
                                                         FunctionBuilder *function,
                                                         F agg_ht_provider) const {
  // var iter_base: AHTIterator
  ast::Identifier aht_iter_base_name = codegen_->MakeFreshIdentifier("iter_base");
  ast::Expr *aht_iter_type = codegen_->BuiltinType(ast::BuiltinType::AHTIterator);
  function->Append(codegen_->DeclareVarNoInit(aht_iter_base_name, aht_iter_type));

  // var aht_iter = &iter_base
  ast::Identifier aht_iter_name = codegen_->MakeFreshIdentifier("iter");
  const auto aht_iter = [&]() { return codegen_->MakeExpr(aht_iter_name); };
  function->Append(codegen_->DeclareVarWithInit(
      aht_iter_name, codegen_->AddressOf(codegen_->MakeExpr(aht_iter_base_name))));

  Loop loop(function,
            codegen_->MakeStmt(codegen_->AggHashTableIteratorInit(aht_iter(), agg_ht_provider())),
            codegen_->AggHashTableIteratorHasNext(aht_iter()),
            codegen_->MakeStmt(codegen_->AggHashTableIteratorNext(aht_iter())));
  {
    // var agg_row = @ahtIterGetRow()
    ast::Expr *row = codegen_->AggHashTableIteratorGetRow(aht_iter(), agg_payload_type_);
    function->Append(codegen_->DeclareVarWithInit(agg_row_var_, row));

    // Check having clause.
    if (const auto having = GetAggPlan().GetHavingClausePredicate(); having != nullptr) {
      If check_having(function, context->DeriveValue(*having, this));
      context->Consume(function);
    } else {
      context->Consume(function);
    }
  }
  loop.EndLoop();

  // Close iterator.
  function->Append(codegen_->AggHashTableIteratorClose(aht_iter()));
}

void HashAggregationTranslator::Consume(ConsumerContext *context, FunctionBuilder *function) const {
  if (context->IsForPipeline(build_pipeline_)) {
    if (context->IsParallel()) {
      const auto hash_table_provider = [&]() { return context->GetStateEntryPtr(local_agg_ht_); };
      UpdateAggregates(context, function, hash_table_provider);
    } else {
      const auto hash_table_provider = [&]() { return GetQueryStateEntryPtr(global_agg_ht_); };
      UpdateAggregates(context, function, hash_table_provider);
    }
  } else {
    TPL_ASSERT(IsProducePipeline(context->GetPipeline()),
               "Pipeline is unknown to hash aggregation translator");
    if (context->IsParallel()) {
      // In parallel-mode, we would've issued a parallel partitioned scan which
      // places the aggregation hash table to scan as the last function arg.
      const auto hash_table_provider = [&]() { return function->GetParameterByPosition(2); };
      ScanAggregationHashTable(context, function, hash_table_provider);
    } else {
      const auto hash_table_provider = [&]() { return GetQueryStateEntryPtr(global_agg_ht_); };
      ScanAggregationHashTable(context, function, hash_table_provider);
    }
  }
}

void HashAggregationTranslator::FinishPipelineWork(const PipelineContext &pipeline_ctx,
                                                   FunctionBuilder *function) const {
  if (pipeline_ctx.IsForPipeline(build_pipeline_) && pipeline_ctx.IsParallel()) {
    ast::Expr *global_agg_ht = GetQueryStateEntryPtr(global_agg_ht_);
    ast::Expr *thread_state_container = GetThreadStateContainer();
    ast::Expr *tl_agg_ht_offset = pipeline_ctx.GetStateEntryByteOffset(local_agg_ht_);
    function->Append(codegen_->AggHashTableMovePartitions(global_agg_ht, thread_state_container,
                                                          tl_agg_ht_offset, merge_partitions_fn_));
  }
}

ast::Expr *HashAggregationTranslator::GetChildOutput(ConsumerContext *context, uint32_t child_idx,
                                                     uint32_t attr_idx) const {
  TPL_ASSERT(child_idx == 0, "Aggregations can only have a single child.");
  if (context->IsForPipeline(*GetPipeline())) {
    if (child_idx == 0) {
      return GetGroupByTerm(agg_row_var_, attr_idx);
    }
    return codegen_->AggregatorResult(GetAggregateTermPtr(agg_row_var_, attr_idx));
  }
  // The request is in the build pipeline. Forward to child translator.
  return OperatorTranslator::GetChildOutput(context, child_idx, attr_idx);
}

void HashAggregationTranslator::DrivePipeline(const PipelineContext &pipeline_ctx) const {
  TPL_ASSERT(pipeline_ctx.IsForPipeline(*GetPipeline()), "Aggregation driving unknown pipeline!");
  if (pipeline_ctx.IsParallel()) {
    const auto dispatch = [&](FunctionBuilder *function, ast::Identifier work_func) {
      ast::Expr *global_hash_table = GetQueryStateEntryPtr(global_agg_ht_);
      ast::Expr *q_state = GetQueryStatePtr();
      ast::Expr *thread_state_container = GetThreadStateContainer();
      function->Append(codegen_->AggHashTableParallelScan(global_hash_table, q_state,
                                                          thread_state_container, work_func));
    };
    std::vector<ast::FieldDecl *> params = {
        codegen_->MakeField(codegen_->MakeIdentifier("agg_hash_table"),
                            codegen_->PointerType(ast::BuiltinType::AggregationHashTable))};
    GetPipeline()->LaunchParallel(pipeline_ctx, dispatch, std::move(params));
  } else {
    GetPipeline()->LaunchSerial(pipeline_ctx);
  }
}

}  // namespace tpl::sql::codegen
