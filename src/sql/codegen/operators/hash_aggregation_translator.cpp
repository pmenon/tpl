#include "sql/codegen/operators/hash_aggregation_translator.h"

#include "sql/codegen/codegen.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/function_builder.h"
#include "sql/codegen/if.h"
#include "sql/codegen/loop.h"
#include "sql/planner/plannodes/aggregate_plan_node.h"

namespace tpl::sql::codegen {

namespace {
constexpr char kGroupByTermAttrPrefix[] = "gb_term_attr";
constexpr char kAggregateTermAttrPrefix[] = "agg_term_attr";
}  // namespace

HashAggregationTranslator::HashAggregationTranslator(const planner::AggregatePlanNode &plan,
                                                     CompilationContext *compilation_context,
                                                     Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline),
      agg_row_var_name_(GetCodeGen()->MakeFreshIdentifier("aggRow")),
      build_pipeline_(this, Pipeline::Parallelism::Flexible),
      agg_payload_(nullptr),
      agg_values_(nullptr),
      key_check_fn_(nullptr),
      key_check_partial_fn_(nullptr),
      merge_partitions_fn_(nullptr) {
  TPL_ASSERT(plan.GetAggregateStrategyType() == planner::AggregateStrategyType::HASH,
             "Expected hash-based aggregation plan node");
  TPL_ASSERT(plan.GetChildrenSize() == 1, "Hash aggregations should only have one child");

  pipeline->RegisterStep(this, Pipeline::Parallelism::Parallel);
  pipeline->LinkSourcePipeline(&build_pipeline_);

  compilation_context->Prepare(*plan.GetChild(0), &build_pipeline_);

  for (const auto group_by_term : plan.GetGroupByTerms()) {
    compilation_context->Prepare(*group_by_term);
  }

  for (const auto agg_term : plan.GetAggregateTerms()) {
    compilation_context->Prepare(*agg_term->GetChild(0));
  }

  if (const auto having_clause = plan.GetHavingClausePredicate(); having_clause != nullptr) {
    compilation_context->Prepare(*having_clause);
  }

  auto codegen = GetCodeGen();
  agg_ht_slot_ = compilation_context->GetQueryState()->DeclareStateEntry(
      codegen, "aggHashTable", codegen->BuiltinType(ast::BuiltinType::AggregationHashTable));
}

void HashAggregationTranslator::DefinePayloadStruct(util::RegionVector<ast::StructDecl *> *decls) {
  auto codegen = GetCodeGen();
  auto fields = codegen->MakeEmptyFieldList();

  // Create a field for every group by term.
  uint32_t term_idx = 0;
  for (const auto &term : GetAggPlan().GetGroupByTerms()) {
    auto field_name = codegen->MakeIdentifier(kGroupByTermAttrPrefix + std::to_string(term_idx));
    auto type = codegen->TplType(term->GetReturnValueType());
    fields.push_back(codegen->MakeField(field_name, type));
    term_idx++;
  }

  // Create a field for every aggregate term.
  term_idx = 0;
  for (const auto &term : GetAggPlan().GetAggregateTerms()) {
    auto field_name = codegen->MakeIdentifier(kAggregateTermAttrPrefix + std::to_string(term_idx));
    auto type = codegen->AggregateType(term->GetExpressionType(), term->GetReturnValueType());
    fields.push_back(codegen->MakeField(field_name, type));
    term_idx++;
  }

  agg_payload_ =
      codegen->DeclareStruct(codegen->MakeFreshIdentifier("AggPayload"), std::move(fields));
  decls->push_back(agg_payload_);
}

void HashAggregationTranslator::DefineInputValuesStruct(
    util::RegionVector<ast::StructDecl *> *decls) {
  const auto &agg_plan = GetPlanAs<planner::AggregatePlanNode>();

  auto codegen = GetCodeGen();
  auto fields = codegen->MakeEmptyFieldList();

  // Create a field for every group by term.
  uint32_t term_idx = 0;
  for (const auto &term : agg_plan.GetGroupByTerms()) {
    auto field_name = codegen->MakeIdentifier(kGroupByTermAttrPrefix + std::to_string(term_idx));
    auto type = codegen->TplType(term->GetReturnValueType());
    fields.push_back(codegen->MakeField(field_name, type));
    term_idx++;
  }

  // Create a field for every aggregate term.
  term_idx = 0;
  for (const auto &term : agg_plan.GetAggregateTerms()) {
    auto field_name = codegen->MakeIdentifier(kAggregateTermAttrPrefix + std::to_string(term_idx));
    auto type = codegen->TplType(term->GetChild(0)->GetReturnValueType());
    fields.push_back(codegen->MakeField(field_name, type));
    term_idx++;
  }

  agg_values_ =
      codegen->DeclareStruct(codegen->MakeFreshIdentifier("AggValues"), std::move(fields));
  decls->push_back(agg_values_);
}

void HashAggregationTranslator::DefineHelperStructs(util::RegionVector<ast::StructDecl *> *decls) {
  DefinePayloadStruct(decls);
  DefineInputValuesStruct(decls);
}

void HashAggregationTranslator::MergeOverflowPartitions(FunctionBuilder *function) {
  auto codegen = GetCodeGen();

  auto iter = function->GetParameterByPosition(2);
  Loop loop(codegen, nullptr, codegen->AggPartitionIteratorHasNext(iter),
            codegen->MakeStmt(codegen->AggPartitionIteratorNext(iter)));
  {
    auto agg_ht = function->GetParameterByPosition(1);
    iter = function->GetParameterByPosition(2);

    // Get hash from overflow entry.
    auto hash_val = codegen->MakeFreshIdentifier("hashVal");
    function->Append(
        codegen->DeclareVarWithInit(hash_val, codegen->AggPartitionIteratorGetHash(iter)));

    // Get the partial aggregate row from the overflow entry.
    auto partial_row = codegen->MakeFreshIdentifier("partialRow");
    function->Append(codegen->DeclareVarWithInit(
        partial_row, codegen->AggPartitionIteratorGetRow(iter, agg_payload_->Name())));

    // Perform lookup.
    auto lookup_result = codegen->MakeFreshIdentifier("aggPayload");
    function->Append(codegen->DeclareVarWithInit(
        lookup_result, codegen->AggHashTableLookup(
                           agg_ht, codegen->MakeExpr(hash_val), key_check_partial_fn_->Name(),
                           codegen->MakeExpr(partial_row), agg_payload_->Name())));

    If check_found(codegen, codegen->IsNilPointer(codegen->MakeExpr(lookup_result)));
    {
      // Link entry.
      auto entry = codegen->AggPartitionIteratorGetRowEntry(iter);
      function->Append(codegen->AggHashTableLinkEntry(agg_ht, entry));
    }
    check_found.Else();
    {
      // Merge partial aggregate.
      for (uint32_t term_idx = 0; term_idx < GetAggPlan().GetAggregateTerms().size(); term_idx++) {
        auto lhs = GetAggregateTermPtr(lookup_result, term_idx);
        auto rhs = GetAggregateTermPtr(partial_row, term_idx);
        function->Append(codegen->AggregatorMerge(lhs, rhs));
      }
    }
    check_found.EndIf();
  }
}

void HashAggregationTranslator::GeneratePartialKeyCheckFunction(
    util::RegionVector<ast::FunctionDecl *> *top_level_funcs) {
  auto codegen = GetCodeGen();

  auto lhs_arg = codegen->MakeIdentifier("lhs");
  auto rhs_arg = codegen->MakeIdentifier("rhs");
  auto params = codegen->MakeFieldList({
      codegen->MakeField(lhs_arg, codegen->PointerType(agg_payload_->Name())),
      codegen->MakeField(rhs_arg, codegen->PointerType(agg_payload_->Name())),
  });
  auto name = build_pipeline_.ConstructPipelineFunctionName("KeyCheckPartial");
  auto ret_type = codegen->BuiltinType(ast::BuiltinType::Kind::Bool);
  FunctionBuilder builder(codegen, codegen->MakeIdentifier(name), std::move(params), ret_type);
  {
    for (uint32_t term_idx = 0; term_idx < GetAggPlan().GetGroupByTerms().size(); term_idx++) {
      auto lhs = GetGroupByTerm(lhs_arg, term_idx);
      auto rhs = GetGroupByTerm(rhs_arg, term_idx);
      If check_match(codegen, codegen->Compare(parsing::Token::Type::BANG_EQUAL, lhs, rhs));
      builder.Append(codegen->Return(codegen->ConstBool(false)));
    }
    builder.Append(codegen->Return(codegen->ConstBool(true)));
  }
  top_level_funcs->push_back(key_check_partial_fn_ = builder.Finish());
}

void HashAggregationTranslator::GenerateMergeOverflowPartitionsFunction(
    util::RegionVector<ast::FunctionDecl *> *top_level_funcs) {
  // The partition merge function has the following signature:
  // (*QueryState,*AggregationHashTable,*AHTOverflowPartitionIterator) -> nil

  auto codegen = GetCodeGen();
  auto params = GetCompilationContext()->QueryParams();

  // Then the aggregation hash table and the overflow partition iterator.
  params.push_back(
      codegen->MakeField(codegen->MakeIdentifier("aggHashTable"),
                         codegen->PointerType(ast::BuiltinType::AggregationHashTable)));
  params.push_back(
      codegen->MakeField(codegen->MakeIdentifier("ahtOvfIter"),
                         codegen->PointerType(ast::BuiltinType::AHTOverflowPartitionIterator)));

  auto ret_type = codegen->BuiltinType(ast::BuiltinType::Kind::Nil);

  auto name = build_pipeline_.ConstructPipelineFunctionName("MergePartitions");
  FunctionBuilder builder(codegen, codegen->MakeIdentifier(name), std::move(params), ret_type);
  {
    // Dummy declaration to avoid LLVM gen bug: var x = 0.
    builder.Append(codegen->DeclareVarNoInit(codegen->MakeIdentifier("x"), codegen->Const8(0)));
    // Main function.
    MergeOverflowPartitions(&builder);
  }
  top_level_funcs->push_back(merge_partitions_fn_ = builder.Finish());
}

void HashAggregationTranslator::GenerateKeyCheckFunction(
    util::RegionVector<ast::FunctionDecl *> *top_level_funcs) {
  auto codegen = GetCodeGen();
  auto agg_payload = codegen->MakeIdentifier("aggPayload");
  auto agg_values = codegen->MakeIdentifier("aggValues");
  auto params = codegen->MakeFieldList({
      codegen->MakeField(agg_payload, codegen->PointerType(agg_payload_->Name())),
      codegen->MakeField(agg_values, codegen->PointerType(agg_values_->Name())),
  });
  auto name = build_pipeline_.ConstructPipelineFunctionName("KeyCheck");
  auto ret_type = codegen->BuiltinType(ast::BuiltinType::Kind::Bool);
  FunctionBuilder builder(codegen, codegen->MakeIdentifier(name), std::move(params), ret_type);
  {
    for (uint32_t term_idx = 0; term_idx < GetAggPlan().GetGroupByTerms().size(); term_idx++) {
      auto lhs = GetGroupByTerm(agg_payload, term_idx);
      auto rhs = GetGroupByTerm(agg_values, term_idx);
      If check_match(codegen, codegen->Compare(parsing::Token::Type::BANG_EQUAL, lhs, rhs));
      builder.Append(codegen->Return(codegen->ConstBool(false)));
    }
    builder.Append(codegen->Return(codegen->ConstBool(true)));
  }
  top_level_funcs->push_back(key_check_fn_ = builder.Finish());
}

void HashAggregationTranslator::DefineHelperFunctions(
    util::RegionVector<ast::FunctionDecl *> *top_level_funcs) {
  if (build_pipeline_.IsParallel()) {
    GeneratePartialKeyCheckFunction(top_level_funcs);
    GenerateMergeOverflowPartitionsFunction(top_level_funcs);
  }
  GenerateKeyCheckFunction(top_level_funcs);
}

void HashAggregationTranslator::InitializeAggregationHashTable(ast::Expr *agg_ht) const {
  auto codegen = GetCodeGen();
  auto func = codegen->CurrentFunction();
  func->Append(codegen->AggHashTableInit(agg_ht, GetMemoryPool(), agg_payload_->Name()));
}

void HashAggregationTranslator::TearDownAggregationHashTable(ast::Expr *agg_ht) const {
  auto codegen = GetCodeGen();
  auto func = codegen->CurrentFunction();
  func->Append(codegen->AggHashTableFree(agg_ht));
}

void HashAggregationTranslator::InitializeQueryState() const {
  InitializeAggregationHashTable(GetQueryStateEntryPtr(agg_ht_slot_));
}

void HashAggregationTranslator::TearDownQueryState() const {
  TearDownAggregationHashTable(GetQueryStateEntryPtr(agg_ht_slot_));
}

void HashAggregationTranslator::DeclarePipelineState(PipelineContext *pipeline_context) {
  if (IsBuildPipeline(pipeline_context->GetPipeline()) && build_pipeline_.IsParallel()) {
    auto codegen = GetCodeGen();
    tl_agg_ht_slot_ = pipeline_context->DeclareStateEntry(
        codegen, "aggHashTable", codegen->BuiltinType(ast::BuiltinType::AggregationHashTable));
  }
}

void HashAggregationTranslator::InitializePipelineState(
    const PipelineContext &pipeline_context) const {
  if (IsBuildPipeline(pipeline_context.GetPipeline()) && build_pipeline_.IsParallel()) {
    InitializeAggregationHashTable(
        pipeline_context.GetThreadStateEntryPtr(GetCodeGen(), tl_agg_ht_slot_));
  }
}

void HashAggregationTranslator::TearDownPipelineState(
    const PipelineContext &pipeline_context) const {
  if (IsBuildPipeline(pipeline_context.GetPipeline()) && build_pipeline_.IsParallel()) {
    TearDownAggregationHashTable(
        pipeline_context.GetThreadStateEntryPtr(GetCodeGen(), tl_agg_ht_slot_));
  }
}

ast::Expr *HashAggregationTranslator::GetGroupByTerm(ast::Identifier agg_row,
                                                     uint32_t attr_idx) const {
  auto codegen = GetCodeGen();
  auto member = codegen->MakeIdentifier(kGroupByTermAttrPrefix + std::to_string(attr_idx));
  return codegen->AccessStructMember(codegen->MakeExpr(agg_row), member);
}

ast::Expr *HashAggregationTranslator::GetAggregateTerm(ast::Identifier agg_row,
                                                       uint32_t attr_idx) const {
  auto codegen = GetCodeGen();
  auto member = codegen->MakeIdentifier(kAggregateTermAttrPrefix + std::to_string(attr_idx));
  return codegen->AccessStructMember(codegen->MakeExpr(agg_row), member);
}

ast::Expr *HashAggregationTranslator::GetAggregateTermPtr(ast::Identifier agg_row,
                                                          uint32_t attr_idx) const {
  return GetCodeGen()->AddressOf(GetAggregateTerm(agg_row, attr_idx));
}

ast::VariableDecl *HashAggregationTranslator::FillInputValues(FunctionBuilder *function,
                                                              WorkContext *ctx) const {
  auto codegen = GetCodeGen();

  // var aggValues : AggValues
  auto agg_values = codegen->DeclareVarNoInit(codegen->MakeFreshIdentifier("aggValues"),
                                              codegen->MakeExpr(agg_values_->Name()));
  function->Append(agg_values);

  // Populate the grouping terms.
  uint32_t term_idx = 0;
  for (const auto &term : GetAggPlan().GetGroupByTerms()) {
    auto lhs = GetGroupByTerm(agg_values->Name(), term_idx);
    auto rhs = ctx->DeriveValue(*term, this);
    function->Append(codegen->Assign(lhs, rhs));
    term_idx++;
  }

  // Populate the raw aggregate values.
  term_idx = 0;
  for (const auto &term : GetAggPlan().GetAggregateTerms()) {
    auto lhs = GetAggregateTerm(agg_values->Name(), term_idx);
    auto rhs = ctx->DeriveValue(*term->GetChild(0), this);
    function->Append(codegen->Assign(lhs, rhs));
    term_idx++;
  }

  return agg_values;
}

ast::VariableDecl *HashAggregationTranslator::HashInputKeys(FunctionBuilder *function,
                                                            ast::Identifier agg_values) const {
  std::vector<ast::Expr *> keys;
  for (uint32_t term_idx = 0; term_idx < GetAggPlan().GetGroupByTerms().size(); term_idx++) {
    keys.push_back(GetGroupByTerm(agg_values, term_idx));
  }

  // var hashVal = @hash(...)
  auto codegen = GetCodeGen();
  auto hash_val =
      codegen->DeclareVarWithInit(codegen->MakeFreshIdentifier("hashVal"), codegen->Hash(keys));
  function->Append(hash_val);

  return hash_val;
}

ast::VariableDecl *HashAggregationTranslator::PerformLookup(FunctionBuilder *function,
                                                            ast::Expr *agg_ht,
                                                            ast::Identifier hash_val,
                                                            ast::Identifier agg_values) const {
  auto codegen = GetCodeGen();
  // var aggPayload = @ptrCast(*AggPayload, @aggHTLookup())
  auto lookup_result = codegen->AggHashTableLookup(
      agg_ht, codegen->MakeExpr(hash_val), key_check_fn_->Name(),
      codegen->AddressOf(codegen->MakeExpr(agg_values)), agg_payload_->Name());
  auto agg_payload =
      codegen->DeclareVarWithInit(codegen->MakeFreshIdentifier("aggPayload"), lookup_result);
  function->Append(agg_payload);

  return agg_payload;
}

void HashAggregationTranslator::ConstructNewAggregate(FunctionBuilder *function, ast::Expr *agg_ht,
                                                      ast::Identifier agg_payload,
                                                      ast::Identifier agg_values,
                                                      ast::Identifier hash_val) const {
  auto codegen = GetCodeGen();

  // aggRow = @ptrCast(*AggPayload, @aggHTInsert(&state.agg_table, agg_hash_val))
  bool partitioned = build_pipeline_.IsParallel();
  auto insert_call = codegen->AggHashTableInsert(agg_ht, codegen->MakeExpr(hash_val), partitioned,
                                                 agg_payload_->Name());
  function->Append(codegen->Assign(codegen->MakeExpr(agg_payload), insert_call));

  // Copy the grouping keys.
  for (uint32_t term_idx = 0; term_idx < GetAggPlan().GetGroupByTerms().size(); term_idx++) {
    auto lhs = GetGroupByTerm(agg_payload, term_idx);
    auto rhs = GetGroupByTerm(agg_values, term_idx);
    function->Append(codegen->Assign(lhs, rhs));
  }

  // Initialize all aggregate terms.
  for (uint32_t term_idx = 0; term_idx < GetAggPlan().GetAggregateTerms().size(); term_idx++) {
    auto agg_term = GetAggregateTermPtr(agg_payload, term_idx);
    function->Append(codegen->AggregatorInit(agg_term));
  }
}

void HashAggregationTranslator::AdvanceAggregate(FunctionBuilder *function,
                                                 ast::Identifier agg_payload,
                                                 ast::Identifier agg_values) const {
  auto codegen = GetCodeGen();
  for (uint32_t term_idx = 0; term_idx < GetAggPlan().GetAggregateTerms().size(); term_idx++) {
    auto agg = GetAggregateTermPtr(agg_payload, term_idx);
    auto val = GetAggregateTermPtr(agg_values, term_idx);
    function->Append(codegen->AggregatorAdvance(agg, val));
  }
}

void HashAggregationTranslator::UpdateAggregates(WorkContext *work_context,
                                                 ast::Expr *agg_ht) const {
  auto codegen = GetCodeGen();
  auto function = codegen->CurrentFunction();

  auto agg_values = FillInputValues(function, work_context);
  auto hash_val = HashInputKeys(function, agg_values->Name());
  auto agg_payload = PerformLookup(function, agg_ht, hash_val->Name(), agg_values->Name());
  If check_new_agg(codegen, codegen->IsNilPointer(codegen->MakeExpr(agg_payload->Name())));
  {
    ConstructNewAggregate(function, agg_ht, agg_payload->Name(), agg_values->Name(),
                          hash_val->Name());
  }
  check_new_agg.EndIf();

  // Advance aggregate.
  AdvanceAggregate(function, agg_payload->Name(), agg_values->Name());
}

void HashAggregationTranslator::ScanAggregationHashTable(WorkContext *work_context,
                                                         ast::Expr *agg_ht) const {
  auto codegen = GetCodeGen();
  auto function = codegen->CurrentFunction();

  // var ahtIterBase: AHTIterator
  auto aht_iter_base = codegen->MakeFreshIdentifier("ahtIterBase");
  function->Append(codegen->DeclareVarNoInit(aht_iter_base,
                                             codegen->BuiltinType(ast::BuiltinType::AHTIterator)));

  // var ahtIter = &ahtIterBase
  auto aht_iter = codegen->MakeFreshIdentifier("ahtIter");
  function->Append(
      codegen->DeclareVarWithInit(aht_iter, codegen->AddressOf(codegen->MakeExpr(aht_iter_base))));

  Loop loop(
      codegen,
      codegen->MakeStmt(codegen->AggHashTableIteratorInit(codegen->MakeExpr(aht_iter), agg_ht)),
      codegen->AggHashTableIteratorHasNext(codegen->MakeExpr(aht_iter)),
      codegen->MakeStmt(codegen->AggHashTableIteratorNext(codegen->MakeExpr(aht_iter))));
  {
    // var aggRow = @ahtIterGetRow()
    function->Append(codegen->DeclareVarWithInit(
        agg_row_var_name_,
        codegen->AggHashTableIteratorGetRow(codegen->MakeExpr(aht_iter), agg_payload_->Name())));

    // Check having clause.
    if (const auto having = GetAggPlan().GetHavingClausePredicate(); having != nullptr) {
      If check_having(codegen, work_context->DeriveValue(*having, this));
      work_context->Push();
    } else {
      work_context->Push();
    }
  }
  loop.EndLoop();

  // Close iterator.
  function->Append(codegen->AggHashTableIteratorClose(codegen->MakeExpr(aht_iter)));
}

void HashAggregationTranslator::PerformPipelineWork(WorkContext *work_context) const {
  auto codegen = GetCodeGen();
  if (IsBuildPipeline(work_context->GetPipeline())) {
    if (build_pipeline_.IsParallel()) {
      auto agg_ht = work_context->GetThreadStateEntryPtr(codegen, tl_agg_ht_slot_);
      UpdateAggregates(work_context, agg_ht);
    } else {
      UpdateAggregates(work_context, GetQueryStateEntryPtr(agg_ht_slot_));
    }
  } else {
    TPL_ASSERT(IsProducePipeline(work_context->GetPipeline()),
               "Pipeline is unknown to hash aggregation translator");
    if (GetPipeline()->IsParallel()) {
      // In parallel-mode, we would've issued a parallel partitioned scan. In
      // this case, the aggregation hash table we're to scan is provided as a
      // function parameter; specifically, the last argument in the worker
      // function which we're generating right now. Pull it out.
      auto agg_ht_param_position = GetCompilationContext()->QueryParams().size();
      auto agg_ht = codegen->CurrentFunction()->GetParameterByPosition(agg_ht_param_position);
      ScanAggregationHashTable(work_context, agg_ht);
    } else {
      ScanAggregationHashTable(work_context, GetQueryStateEntryPtr(agg_ht_slot_));
    }
  }
}

void HashAggregationTranslator::FinishPipelineWork(const PipelineContext &pipeline_context) const {
  if (IsBuildPipeline(pipeline_context.GetPipeline()) && build_pipeline_.IsParallel()) {
    auto codegen = GetCodeGen();
    auto function = codegen->CurrentFunction();
    auto global_agg_ht = GetQueryStateEntryPtr(agg_ht_slot_);
    auto thread_state_container = GetThreadStateContainer();
    auto tl_agg_ht_offset = pipeline_context.GetThreadStateEntryOffset(codegen, tl_agg_ht_slot_);
    function->Append(codegen->AggHashTableMovePartitions(
        global_agg_ht, thread_state_container, tl_agg_ht_offset, merge_partitions_fn_->Name()));
  }
}

ast::Expr *HashAggregationTranslator::GetChildOutput(WorkContext *work_context,
                                                     UNUSED uint32_t child_idx,
                                                     uint32_t attr_idx) const {
  TPL_ASSERT(child_idx == 0, "Aggregations can only have a single child.");
  if (IsProducePipeline(work_context->GetPipeline())) {
    if (attr_idx < GetAggPlan().GetGroupByTerms().size()) {
      return GetGroupByTerm(agg_row_var_name_, attr_idx);
    }
    return GetCodeGen()->AggregatorResult(GetAggregateTermPtr(agg_row_var_name_, attr_idx));
  }

  // The request is in the build pipeline. Forward to child translator.
  const auto child_translator = GetCompilationContext()->LookupTranslator(*GetPlan().GetChild(0));
  return child_translator->GetOutput(work_context, attr_idx);
}

util::RegionVector<ast::FieldDecl *> HashAggregationTranslator::GetWorkerParams() const {
  TPL_ASSERT(build_pipeline_.IsParallel(),
             "Should not issue parallel scan if pipeline isn't parallelized.");
  auto codegen = GetCodeGen();
  return codegen->MakeFieldList(
      {codegen->MakeField(codegen->MakeIdentifier("aggHashTable"),
                          codegen->PointerType(ast::BuiltinType::AggregationHashTable))});
}

void HashAggregationTranslator::LaunchWork(ast::Identifier work_func_name) const {
  TPL_ASSERT(build_pipeline_.IsParallel(),
             "Should not issue parallel scan if pipeline isn't parallelized.");
  auto codegen = GetCodeGen();
  auto function = codegen->CurrentFunction();
  function->Append(codegen->AggHashTableParallelScan(GetQueryStateEntryPtr(agg_ht_slot_),
                                                     GetQueryStatePtr(), GetThreadStateContainer(),
                                                     work_func_name));
}

}  // namespace tpl::sql::codegen
