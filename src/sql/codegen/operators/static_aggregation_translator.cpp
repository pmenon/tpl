#include "sql/codegen/operators/static_aggregation_translator.h"

#include "sql/codegen/compilation_context.h"
#include "sql/codegen/consumer_context.h"
#include "sql/codegen/function_builder.h"
#include "sql/codegen/if.h"
#include "sql/planner/plannodes/aggregate_plan_node.h"

namespace tpl::sql::codegen {

namespace {
constexpr char kAggAttrPrefix[] = "agg";
}  // namespace

StaticAggregationTranslator::StaticAggregationTranslator(const planner::AggregatePlanNode &plan,
                                                         CompilationContext *compilation_context,
                                                         Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline),
      agg_row_var_(codegen_->MakeFreshIdentifier("agg_row")),
      agg_payload_type_(codegen_->MakeFreshIdentifier("AggPayload")),
      agg_values_type_(codegen_->MakeFreshIdentifier("AggValues")),
      merge_func_(codegen_->MakeFreshIdentifier("MergeAggregates")),
      build_pipeline_(this, pipeline->GetPipelineGraph(), Pipeline::Parallelism::Parallel) {
  TPL_ASSERT(plan.GetGroupByTerms().empty(), "Global aggregations shouldn't have grouping keys");
  TPL_ASSERT(plan.GetChildrenSize() == 1, "Global aggregations should only have one child");
  // The produce-side is serial since it only generates one output tuple.
  pipeline->RegisterSource(this, Pipeline::Parallelism::Serial);

  // Prepare the child.
  compilation_context->Prepare(*plan.GetChild(0), &build_pipeline_);

  // Prepare each of the aggregate expressions.
  for (const auto agg_term : plan.GetAggregateTerms()) {
    compilation_context->Prepare(*agg_term->GetChild(0));
  }

  // If there's a having clause, prepare it, too.
  if (const auto having_clause = plan.GetHavingClausePredicate(); having_clause != nullptr) {
    compilation_context->Prepare(*having_clause);
  }

  ast::Expr *payload_type = codegen_->MakeExpr(agg_payload_type_);
  global_aggs_ =
      compilation_context->GetQueryState()->DeclareStateEntry(codegen_, "aggs", payload_type);
}

void StaticAggregationTranslator::DeclarePipelineDependencies() const {
  GetPipeline()->AddDependency(build_pipeline_);
}

ast::StructDecl *StaticAggregationTranslator::GeneratePayloadStruct() {
  auto fields = codegen_->MakeEmptyFieldList();
  fields.reserve(GetAggPlan().GetAggregateTerms().size());

  uint32_t term_idx = 0;
  for (const auto &term : GetAggPlan().GetAggregateTerms()) {
    auto name = codegen_->MakeIdentifier(kAggAttrPrefix + std::to_string(term_idx++));
    auto type = codegen_->AggregateType(term->GetExpressionType(), term->GetReturnValueType());
    fields.push_back(codegen_->MakeField(name, type));
  }
  return codegen_->DeclareStruct(agg_payload_type_, std::move(fields));
}

ast::StructDecl *StaticAggregationTranslator::GenerateValuesStruct() {
  auto fields = codegen_->MakeEmptyFieldList();
  fields.reserve(GetAggPlan().GetAggregateTerms().size());

  uint32_t term_idx = 0;
  for (const auto &term : GetAggPlan().GetAggregateTerms()) {
    auto field_name = codegen_->MakeIdentifier(kAggAttrPrefix + std::to_string(term_idx));
    auto type = codegen_->TplType(term->GetReturnValueType());
    fields.push_back(codegen_->MakeField(field_name, type));
    term_idx++;
  }
  return codegen_->DeclareStruct(agg_values_type_, std::move(fields));
}

void StaticAggregationTranslator::DefineStructsAndFunctions() {
  GeneratePayloadStruct();
  GenerateValuesStruct();
}

void StaticAggregationTranslator::DeclarePipelineState(PipelineContext *pipeline_ctx) {
  if (pipeline_ctx->IsForPipeline(build_pipeline_) && pipeline_ctx->IsParallel()) {
    ast::Expr *payload_type = codegen_->MakeExpr(agg_payload_type_);
    local_aggs_ = pipeline_ctx->DeclarePipelineStateEntry("aggs", payload_type);
  }
}

void StaticAggregationTranslator::GenerateAggregateMergeFunction(
    const PipelineContext &pipeline_ctx) const {
  util::RegionVector<ast::FieldDecl *> params = pipeline_ctx.PipelineParams();
  FunctionBuilder function(codegen_, merge_func_, std::move(params), codegen_->Nil());
  {
    for (uint32_t term_idx = 0; term_idx < GetAggPlan().GetAggregateTerms().size(); term_idx++) {
      auto lhs = GetAggregateTermPtr(GetQueryStateEntry(global_aggs_), term_idx);
      auto rhs = GetAggregateTermPtr(pipeline_ctx.GetStateEntry(local_aggs_), term_idx);
      function.Append(codegen_->AggregatorMerge(lhs, rhs));
    }
  }
  function.Finish();
}

void StaticAggregationTranslator::DefinePipelineFunctions(const PipelineContext &pipeline_ctx) {
  if (pipeline_ctx.IsForPipeline(build_pipeline_) && pipeline_ctx.IsParallel()) {
    GenerateAggregateMergeFunction(pipeline_ctx);
  }
}

ast::Expr *StaticAggregationTranslator::GetAggregateTerm(ast::Expr *agg_row,
                                                         uint32_t attr_idx) const {
  auto member = codegen_->MakeIdentifier(kAggAttrPrefix + std::to_string(attr_idx));
  return codegen_->AccessStructMember(agg_row, member);
}

ast::Expr *StaticAggregationTranslator::GetAggregateTermPtr(ast::Expr *agg_row,
                                                            uint32_t attr_idx) const {
  return codegen_->AddressOf(GetAggregateTerm(agg_row, attr_idx));
}

template <typename F>
void StaticAggregationTranslator::InitializeAggregates(FunctionBuilder *function,
                                                       F agg_provider) const {
  for (uint32_t term_idx = 0; term_idx < GetAggPlan().GetAggregateTerms().size(); term_idx++) {
    ast::Expr *agg_term = GetAggregateTermPtr(agg_provider(), term_idx);
    function->Append(codegen_->AggregatorInit(agg_term));
  }
}

void StaticAggregationTranslator::InitializeQueryState(FunctionBuilder *function) const {
  InitializeAggregates(function, [&]() { return GetQueryStateEntry(global_aggs_); });
}

void StaticAggregationTranslator::InitializePipelineState(const PipelineContext &pipeline_ctx,
                                                          FunctionBuilder *function) const {
  if (pipeline_ctx.IsForPipeline(build_pipeline_) && pipeline_ctx.IsParallel()) {
    InitializeAggregates(function, [&]() { return pipeline_ctx.GetStateEntry(local_aggs_); });
  }
}

void StaticAggregationTranslator::ProduceAggregates(ConsumerContext *context,
                                                    FunctionBuilder *function) const {
  // var agg_row = &state.aggs
  function->Append(codegen_->DeclareVarWithInit(agg_row_var_, GetQueryStateEntryPtr(global_aggs_)));

  if (const auto having = GetAggPlan().GetHavingClausePredicate(); having != nullptr) {
    If check_having(function, context->DeriveValue(*having, this));
    context->Consume(function);
    check_having.EndIf();
  } else {
    context->Consume(function);
  }
}

template <typename F>
void StaticAggregationTranslator::UpdateAggregate(ConsumerContext *ctx, FunctionBuilder *function,
                                                  F agg_provider) const {
  // var aggValues: AggValues
  auto agg_values = codegen_->MakeFreshIdentifier("agg_values");
  function->Append(codegen_->DeclareVarNoInit(agg_values, codegen_->MakeExpr(agg_values_type_)));

  // Fill values.
  uint32_t term_idx = 0;
  for (const auto &term : GetAggPlan().GetAggregateTerms()) {
    ast::Expr *lhs = GetAggregateTerm(codegen_->MakeExpr(agg_values), term_idx++);
    ast::Expr *rhs = ctx->DeriveValue(*term->GetChild(0), this);
    function->Append(codegen_->Assign(lhs, rhs));
  }

  // Update aggregate.
  for (term_idx = 0; term_idx < GetAggPlan().GetAggregateTerms().size(); term_idx++) {
    ast::Expr *aggregate = GetAggregateTermPtr(agg_provider(), term_idx);
    ast::Expr *advance_val = GetAggregateTermPtr(codegen_->MakeExpr(agg_values), term_idx);
    function->Append(codegen_->AggregatorAdvance(aggregate, advance_val));
  }
}

void StaticAggregationTranslator::Consume(ConsumerContext *context,
                                          FunctionBuilder *function) const {
  if (context->IsForPipeline(*GetPipeline())) {
    ProduceAggregates(context, function);
  } else {
    if (context->IsParallel()) {
      const auto agg_provider = [&]() { return context->GetStateEntry(local_aggs_); };
      UpdateAggregate(context, function, agg_provider);
    } else {
      const auto agg_provider = [&]() { return GetQueryStateEntry(global_aggs_); };
      UpdateAggregate(context, function, agg_provider);
    }
  }
}

void StaticAggregationTranslator::FinishPipelineWork(const PipelineContext &pipeline_ctx,
                                                     FunctionBuilder *function) const {
  if (pipeline_ctx.IsForPipeline(build_pipeline_) && pipeline_ctx.IsParallel()) {
    // Merge thread-local aggregates into one.
    ast::Expr *thread_state_container = GetThreadStateContainer();
    ast::Expr *query_state = GetQueryStatePtr();
    function->Append(codegen_->TLSIterate(thread_state_container, query_state, merge_func_));
  }
}

ast::Expr *StaticAggregationTranslator::GetChildOutput(ConsumerContext *context,
                                                       UNUSED uint32_t child_idx,
                                                       uint32_t attr_idx) const {
  TPL_ASSERT(child_idx == 0, "Aggregations can only have a single child.");
  if (context->IsForPipeline(*GetPipeline())) {
    return codegen_->AggregatorResult(
        GetAggregateTermPtr(codegen_->MakeExpr(agg_row_var_), attr_idx));
  }

  // The request is in the build pipeline. Forward to child translator.
  return OperatorTranslator::GetChildOutput(context, child_idx, attr_idx);
}

}  // namespace tpl::sql::codegen
