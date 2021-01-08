#include "sql/codegen/operators/static_aggregation_translator.h"

// For string formatting.
#include "spdlog/fmt/fmt.h"

#include "sql/codegen/compilation_context.h"
#include "sql/codegen/consumer_context.h"
#include "sql/codegen/function_builder.h"
#include "sql/codegen/if.h"
#include "sql/planner/plannodes/aggregate_plan_node.h"

namespace tpl::sql::codegen {

namespace {
constexpr std::string_view kAggAttrPrefix = "agg";
}  // namespace

StaticAggregationTranslator::StaticAggregationTranslator(const planner::AggregatePlanNode &plan,
                                                         CompilationContext *compilation_context,
                                                         Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline),
      payload_struct_(codegen_, "AggPayload", false),
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

  // Build up the structure.
  GeneratePayloadStruct();

  agg_row_ =
      std::make_unique<edsl::VariableVT>(codegen_, "agg_row", payload_struct_.GetPtrToType());

  global_aggs_ = GetQueryState()->DeclareStateEntry("aggs", payload_struct_.GetType());
}

void StaticAggregationTranslator::DeclarePipelineDependencies() const {
  GetPipeline()->AddDependency(build_pipeline_);
}

void StaticAggregationTranslator::GeneratePayloadStruct() {
  uint32_t term_idx = 0;
  for (const auto &term : GetAggPlan().GetAggregateTerms()) {
    auto name = fmt::format("{}{}", kAggAttrPrefix, term_idx++);
    auto type =
        codegen_->AggregateType(term->GetKind(), term->GetReturnValueType().GetPrimitiveTypeId());
    payload_struct_.AddMember(name, type);
  }
  payload_struct_.Seal();
}

void StaticAggregationTranslator::DefineStructsAndFunctions() {}

void StaticAggregationTranslator::DeclarePipelineState(PipelineContext *pipeline_ctx) {
  if (pipeline_ctx->IsForPipeline(build_pipeline_) && pipeline_ctx->IsParallel()) {
    local_aggs_ = pipeline_ctx->DeclarePipelineStateEntry("aggs", payload_struct_.GetType());
  }
}

void StaticAggregationTranslator::GenerateAggregateMergeFunction(
    const PipelineContext &pipeline_ctx) const {
  auto params = pipeline_ctx.PipelineParams();
  FunctionBuilder function(codegen_, merge_func_, std::move(params), codegen_->GetType<void>());
  {
    for (uint32_t term_idx = 0; term_idx < GetAggPlan().GetAggregateTerms().size(); term_idx++) {
      auto lhs = payload_struct_.GetMemberPtr(GetQueryStateEntryGeneric(global_aggs_), term_idx);
      auto rhs =
          payload_struct_.GetMemberPtr(pipeline_ctx.GetStateEntryGeneric(local_aggs_), term_idx);
      function.Append(edsl::AggregatorMerge(lhs, rhs));
    }
  }
  function.Finish();
}

void StaticAggregationTranslator::DefinePipelineFunctions(const PipelineContext &pipeline_ctx) {
  if (pipeline_ctx.IsForPipeline(build_pipeline_) && pipeline_ctx.IsParallel()) {
    GenerateAggregateMergeFunction(pipeline_ctx);
  }
}

void StaticAggregationTranslator::InitializeAggregates(FunctionBuilder *fn,
                                                       const edsl::ReferenceVT &agg) const {
  for (uint32_t term_idx = 0; term_idx < GetAggPlan().GetAggregateTerms().size(); term_idx++) {
    fn->Append(edsl::AggregatorInit(payload_struct_.GetMemberPtr(agg, term_idx)));
  }
}

void StaticAggregationTranslator::InitializeQueryState(FunctionBuilder *function) const {
  InitializeAggregates(function, GetQueryStateEntryGeneric(global_aggs_));
}

void StaticAggregationTranslator::InitializePipelineState(const PipelineContext &pipeline_ctx,
                                                          FunctionBuilder *function) const {
  if (pipeline_ctx.IsForPipeline(build_pipeline_) && pipeline_ctx.IsParallel()) {
    InitializeAggregates(function, pipeline_ctx.GetStateEntryGeneric(local_aggs_));
  }
}

void StaticAggregationTranslator::ProduceAggregates(ConsumerContext *ctx,
                                                    FunctionBuilder *fn) const {
  // var agg_row = &state.aggs
  fn->Append(edsl::Declare(*agg_row_, GetQueryStateEntryPtrGeneric(global_aggs_)));

  if (const auto having = GetAggPlan().GetHavingClausePredicate(); having != nullptr) {
    If check_having(fn, ctx->DeriveValue(*having, this).As<bool>());
    ctx->Consume(fn);
  } else {
    ctx->Consume(fn);
  }
}

void StaticAggregationTranslator::UpdateAggregate(ConsumerContext *ctx, FunctionBuilder *fn,
                                                  const edsl::ReferenceVT &agg) const {
  // Create variables storing all input values.
  std::vector<edsl::VariableVT> vals;
  vals.reserve(GetAggPlan().NumAggregateTerms());
  for (uint32_t idx = 0; const auto &term : GetAggPlan().GetAggregateTerms()) {
    auto name = fmt::format("{}{}", kAggAttrPrefix, idx++);
    auto type = codegen_->GetTPLType(term->GetChild(0)->GetReturnValueType().GetTypeId());
    vals.emplace_back(codegen_, name, type);
  }

  // Store the input values.
  for (uint32_t idx = 0; const auto &term : GetAggPlan().GetAggregateTerms()) {
    fn->Append(edsl::Declare(vals[idx++], ctx->DeriveValue(*term->GetChild(0), this)));
  }

  // Update aggregate.
  for (uint32_t term_idx = 0; term_idx < GetAggPlan().NumAggregateTerms(); term_idx++) {
    auto aggregate = payload_struct_.GetMemberPtr(agg, term_idx);
    fn->Append(edsl::AggregatorAdvance(aggregate, vals[term_idx]));
  }
}

void StaticAggregationTranslator::Consume(ConsumerContext *context,
                                          FunctionBuilder *function) const {
  if (context->IsForPipeline(*GetPipeline())) {
    ProduceAggregates(context, function);
  } else {
    if (context->IsParallel()) {
      function->Append(edsl::Declare(*agg_row_, context->GetStateEntryPtrGeneric(local_aggs_)));
    } else {
      function->Append(edsl::Declare(*agg_row_, GetQueryStateEntryPtrGeneric(global_aggs_)));
    }
    UpdateAggregate(context, function, *agg_row_);
  }
}

void StaticAggregationTranslator::FinishPipelineWork(const PipelineContext &pipeline_ctx,
                                                     FunctionBuilder *function) const {
  if (pipeline_ctx.IsForPipeline(build_pipeline_) && pipeline_ctx.IsParallel()) {
    auto opaque_state = edsl::PtrCast<uint8_t *>(GetQueryStatePtr());
    function->Append(GetThreadStateContainer()->Iterate(opaque_state, merge_func_));
  }
}

edsl::ValueVT StaticAggregationTranslator::GetChildOutput(ConsumerContext *context,
                                                          UNUSED uint32_t child_idx,
                                                          uint32_t attr_idx) const {
  TPL_ASSERT(child_idx == 0, "Aggregations can only have a single child.");
  if (context->IsForPipeline(*GetPipeline())) {
    return edsl::AggregatorResult(payload_struct_.GetMemberPtr(*agg_row_, attr_idx));
  }

  // The request is in the build pipeline. Forward to child translator.
  return OperatorTranslator::GetChildOutput(context, child_idx, attr_idx);
}

void StaticAggregationTranslator::DrivePipeline(const PipelineContext &pipeline_ctx) const {
  TPL_ASSERT(pipeline_ctx.IsForPipeline(*GetPipeline()), "Driving unknown pipeline!");
  GetPipeline()->LaunchSerial(pipeline_ctx);
}

}  // namespace tpl::sql::codegen
