#include "sql/codegen/operators/limit_translator.h"

#include "sql/codegen/compilation_context.h"
#include "sql/codegen/consumer_context.h"
#include "sql/codegen/edsl/arithmetic_ops.h"
#include "sql/codegen/edsl/boolean_ops.h"
#include "sql/codegen/edsl/comparison_ops.h"
#include "sql/codegen/edsl/value.h"
#include "sql/codegen/function_builder.h"
#include "sql/codegen/if.h"
#include "sql/planner/plannodes/limit_plan_node.h"

namespace tpl::sql::codegen {

LimitTranslator::LimitTranslator(const planner::LimitPlanNode &plan,
                                 CompilationContext *compilation_context, Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline) {
  TPL_ASSERT(plan.GetOffset() != 0 || plan.GetLimit() != 0, "Both offset and limit cannot be 0");
  // Limits are serial ... for now.
  pipeline->UpdateParallelism(Pipeline::Parallelism::Serial);
  // Prepare child.
  compilation_context->Prepare(*plan.GetChild(0), pipeline);
}

void LimitTranslator::DeclarePipelineState(PipelineContext *pipeline_ctx) {
  count_ = pipeline_ctx->DeclarePipelineStateEntry<uint64_t>("num_tuples");
}

void LimitTranslator::InitializePipelineState(const PipelineContext &pipeline_ctx,
                                              FunctionBuilder *function) const {
  auto count = pipeline_ctx.GetStateEntry(count_);
  function->Append(edsl::Assign(count, edsl::Literal(codegen_, 0ul)));
}

void LimitTranslator::Consume(ConsumerContext *context, FunctionBuilder *function) const {
  const auto &plan = GetPlanAs<planner::LimitPlanNode>();
  const auto offset = plan.GetOffset(), limit = plan.GetLimit();

  const auto count = context->GetStateEntry(count_);
  if (offset != 0 && limit != 0) {
    If check_limit(function, count >= offset && count < offset + limit);
    context->Consume(function);
  } else if (offset != 0) {
    If check_limit(function, count >= offset);
    context->Consume(function);
  } else {
    If check_limit(function, count < limit);
    context->Consume(function);
  }

  function->Append(edsl::Assign(count, count + 1ul));
}

}  // namespace tpl::sql::codegen
