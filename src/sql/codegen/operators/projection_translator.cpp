#include "sql/codegen/operators/projection_translator.h"

#include "sql/codegen/compilation_context.h"
#include "sql/codegen/consumer_context.h"
#include "sql/planner/plannodes/projection_plan_node.h"

namespace tpl::sql::codegen {

// The majority of work for projections are performed during expression
// evaluation. In the context of projections, expressions are derived when
// requesting an output from the expression.

ProjectionTranslator::ProjectionTranslator(const planner::ProjectionPlanNode &plan,
                                           CompilationContext *compilation_context,
                                           Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline) {
  if (plan.GetChildrenSize() > 0) {
    TPL_ASSERT(plan.GetChildrenSize() == 1, "Projections expected to have one child");
    compilation_context->Prepare(*plan.GetChild(0), pipeline);
  } else {
    // If there are no children, the projection drives the pipeline serially.
    pipeline->RegisterSource(this, Pipeline::Parallelism::Serial);
  }
}

void ProjectionTranslator::Consume(ConsumerContext *context, FunctionBuilder *function) const {
  context->Consume(function);
}

void ProjectionTranslator::DrivePipeline(const PipelineContext &pipeline_ctx) const {
  TPL_ASSERT(pipeline_ctx.IsForPipeline(*GetPipeline()), "Driving unknown pipeline");
  GetPipeline()->LaunchSerial(pipeline_ctx);
}

}  // namespace tpl::sql::codegen
