#include "sql/codegen/operators/nested_loop_join_translator.h"

#include "sql/codegen/compilation_context.h"
#include "sql/codegen/consumer_context.h"
#include "sql/codegen/if.h"
#include "sql/codegen/pipeline.h"
#include "sql/planner/plannodes/nested_loop_join_plan_node.h"

namespace tpl::sql::codegen {

NestedLoopJoinTranslator::NestedLoopJoinTranslator(const planner::NestedLoopJoinPlanNode &plan,
                                                   CompilationContext *compilation_context,
                                                   Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline) {
  TPL_ASSERT(plan.GetChildrenSize() == 2, "NLJ expected to have only two children.");

  // In a nested loop, only the outer most loop determines the parallelism level.
  // So disable the parallelism check until the last child.
  pipeline->SetParallelCheck(false);
  compilation_context->Prepare(*plan.GetChild(0), pipeline);

  // Re-enable the parallelism check for the outer most loop.
  pipeline->SetParallelCheck(true);
  compilation_context->Prepare(*plan.GetChild(1), pipeline);

  // Prepare join condition.
  if (const auto join_predicate = plan.GetJoinPredicate(); join_predicate != nullptr) {
    compilation_context->Prepare(*join_predicate);
  }
}

void NestedLoopJoinTranslator::Consume(ConsumerContext *context, FunctionBuilder *function) const {
  if (const auto join_predicate = GetNLJPlan().GetJoinPredicate(); join_predicate != nullptr) {
    auto condition = context->DeriveValue(*join_predicate, this).As<bool>();
    If check_condition(function, condition);
    context->Consume(function);
  } else {
    context->Consume(function);
  }
}

}  // namespace tpl::sql::codegen
