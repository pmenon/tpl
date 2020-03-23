#include "sql/codegen/operators/nested_loop_join_translator.h"

#include "sql/codegen/compilation_context.h"
#include "sql/codegen/if.h"
#include "sql/codegen/pipeline.h"
#include "sql/codegen/work_context.h"
#include "sql/planner/plannodes/nested_loop_join_plan_node.h"

namespace tpl::sql::codegen {

NestedLoopJoinTranslator::NestedLoopJoinTranslator(const planner::NestedLoopJoinPlanNode &plan,
                                                   CompilationContext *compilation_context,
                                                   Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline) {
  pipeline->RegisterStep(this, Pipeline::Parallelism::Parallel);

  // In a nested loop, only the outer most loop determines the parallelism level.
  // So disable the parallelism check until the last child.
  pipeline->SetParallelCheck(false);

  // Prepare all children in this pipeline.
  auto num_children = static_cast<uint32_t>(plan.GetChildren().size());
  for (uint32_t i = 0; i < num_children; i++) {
    auto child_plan = plan.GetChild(i);
    if (i == num_children - 1) {
      // Reenable the parallelism check for the outer most loop.
      pipeline->SetParallelCheck(true);
    }
    compilation_context->Prepare(*child_plan, pipeline);
  }

  // Prepare join condition.
  if (plan.GetJoinPredicate() != nullptr) {
    compilation_context->Prepare(*plan.GetJoinPredicate());
  }
}

void NestedLoopJoinTranslator::PerformPipelineWork(WorkContext *work_context,
                                                   FunctionBuilder *function) const {
  const auto *predicate = GetPlanAs<planner::NestedLoopJoinPlanNode>().GetJoinPredicate();
  if (predicate != nullptr) {
    If cond(function, work_context->DeriveValue(*predicate, this));
    {
      // Valid tuple. Push to next operator in pipeline.
      work_context->Consume(function);
    }
    cond.EndIf();
  } else {
    // No join predicate. Push to next operator in pipeline.
    work_context->Consume(function);
  }
}

ast::Expr *NestedLoopJoinTranslator::GetChildOutput(WorkContext *work_context, uint32_t child_idx,
                                                    uint32_t attr_idx) const {
  const auto child_translator =
      GetCompilationContext()->LookupTranslator(*GetPlan().GetChild(child_idx));
  return child_translator->GetOutput(work_context, attr_idx);
}

}  // namespace tpl::sql::codegen
