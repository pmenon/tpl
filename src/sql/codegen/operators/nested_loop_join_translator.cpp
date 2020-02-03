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
  // NLJ joins aren't parallel ... yet.
  pipeline->RegisterStep(this, Pipeline::Parallelism::Serial);

  // Prepare all children in this pipeline.
  for (const auto child_plan : plan.GetChildren()) {
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
      work_context->Push(function);
    }
    cond.EndIf();
  } else {
    // No join predicate. Push to next operator in pipeline.
    work_context->Push(function);
  }
}

ast::Expr *NestedLoopJoinTranslator::GetChildOutput(WorkContext *work_context, uint32_t child_idx,
                                                    uint32_t attr_idx) const {
  const auto child_translator =
      GetCompilationContext()->LookupTranslator(*GetPlan().GetChild(child_idx));
  return child_translator->GetOutput(work_context, attr_idx);
}

}  // namespace tpl::sql::codegen
