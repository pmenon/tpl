#include "sql/codegen/operators/projection_translator.h"

#include "sql/codegen/compilation_context.h"
#include "sql/codegen/work_context.h"
#include "sql/planner/plannodes/projection_plan_node.h"

namespace tpl::sql::codegen {

ProjectionTranslator::ProjectionTranslator(const planner::ProjectionPlanNode &plan,
                                           CompilationContext *compilation_context,
                                           Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline) {}

void ProjectionTranslator::PerformPipelineWork(WorkContext *work_context) const {
  work_context->Push();
}

ast::Expr *ProjectionTranslator::GetChildOutput(WorkContext *work_context, uint32_t child_idx,
                                                uint32_t attr_idx) const {
  const auto child_translator =
      GetCompilationContext()->LookupTranslator(*GetPlan().GetChild(child_idx));
  return child_translator->GetOutput(work_context, attr_idx);
}

}  // namespace tpl::sql::codegen
