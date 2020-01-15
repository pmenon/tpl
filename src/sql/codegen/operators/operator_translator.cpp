#include "sql/codegen/operators/operator_translator.h"

#include "sql/codegen/compilation_context.h"
#include "sql/planner/plannodes/abstract_plan_node.h"

namespace tpl::sql::codegen {

OperatorTranslator::OperatorTranslator(const planner::AbstractPlanNode &plan,
                                       CompilationContext *compilation_context, Pipeline *pipeline)
    : plan_(plan), compilation_context_(compilation_context), pipeline_(pipeline) {}

CodeGen *OperatorTranslator::GetCodeGen() const { return compilation_context_->GetCodeGen(); }

const QueryState &OperatorTranslator::GetQueryState() const {
  return *compilation_context_->GetQueryState();
}

ast::Expr *OperatorTranslator::GetExecutionContext() const {
  const auto exec_ctx_slot = compilation_context_->GetExecutionContextStateSlot();
  return GetQueryState().GetStateEntry(GetCodeGen(), exec_ctx_slot);
}

ast::Expr *OperatorTranslator::GetThreadStateContainer() const {
  return GetCodeGen()->ExecCtxGetTLS(GetExecutionContext());
}

}  // namespace tpl::sql::codegen
