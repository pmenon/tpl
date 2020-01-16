#include "sql/codegen/operators/operator_translator.h"

#include "sql/codegen/compilation_context.h"
#include "sql/planner/plannodes/abstract_plan_node.h"

namespace tpl::sql::codegen {

OperatorTranslator::OperatorTranslator(const planner::AbstractPlanNode &plan,
                                       CompilationContext *compilation_context, Pipeline *pipeline)
    : plan_(plan), compilation_context_(compilation_context), pipeline_(pipeline) {
  // Register output schema expressions
  for (const auto &output_column : plan.GetOutputSchema()->GetColumns()) {
    compilation_context->Prepare(*output_column.GetExpr());
  }
}

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

void OperatorTranslator::GetChildOutputFields(uint32_t child_index, const std::string &prefix,
                                              util::RegionVector<ast::FieldDecl *> *fields) const {
  CodeGen *codegen = GetCodeGen();
  uint32_t attr_idx = 0;
  for (const auto &col : GetPlan().GetChild(child_index)->GetOutputSchema()->GetColumns()) {
    ast::Identifier field_name = codegen->MakeIdentifier(prefix + std::to_string(attr_idx++));
    ast::Expr *type = codegen->TplType(col.GetExpr()->GetReturnValueType());
    fields->emplace_back(codegen->MakeField(field_name, type));
  }
}

}  // namespace tpl::sql::codegen
