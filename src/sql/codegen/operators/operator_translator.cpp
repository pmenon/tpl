#include "sql/codegen/operators/operator_translator.h"

#include "sql/codegen/compilation_context.h"
#include "sql/codegen/work_context.h"
#include "sql/planner/plannodes/abstract_plan_node.h"

namespace tpl::sql::codegen {

OperatorTranslator::OperatorTranslator(const planner::AbstractPlanNode &plan,
                                       CompilationContext *compilation_context, Pipeline *pipeline)
    : plan_(plan), compilation_context_(compilation_context), pipeline_(pipeline) {
  TPL_ASSERT(plan.GetOutputSchema() != nullptr, "Output schema shouldn't be null");
  for (const auto &output_column : plan.GetOutputSchema()->GetColumns()) {
    compilation_context->Prepare(*output_column.GetExpr());
  }
}

ast::Expr *OperatorTranslator::GetOutput(WorkContext *work_context, uint32_t attr_idx) const {
  const auto &output_col = GetPlan().GetOutputSchema()->GetColumn(attr_idx);
  return work_context->DeriveValue(*output_col.GetExpr(), this);
}

CodeGen *OperatorTranslator::GetCodeGen() const { return compilation_context_->GetCodeGen(); }

ast::Expr *OperatorTranslator::GetQueryStatePtr() const {
  return compilation_context_->GetQueryState()->GetStatePointer(GetCodeGen());
}

ast::Expr *OperatorTranslator::GetExecutionContext() const {
  return compilation_context_->GetExecutionContextPtrFromQueryState();
}

ast::Expr *OperatorTranslator::GetThreadStateContainer() const {
  return GetCodeGen()->ExecCtxGetTLS(GetExecutionContext());
}

ast::Expr *OperatorTranslator::GetMemoryPool() const {
  return GetCodeGen()->ExecCtxGetMemoryPool(GetExecutionContext());
}

void OperatorTranslator::GetAllChildOutputFields(
    const uint32_t child_index, const std::string &field_name_prefix,
    util::RegionVector<ast::FieldDecl *> *fields) const {
  auto codegen = GetCodeGen();
  auto attr_idx = uint32_t{0};
  for (const auto &col : GetPlan().GetChild(child_index)->GetOutputSchema()->GetColumns()) {
    auto field_name = codegen->MakeIdentifier(field_name_prefix + std::to_string(attr_idx++));
    auto type = codegen->TplType(col.GetExpr()->GetReturnValueType());
    fields->emplace_back(codegen->MakeField(field_name, type));
  }
}

}  // namespace tpl::sql::codegen
