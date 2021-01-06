#include "sql/codegen/operators/operator_translator.h"

// For string formatting.
#include "spdlog/fmt/fmt.h"

#include "common/exception.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/consumer_context.h"
#include "sql/codegen/edsl/ops.h"
#include "sql/planner/plannodes/abstract_plan_node.h"

namespace tpl::sql::codegen {

OperatorTranslator::OperatorTranslator(const planner::AbstractPlanNode &plan,
                                       CompilationContext *compilation_context, Pipeline *pipeline)
    : plan_(plan),
      compilation_ctx_(compilation_context),
      pipeline_(pipeline),
      codegen_(compilation_context->GetCodeGen()) {
  TPL_ASSERT(plan.GetOutputSchema() != nullptr, "Output schema shouldn't be null");
  // Register this operator.
  pipeline->RegisterStep(this);
  // Prepare all output expressions.
  for (const auto &output_column : plan.GetOutputSchema()->GetColumns()) {
    compilation_context->Prepare(*output_column.GetExpr());
  }
}

edsl::ValueVT OperatorTranslator::GetOutput(ConsumerContext *context, uint32_t attr_idx) const {
  // Check valid output column.
  const auto output_schema = plan_.GetOutputSchema();
  if (attr_idx >= output_schema->NumColumns()) {
    throw CodeGenerationException(fmt::format(
        "Cannot read column {} from '{}' with output schema {}", attr_idx,
        planner::PlanNodeTypeToString(plan_.GetPlanNodeType()), output_schema->ToString()));
  }

  const auto output_expression = output_schema->GetColumn(attr_idx).GetExpr();
  return context->DeriveValue(*output_expression, this);
}

edsl::ValueVT OperatorTranslator::GetChildOutput(ConsumerContext *context, uint32_t child_idx,
                                                 uint32_t attr_idx) const {
  // Check valid child.
  if (child_idx >= plan_.GetChildrenSize()) {
    throw CodeGenerationException(
        fmt::format("Plan type '{}' does not have child at index {}",
                    planner::PlanNodeTypeToString(plan_.GetPlanNodeType()), child_idx));
  }

  // Check valid output column from child.
  auto child_translator = compilation_ctx_->LookupTranslator(*plan_.GetChild(child_idx));
  TPL_ASSERT(child_translator != nullptr, "Missing translator for child!");
  return child_translator->GetOutput(context, attr_idx);
}

ExecutionState *OperatorTranslator::GetQueryState() const {
  return compilation_ctx_->GetQueryState();
}

edsl::ValueVT OperatorTranslator::GetQueryStatePtr() const {
  return GetQueryState()->GetStatePtr(codegen_);
}

edsl::ReferenceVT OperatorTranslator::GetQueryStateEntryGeneric(ExecutionState::RTSlot slot) const {
  return GetQueryState()->GetStateEntryGeneric(codegen_, slot);
}

edsl::ValueVT OperatorTranslator::GetQueryStateEntryPtrGeneric(ExecutionState::RTSlot slot) const {
  return GetQueryState()->GetStateEntryPtrGeneric(codegen_, slot);
}

edsl::Value<ast::x::ExecutionContext *> OperatorTranslator::GetExecutionContext() const {
  return compilation_ctx_->GetExecutionContextPtrFromQueryState();
}

edsl::Value<ast::x::ThreadStateContainer *> OperatorTranslator::GetThreadStateContainer() const {
  return GetExecutionContext()->GetThreadStateContainer();
}

edsl::Value<ast::x::MemoryPool *> OperatorTranslator::GetMemoryPool() const {
  return GetExecutionContext()->GetMemoryPool();
}

const planner::OutputSchema *OperatorTranslator::GetChildOutputSchema(uint32_t child_idx) const {
  return plan_.GetChild(child_idx)->GetOutputSchema();
}

void OperatorTranslator::GetAllChildOutputFields(uint32_t child_idx, std::string_view prefix,
                                                 edsl::Struct *s) const {
  uint32_t attr_idx = 0;
  for (const auto &col : plan_.GetChild(child_idx)->GetOutputSchema()->GetColumns()) {
    auto name = fmt::format("{}{}", prefix, attr_idx++);
    auto type = codegen_->GetTPLType(col.GetExpr()->GetReturnValueType().GetPrimitiveTypeId());
    s->AddMember(name, type);
  }
}

}  // namespace tpl::sql::codegen
