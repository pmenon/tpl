#include "sql/planner/expressions/operator_expression.h"

namespace tpl::sql::planner {

OperatorExpression::OperatorExpression(const ExpressionType expression_type,
                                       const TypeId return_value_type,
                                       std::vector<const AbstractExpression *> &&children)
    : AbstractExpression(expression_type, return_value_type, std::move(children)) {}

void OperatorExpression::DeriveReturnValueType() {
  if (GetExpressionType() == ExpressionType::OPERATOR_NOT ||
      GetExpressionType() == ExpressionType::OPERATOR_IS_NULL ||
      GetExpressionType() == ExpressionType::OPERATOR_IS_NOT_NULL ||
      GetExpressionType() == ExpressionType::OPERATOR_EXISTS) {
    SetReturnValueType(TypeId::Boolean);
    return;
  }

  // if we are a decimal or int we should take the highest type id of both children
  // This relies on a particular order in types.h
  auto max_type_child = std::ranges::max_element(GetChildren(), [](auto t1, auto t2) {
    return t1->GetReturnValueType() < t2->GetReturnValueType();
  });

  auto type = (*max_type_child)->GetReturnValueType();
  TPL_ASSERT(type <= TypeId::Varchar, "Invalid operand type in Operator Expression.");
  SetReturnValueType(type);
}

}  // namespace tpl::sql::planner
