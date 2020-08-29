#pragma once

#include <utility>
#include <vector>

#include "sql/planner/expressions/abstract_expression.h"

namespace tpl::sql::planner {

/**
 * Represents an operator.
 */
class OperatorExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new operator.
   * @param expression_type type of operator
   * @param return_value_type return type of the operator
   * @param children vector containing arguments to the operator left to right
   */
  OperatorExpression(const ExpressionType expression_type, const TypeId return_value_type,
                     std::vector<const AbstractExpression *> &&children)
      : AbstractExpression(expression_type, return_value_type, std::move(children)) {}

  void DeriveReturnValueType() override {
    // if we are a decimal or int we should take the highest type id of both children
    // This relies on a particular order in types.h
    if (this->GetExpressionType() == ExpressionType::OPERATOR_NOT ||
        this->GetExpressionType() == ExpressionType::OPERATOR_IS_NULL ||
        this->GetExpressionType() == ExpressionType::OPERATOR_IS_NOT_NULL ||
        this->GetExpressionType() == ExpressionType::OPERATOR_EXISTS) {
      SetReturnValueType(TypeId::Boolean);
      return;
    }
    auto children = this->GetChildren();
    auto max_type_child = std::max_element(children.begin(), children.end(), [](auto t1, auto t2) {
      return t1->GetReturnValueType() < t2->GetReturnValueType();
    });
    auto type = (*max_type_child)->GetReturnValueType();
    TPL_ASSERT(type <= TypeId::Varchar, "Invalid operand type in Operator Expression.");
    SetReturnValueType(type);
  }
};

}  // namespace tpl::sql::planner
