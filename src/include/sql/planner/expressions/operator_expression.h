#pragma once

#include <algorithm>
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
                     std::vector<const AbstractExpression *> &&children);

  /**
   * Derive the return type.
   */
  void DeriveReturnValueType() override;
};

}  // namespace tpl::sql::planner
