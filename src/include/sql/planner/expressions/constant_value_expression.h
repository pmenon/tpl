#pragma once

#include <vector>

#include "sql/generic_value.h"
#include "sql/planner/expressions/abstract_expression.h"

namespace tpl::sql::planner {

/**
 * Represents a logical constant expression.
 */
class ConstantValueExpression : public AbstractExpression {
 public:
  /**
   * Instantiate a new constant value expression.
   * @param value value to be held
   */
  explicit ConstantValueExpression(GenericValue value)
      : AbstractExpression(ExpressionType::VALUE_CONSTANT, value.GetTypeId(), {}),
        value_(std::move(value)) {}

  GenericValue GetValue() const { return value_; }

 private:
  /**
   * Value of the constant value expression
   */
  GenericValue value_;
};
}  // namespace tpl::sql::planner
