#pragma once

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
   * @param value value to be held.
   */
  ConstantValueExpression(const GenericValue &value)
      : AbstractExpression(ExpressionType::CONSTANT,
                           Type(GetSqlTypeFromInternalType(value.GetTypeId()), value.IsNull()), {}),
        value_(value) {}

  /** @return The constant value this expression represents. */
  const GenericValue &GetValue() const { return value_; }

 private:
  // The constant value.
  GenericValue value_;
};

}  // namespace tpl::sql::planner
