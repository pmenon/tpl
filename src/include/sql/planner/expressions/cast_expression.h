#pragma once

#include "sql/planner/expressions/abstract_expression.h"
#include "sql/sql.h"

namespace tpl::sql::planner {

/**
 * Represents a cast expression.
 */
class CastExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new operator.
   * @param target_type The type to cast the input into.
   * @param input The input to the cast expression.
   */
  CastExpression(const TypeId target_type, const AbstractExpression *input)
      : AbstractExpression(ExpressionType::CAST, target_type, {input}) {}

  /**
   * @return The right input to this binary expression.
   */
  const AbstractExpression *GetInput() const { return GetChild(0); }

  /**
   * @return The target type of the cast.
   */
  TypeId GetTargetType() const { return GetReturnValueType(); }

  /**
   * @return The input type of the cast.
   */
  TypeId GetInputType() const { return GetInput()->GetReturnValueType(); }
};

}  // namespace tpl::sql::planner
