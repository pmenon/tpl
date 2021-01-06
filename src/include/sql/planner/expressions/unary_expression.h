#pragma once

#include "sql/planner/expressions/abstract_expression.h"

namespace tpl::sql::planner {

/**
 * Represents a unary expression.
 */
class UnaryExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new operator.
   * @param return_type The type of the return value from this expression.
   * @param input The input to the unary expression.
   */
  UnaryExpression(const Type &return_type, KnownOperator op, const AbstractExpression *input)
      : AbstractExpression(ExpressionType::UNARY_OPERATOR, return_type, {input}), op_(op) {}

  /** @return The left input to this binary expression. */
  const AbstractExpression *GetInput() const { return GetChild(0); }

  /** @return The (binary) operator to apply to the inputs. */
  KnownOperator GetOp() const { return op_; }

 private:
  // The operator.
  KnownOperator op_;
};

}  // namespace tpl::sql::planner
