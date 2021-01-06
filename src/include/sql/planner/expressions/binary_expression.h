#pragma once

#include "sql/planner/expressions/abstract_expression.h"
#include "sql/sql.h"

namespace tpl::sql::planner {

/**
 * Represents a binary expression using a known operator.
 */
class BinaryExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new operator.
   * @param return_type The type of the return value from this expression.
   * @param op The (binary) operator to apply.
   * @param lhs The left input to the binary expression.
   * @param rhs The right input to the binary expression.
   */
  BinaryExpression(const Type &return_type, KnownOperator op, const AbstractExpression *lhs,
                   const AbstractExpression *rhs)
      : AbstractExpression(ExpressionType::BINARY_OPERATOR, return_type, {lhs, rhs}), op_(op) {}

  /**
   * @return The left input to this binary expression.
   */
  const AbstractExpression *GetLeft() const { return GetChild(0); }

  /**
   * @return The right input to this binary expression.
   */
  const AbstractExpression *GetRight() const { return GetChild(1); }

  /**
   * @return The (binary) operator to apply to the inputs.
   */
  KnownOperator GetOp() const { return op_; }

 private:
  // The operator.
  KnownOperator op_;
};

}  // namespace tpl::sql::planner
