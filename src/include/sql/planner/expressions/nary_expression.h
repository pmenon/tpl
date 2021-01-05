#pragma once

#include <vector>

#include "sql/planner/expressions/abstract_expression.h"

namespace tpl::sql::planner {

/**
 * Represents an n-ary expression.
 */
class NAryExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new n-ary expression.
   * @param return_type The type of the return value from this expression.
   * @param children The children of this expression.
   */
  NAryExpression(const TypeId return_type, std::vector<const AbstractExpression> &&children)
      : AbstractExpression(ExpressionType::NARY_OPERATOR, return_type, {lhs, rhs}) {}
};

}  // namespace tpl::sql::planner
