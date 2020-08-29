#pragma once

#include <utility>
#include <vector>

#include "sql/planner/expressions/abstract_expression.h"

namespace tpl::sql::planner {

/**
 * Represents a logical conjunction expression.
 */
class ConjunctionExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new conjunction expression.
   * @param cmp_type type of conjunction
   * @param children vector containing exactly two children, left then right
   */
  ConjunctionExpression(const ExpressionType cmp_type,
                        std::vector<const AbstractExpression *> &&children)
      : AbstractExpression(cmp_type, sql::TypeId::Boolean, std::move(children)) {}
};

}  // namespace tpl::sql::planner
