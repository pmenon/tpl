#pragma once

#include <vector>

#include "sql/planner/expressions/abstract_expression.h"

namespace tpl::sql::planner {

/**
 * Represents a logical comparison expression.
 */
class ComparisonExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new comparison expression.
   * @param cmp_type type of comparison
   * @param children vector containing exactly two children, left then right
   */
  ComparisonExpression(const ExpressionType cmp_type,
                       std::vector<const AbstractExpression *> &&children)
      : AbstractExpression(cmp_type, sql::TypeId::Boolean, std::move(children)) {}
};

}  // namespace tpl::sql::planner
