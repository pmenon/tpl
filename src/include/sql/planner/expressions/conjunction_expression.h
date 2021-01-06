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
   * @param kind The kind of the conjunction.
   * @param children vector containing exactly two children, left then right.
   */
  ConjunctionExpression(ConjunctionKind kind, std::vector<const AbstractExpression *> &&children)
      : AbstractExpression(ExpressionType::CONJUNCTION,
                           Type::BooleanType(std::ranges::any_of(
                               children, [](auto e) { return e->HasNullableValue(); })),
                           std::move(children)),
        kind_(kind) {}

  /** @return The kind of conjunction. */
  ConjunctionKind GetKind() const { return kind_; }

 private:
  // The kind of the conjunction.
  ConjunctionKind kind_;
};

}  // namespace tpl::sql::planner
