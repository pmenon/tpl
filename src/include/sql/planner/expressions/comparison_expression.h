#pragma once

#include <algorithm>
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
   * @param cmp_kind The kind of comparison.
   * @param children vector containing exactly two children, left then right.
   */
  explicit ComparisonExpression(ComparisonKind cmp_kind,
                                std::vector<const AbstractExpression *> &&children)
      : AbstractExpression(ExpressionType::COMPARISON, Type::BooleanType(false),
                           std::move(children)),
        cmp_kind_(cmp_kind) {
    bool nullable = std::ranges::any_of(children, [](auto e) { return e->HasNullableValue(); });
    SetReturnValueType(Type::BooleanType(nullable));
  }

  /**
   * @return The kind of the comparison.
   */
  ComparisonKind GetKind() const { return cmp_kind_; }

 private:
  // The kind of the comparison.
  ComparisonKind cmp_kind_;
};

}  // namespace tpl::sql::planner
