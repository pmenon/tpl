#pragma once

#include <vector>

#include "sql/planner/expressions/abstract_expression.h"

namespace tpl::sql::planner {

/**
 * Represents a tuple of values that are derived from nested expressions
 */
class DerivedValueExpression : public AbstractExpression {
 public:
  /**
   * Instantiate a DVE expression.
   * @param type The type of the return value of the expression.
   * @param tuple_idx The index of the tuple.
   * @param value_idx The offset of the value in the tuple.
   */
  DerivedValueExpression(const Type &type, int32_t tuple_idx, int32_t value_idx)
      : AbstractExpression(ExpressionType::DERIVED_VALUE, type, {}),
        tuple_idx_(tuple_idx),
        value_idx_(value_idx) {}

  /**
   * @return The index of the tuple.
   */
  int32_t GetTupleIdx() const { return tuple_idx_; }

  /**
   * @return The offset of the value in the tuple.
   */
  int32_t GetValueIdx() const { return value_idx_; }

 private:
  // Index of the tuple.
  int32_t tuple_idx_;
  // Offset of the value in the tuple.
  int32_t value_idx_;
};

}  // namespace tpl::sql::planner
