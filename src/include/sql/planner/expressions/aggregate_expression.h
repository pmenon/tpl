#pragma once

#include <utility>
#include <vector>

#include "sql/planner/expressions/abstract_expression.h"
#include "sql/planner/expressions/expression_defs.h"
#include "sql/sql.h"

namespace tpl::sql::planner {

/**
 * An AggregateExpression is only used for parsing, planning and optimizing.
 */
class AggregateExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new aggregate expression.
   * @param kind The kind of aggregate this expression represents.
   * @param children children to be added
   * @param distinct whether to eliminate duplicate values in aggregate function calculations
   */
  AggregateExpression(AggregateKind kind, std::vector<const AbstractExpression *> &&children,
                      bool distinct);

  /**
   * @return The kind of aggregate this is.
   */
  AggregateKind GetKind() const { return kind_; }

  /**
   * @return true if we should eliminate duplicate values in aggregate function calculations.
   */
  bool IsDistinct() const { return distinct_; }

  /**
   * Derive the return type of an aggregate with the given kind and input type.
   * @param kind The kind of aggregate.
   * @param input_type The type of the input to the aggregation.
   * @return The return type.
   */
  static Type DerivedReturnType(AggregateKind kind, const Type &input_type);

 private:
  // The kind of aggregate.
  AggregateKind kind_;
  // If duplicate rows will be removed.
  bool distinct_;
};

}  // namespace tpl::sql::planner
