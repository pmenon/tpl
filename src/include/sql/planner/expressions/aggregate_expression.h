#pragma once

#include <vector>

#include "sql/planner/expressions/abstract_expression.h"
#include "sql/planner/expressions/expression_defs.h"

namespace tpl::sql::planner {

/**
 * An AggregateExpression is only used for parsing, planning and optimizing.
 */
class AggregateExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new aggregate expression.
   * @param type type of aggregate expression
   * @param children children to be added
   * @param distinct whether to eliminate duplicate values in aggregate function calculations
   */
  AggregateExpression(ExpressionType type, std::vector<const AbstractExpression *> &&children,
                      bool distinct)
      : AbstractExpression(type, children[0]->GetReturnValueType(), std::move(children)),
        distinct_(distinct) {}

  /**
   * @return true if we should eliminate duplicate values in aggregate function calculations
   */
  bool IsDistinct() const { return distinct_; }

 private:
  /**
   * If duplicate rows will be removed
   */
  bool distinct_;
};

}  // namespace tpl::sql::planner
