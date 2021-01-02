#pragma once

#include "common/common.h"
#include "sql/planner/expressions/abstract_expression.h"

namespace tpl::sql::planner {

class ExpressionUtil : public AllStatic {
 public:
  /**
   * @return True if the given expression is of the form: col <op> const_val. False otherwise.
   */
  static bool IsColumnCompareWithConst(const AbstractExpression &expr);

  /**
   * @return True if the given expression is of the form: const <op> col. False otherwise.
   */
  static bool IsConstCompareWithColumn(const AbstractExpression &expr);

  /**
   * @return True if the given expression is a LIKE or NOT LIKE expression. False otherwise.
   */
  static bool IsLikeComparison(const AbstractExpression &expr);
};

}  // namespace tpl::sql::planner
