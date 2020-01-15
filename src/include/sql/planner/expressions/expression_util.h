#pragma once

#include "common/common.h"
#include "sql/planner/expressions/abstract_expression.h"

namespace tpl::sql::planner {

class ExpressionUtil : public AllStatic {
 public:

  /**
   * @return True if the given expression is of the form: col <op> const_val. False otherwise.
   */
  static bool IsColumnCompareWithConst(const planner::AbstractExpression &expr);

  /**
   * @return True if the given expression is of the form: const <op> col. False otherwise.
   */
  static bool IsConstCompareWithColumn(const planner::AbstractExpression &expr);
};

}  // namespace tpl::sql::planner
