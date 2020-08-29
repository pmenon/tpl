#include "sql/planner/expressions/expression_util.h"

namespace tpl::sql::planner {

bool ExpressionUtil::IsColumnCompareWithConst(const planner::AbstractExpression &expr) {
  if (expr.GetChildrenSize() != 2) {
    return false;
  }
  return IsComparisonExpression(expr.GetExpressionType()) &&
         IsColumnRefExpression(expr.GetChild(0)->GetExpressionType()) &&
         IsConstantExpression(expr.GetChild(1)->GetExpressionType());
}

bool ExpressionUtil::IsConstCompareWithColumn(const planner::AbstractExpression &expr) {
  if (expr.GetChildrenSize() != 2) {
    return false;
  }
  return IsComparisonExpression(expr.GetExpressionType()) &&
         IsConstantExpression(expr.GetChild(0)->GetExpressionType()) &&
         IsColumnRefExpression(expr.GetChild(1)->GetExpressionType());
}

}  // namespace tpl::sql::planner
