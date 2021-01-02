#include "sql/planner/expressions/expression_util.h"

#include "sql/planner/expressions/comparison_expression.h"

namespace tpl::sql::planner {

bool ExpressionUtil::IsColumnCompareWithConst(const AbstractExpression &expr) {
  return expr.Is<ExpressionType::COMPARISON>() &&
         expr.GetChild(0)->Is<ExpressionType::COLUMN_VALUE>() &&
         expr.GetChild(1)->Is<ExpressionType::CONSTANT>();
}

bool ExpressionUtil::IsConstCompareWithColumn(const AbstractExpression &expr) {
  return expr.Is<ExpressionType::COMPARISON>() &&
         expr.GetChild(0)->Is<ExpressionType::CONSTANT>() &&
         expr.GetChild(1)->Is<ExpressionType::COLUMN_VALUE>();
}

bool ExpressionUtil::IsLikeComparison(const AbstractExpression &expr) {
  if (!expr.Is<ExpressionType::COMPARISON>()) return false;
  auto &cmp = static_cast<const ComparisonExpression &>(expr);
  return cmp.GetKind() == ComparisonKind::LIKE || cmp.GetKind() == ComparisonKind::NOT_LIKE;
}

}  // namespace tpl::sql::planner
