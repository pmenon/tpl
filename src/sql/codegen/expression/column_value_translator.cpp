#include "sql/codegen/expression/column_value_translator.h"

#include "sql/codegen/consumer_context.h"

namespace tpl::sql::codegen {

ColumnValueTranslator::ColumnValueTranslator(const planner::ColumnValueExpression &expr,
                                             CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {}

edsl::ValueVT ColumnValueTranslator::DeriveValue(UNUSED ConsumerContext *ctx,
                                                 const ColumnValueProvider *cvp) const {
  auto &col_expr = GetExpressionAs<const planner::ColumnValueExpression>();
  return cvp->GetTableColumn(col_expr.GetColumnOid());
}

}  // namespace tpl::sql::codegen
