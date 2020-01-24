#include "sql/codegen/expression/column_value_translator.h"

#include "sql/codegen/work_context.h"

namespace tpl::sql::codegen {

ColumnValueTranslator::ColumnValueTranslator(const planner::ColumnValueExpression &expr,
                                             CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {}

ast::Expr *ColumnValueTranslator::DeriveValue(UNUSED WorkContext *ctx,
                                              const ColumnValueProvider *provider) const {
  auto &col_expr = GetExpressionAs<const planner::ColumnValueExpression>();
  return provider->GetTableColumn(col_expr.GetColumnOid());
}

}  // namespace tpl::sql::codegen
