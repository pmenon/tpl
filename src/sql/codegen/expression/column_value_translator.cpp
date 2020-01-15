#include "sql/codegen/expression/column_value_translator.h"

#include "sql/codegen/consumer_context.h"

namespace tpl::sql::codegen {

ColumnValueTranslator::ColumnValueTranslator(const planner::ColumnValueExpression &expr,
                                             CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {}

ast::Expr *ColumnValueTranslator::DeriveValue(ConsumerContext *ctx) const {
  auto &col_expr = GetExpressionAs<const planner::ColumnValueExpression>();
  auto provider = ctx->LookupColumnValueProvider(col_expr.GetColumnOid());
  TPL_ASSERT(provider != nullptr, "No value provider available for column OID");
  return provider->GetValue(ctx);
}

}  // namespace tpl::sql::codegen
