#pragma once

#include "sql/codegen/expression/expression_translator.h"
#include "sql/planner/expressions/unary_expression.h"

namespace tpl::sql::codegen {

/**
 * A translator for unary expressions.
 */
class UnaryExpressionTranslator : public ExpressionTranslator {
 public:
  /**
   * Create a translator for the given derived value.
   * @param expr The expression to translate.
   * @param compilation_context The context in which translation occurs.
   */
  UnaryExpressionTranslator(const planner::UnaryExpression &expr,
                            CompilationContext *compilation_context);

  /**
   * Derive the value of the expression.
   * @param context The context containing collected subexpressions.
   * @param provider A provider for specific column values.
   * @return The value of the expression.
   */
  edsl::ValueVT DeriveValue(ConsumerContext *context,
                            const ColumnValueProvider *provider) const override;

 private:
  const planner::UnaryExpression GetUnaryExpression() const {
    return GetExpressionAs<planner::UnaryExpression>();
  }
};

}  // namespace tpl::sql::codegen
