#pragma once

#include "ast/identifier.h"
#include "sql/codegen/expression/expression_translator.h"

namespace tpl::sql::planner {
class CastExpression;
}  // namespace tpl::sql::planner

namespace tpl::sql::codegen {

/**
 * A translator for CAST() or expr::TYPE expressions.
 */
class CastTranslator : public ExpressionTranslator {
 public:
  /**
   * Create a translator for the given cast expression.
   * @param expr The expression to translate.
   * @param compilation_context The context in which translation occurs.
   */
  CastTranslator(const planner::CastExpression &expr, CompilationContext *compilation_context);

  /**
   * Derive the value of the expression.
   * @param context The context containing collected subexpressions.
   * @param provider A provider for specific column values.
   * @return The value of the expression.
   */
  ast::Expression *DeriveValue(ConsumerContext *context,
                               const ColumnValueProvider *provider) const override;

 private:
  const planner::CastExpression &GetCastExpression() const {
    return GetExpressionAs<planner::CastExpression>();
  }
};

}  // namespace tpl::sql::codegen