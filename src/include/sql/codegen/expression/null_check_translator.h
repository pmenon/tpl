#pragma once

#include "sql/codegen/expression/expression_translator.h"
#include "sql/planner/expressions/operator_expression.h"

namespace tpl::sql::codegen {

/**
 * A translator for null-checking expressions.
 */
class NullCheckTranslator : public ExpressionTranslator {
 public:
  /**
   * Create a translator for the given derived value.
   * @param expr The expression to translate.
   * @param compilation_context The context in which translation occurs.
   */
  NullCheckTranslator(const planner::OperatorExpression &expr,
                      CompilationContext *compilation_context);

  /**
   * Derive the value of the expression.
   * @param ctx The context containing collected subexpressions.
   * @return The value of the expression.
   */
  ast::Expr *DeriveValue(ConsumerContext *ctx, const ColumnValueProvider *provider) const override;
};

}  // namespace tpl::sql::codegen
