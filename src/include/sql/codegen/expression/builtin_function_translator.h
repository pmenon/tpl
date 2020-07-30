#pragma once

#include "sql/codegen/expression/expression_translator.h"
#include "sql/planner/expressions/builtin_function_expression.h"

namespace tpl::sql::codegen {

/**
 * Translator for TPL builtins functions.
 */
class BuiltinFunctionTranslator : public ExpressionTranslator {
 public:
  /**
   * Create a translator for an expression.
   * @param expr The expression.
   * @param compilation_context The context the translation occurs in.
   */
  BuiltinFunctionTranslator(const planner::BuiltinFunctionExpression &expr,
                            CompilationContext *compilation_context);

  /**
   * Derive the value of the expression.
   * @param ctx The context containing collected subexpressions.
   * @param provider A provider for specific column values.
   * @return The value of the expression.
   */
  ast::Expr *DeriveValue(ConsumerContext *ctx, const ColumnValueProvider *provider) const override;
};

}  // namespace tpl::sql::codegen
