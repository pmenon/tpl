#pragma once

#include "sql/codegen/expression/expression_translator.h"
#include "sql/planner/expressions/builtin_function_expression.h"

namespace tpl::sql::codegen {

/**
 * Builtin Function Translator
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

  ast::Expr *DeriveValue(WorkContext *ctx, const ColumnValueProvider *provider) const override;

 private:
  std::vector<std::unique_ptr<ExpressionTranslator>> arg_translators_;
};
}  // namespace tpl::sql::codegen
