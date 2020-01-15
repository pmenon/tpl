#pragma once

#include "sql/codegen/expression/expression_translator.h"
#include "sql/planner/expressions/constant_value_expression.h"

namespace tpl::sql::codegen {

/**
 * A translator for a constant value.
 */
class ConstantTranslator : public ExpressionTranslator {
 public:
  /**
   * Create a translator for the given constant value expression.
   * @param expr The expression to translate.
   * @param compilation_context The context in which translation occurs.
   */
  ConstantTranslator(const planner::ConstantValueExpression &expr,
                     CompilationContext *compilation_context);

  /**
   * Derive the value of the expression.
   * @param ctx The context containing collected subexpressions.
   * @return The value of the expression.
   */
  ast::Expr *DeriveValue(ConsumerContext *ctx) const override;
};

}  // namespace tpl::sql::codegen
