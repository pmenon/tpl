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
   * @copydoc ExpressionTranslator::DeriveValue().
   */
  edsl::ValueVT DeriveValue(ConsumerContext *ctx, const ColumnValueProvider *cvp) const override;
};

}  // namespace tpl::sql::codegen
