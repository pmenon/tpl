#pragma once

#include "sql/codegen/expression/expression_translator.h"
#include "sql/planner/expressions/column_value_expression.h"

namespace tpl::sql::codegen {

/**
 * A translator for column value expressions.
 */
class ColumnValueTranslator : public ExpressionTranslator {
 public:
  /**
   * Create a translator for the given column value.
   * @param expr The expression to translate.
   * @param compilation_context The context in which translation occurs.
   */
  ColumnValueTranslator(const planner::ColumnValueExpression &expr,
                        CompilationContext *compilation_context);

  /**
   * @copydoc ExpressionTranslator::DeriveValue().
   */
  edsl::ValueVT DeriveValue(ConsumerContext *ctx, const ColumnValueProvider *cvp) const override;
};

}  // namespace tpl::sql::codegen
