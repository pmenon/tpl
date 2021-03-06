#pragma once

#include "sql/codegen/expression/expression_translator.h"
#include "sql/planner/expressions/derived_value_expression.h"

namespace tpl::sql::codegen {

/**
 * A translator for derived value expressions.
 */
class DerivedValueTranslator : public ExpressionTranslator {
 public:
  /**
   * Create a translator for the given derived value.
   * @param expr The expression to translate.
   * @param compilation_context The context in which translation occurs.
   */
  DerivedValueTranslator(const planner::DerivedValueExpression &expr,
                         CompilationContext *compilation_context);

  /**
   * @copydoc ExpressionTranslator::DeriveValue().
   */
  edsl::ValueVT DeriveValue(ConsumerContext *ctx, const ColumnValueProvider *cvp) const override;
};

}  // namespace tpl::sql::codegen
