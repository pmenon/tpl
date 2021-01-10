#pragma once

#include "sql/codegen/expression/expression_translator.h"
#include "sql/planner/expressions/comparison_expression.h"

namespace tpl::sql::codegen {

/**
 * A translator for a comparison.
 */
class ComparisonTranslator : public ExpressionTranslator {
 public:
  /**
   * Create a translator for the given comparison expression.
   * @param expr The expression to translate.
   * @param compilation_context The context in which translation occurs.
   */
  ComparisonTranslator(const planner::ComparisonExpression &expr,
                       CompilationContext *compilation_context);

  /**
   * @copydoc ExpressionTranslator::DeriveValue().
   */
  edsl::ValueVT DeriveValue(ConsumerContext *ctx, const ColumnValueProvider *cvp) const override;
};

}  // namespace tpl::sql::codegen
