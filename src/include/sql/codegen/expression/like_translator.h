#pragma once

#include "sql/codegen/expression/expression_translator.h"
#include "sql/planner/expressions/comparison_expression.h"

namespace tpl::sql::codegen {

/**
 * A translator for a like-comparison.
 */
class LikeTranslator : public ExpressionTranslator {
 public:
  /**
   * Create a translator for the given like-comparison expression.
   * @param expr The expression to translate.
   * @param compilation_context The context in which translation occurs.
   */
  LikeTranslator(const planner::ComparisonExpression &expr,
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
  const planner::ComparisonExpression &GetComparisonExpression() const {
    return GetExpressionAs<planner::ComparisonExpression>();
  }
};

}  // namespace tpl::sql::codegen
