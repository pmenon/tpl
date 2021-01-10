#pragma once

#include "sql/codegen/expression/expression_translator.h"
#include "sql/planner/expressions/conjunction_expression.h"

namespace tpl::sql::codegen {

/**
 * A translator for conjunction expressions.
 */
class ConjunctionTranslator : public ExpressionTranslator {
 public:
  /**
   * Create a translator for the given conjunction expression.
   * @param expr The expression to translate.
   * @param compilation_context The context in which translation occurs.
   */
  ConjunctionTranslator(const planner::ConjunctionExpression &expr,
                        CompilationContext *compilation_context);

  /**
   * @copydoc ExpressionTranslator::DeriveValue().
   */
  edsl::ValueVT DeriveValue(ConsumerContext *ctx, const ColumnValueProvider *cvp) const override;
};

}  // namespace tpl::sql::codegen
