#pragma once

#include "ast/identifier.h"
#include "sql/codegen/expression/expression_translator.h"

namespace tpl::sql::planner {
class CaseExpression;
}  // namespace tpl::sql::planner

namespace tpl::sql::codegen {

/**
 * A translator for case expressions.
 */
class CaseTranslator : public ExpressionTranslator {
 public:
  /**
   * Create a translator for the given arithmetic expression.
   * @param expr The expression to translate.
   * @param compilation_context The context in which translation occurs.
   */
  CaseTranslator(const planner::CaseExpression &expr, CompilationContext *compilation_context);

  /**
   * Derive the value of the expression.
   * @param context The context containing collected subexpressions.
   * @param provider A provider for specific column values.
   * @return The value of the expression.
   */
  edsl::ValueVT DeriveValue(ConsumerContext *context,
                            const ColumnValueProvider *provider) const override;

 private:
  void GenerateCases(const edsl::VariableVT &ret, std::size_t clause_idx, ConsumerContext *context,
                     const ColumnValueProvider *provider) const;
};

}  // namespace tpl::sql::codegen
