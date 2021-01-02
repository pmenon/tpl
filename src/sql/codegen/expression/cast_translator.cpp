#include "sql/codegen/expression/cast_translator.h"

#include "common/macros.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/consumer_context.h"
#include "sql/planner/expressions/cast_expression.h"

namespace tpl::sql::codegen {

CastTranslator::CastTranslator(const planner::CastExpression &expr,
                               CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {
  TPL_ASSERT(expr.NumChildren() == 1, "Cast expression expected to have single input.");
  compilation_context->Prepare(*expr.GetInput());
}

ast::Expression *CastTranslator::DeriveValue(ConsumerContext *context,
                                             const ColumnValueProvider *provider) const {
  const auto &expr = GetCastExpression();
  const auto input = context->DeriveValue(*expr.GetInput(), provider);
  return codegen_->ConvertSql(input, expr.GetInputType(), expr.GetTargetType());
}

}  // namespace tpl::sql::codegen
