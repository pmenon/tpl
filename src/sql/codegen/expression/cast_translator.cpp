#include "sql/codegen/expression/cast_translator.h"

#include "common/macros.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/consumer_context.h"
#include "sql/codegen/edsl/ops.h"
#include "sql/planner/expressions/cast_expression.h"

namespace tpl::sql::codegen {

CastTranslator::CastTranslator(const planner::CastExpression &expr,
                               CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {
  TPL_ASSERT(expr.NumChildren() == 1, "Cast expression expected to have single input.");
  compilation_context->Prepare(*expr.GetInput());
}

edsl::ValueVT CastTranslator::DeriveValue(ConsumerContext *ctx,
                                          const ColumnValueProvider *cvp) const {
  const auto &expr = GetCastExpression();
  const auto input = ctx->DeriveValue(*expr.GetInput(), cvp);
  return edsl::ConvertSql(input, expr.GetTargetType().GetPrimitiveTypeId());
}

}  // namespace tpl::sql::codegen
