#include "sql/codegen/expression/cast_translator.h"

#include "common/macros.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/consumer_context.h"
#include "sql/planner/expressions/operator_expression.h"

namespace tpl::sql::codegen {

CastTranslator::CastTranslator(const planner::OperatorExpression &expr,
                               CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {
  TPL_ASSERT(expr.NumChildren() == 1, "Cast expression expected to have single input.");
  compilation_context->Prepare(*expr.GetChild(0));
}

ast::Expression *CastTranslator::DeriveValue(ConsumerContext *context,
                                             const ColumnValueProvider *provider) const {
  ast::Expression *input = context->DeriveValue(*GetExpression().GetChild(0), provider);
  const auto input_type = GetExpression().GetChild(0)->GetReturnValueType();
  const auto output_type = GetExpression().GetReturnValueType();
  return codegen_->ConvertSql(input, input_type, output_type);
}

}  // namespace tpl::sql::codegen
