#include "sql/codegen/expression/unary_expression_translator.h"

#include "spdlog/fmt/fmt.h"

#include "common/exception.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/consumer_context.h"

namespace tpl::sql::codegen {

UnaryExpressionTranslator::UnaryExpressionTranslator(const planner::UnaryExpression &expr,
                                                     CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {
  compilation_context->Prepare(*expr.GetInput());
}

edsl::ValueVT UnaryExpressionTranslator::DeriveValue(ConsumerContext *context,
                                                     const ColumnValueProvider *provider) const {
  auto input = context->DeriveValue(*GetUnaryExpression().GetInput(), provider);
  switch (const auto op = GetUnaryExpression().GetOp()) {
    case KnownOperator::Neg:
      return edsl::Negate(input);
    case KnownOperator::LogicalNot:
      return !edsl::Value<bool>(input);
    case KnownOperator::IsNull:
      return edsl::IsValNull(input);
    case KnownOperator::IsNotNull:
      return edsl::IsValNotNull(input);
    case KnownOperator::ExtractYear:
      return edsl::ExtractYear(edsl::Value<ast::x::DateVal>(input));
    case KnownOperator::ACos:
      return edsl::ACos(edsl::Value<ast::x::RealVal>(input));
    case KnownOperator::ASin:
      return edsl::ASin(edsl::Value<ast::x::RealVal>(input));
    case KnownOperator::ATan:
      return edsl::ATan(edsl::Value<ast::x::RealVal>(input));
    case KnownOperator::Cos:
      return edsl::Cos(edsl::Value<ast::x::RealVal>(input));
    case KnownOperator::Cot:
      return edsl::Cot(edsl::Value<ast::x::RealVal>(input));
    case KnownOperator::Sin:
      return edsl::Sin(edsl::Value<ast::x::RealVal>(input));
    case KnownOperator::Tan:
      return edsl::Tan(edsl::Value<ast::x::RealVal>(input));
    default:
      throw NotImplementedException(fmt::format("Translation of unary operator expression type {}",
                                                KnownOperatorToString(op, true)));
  }
}

}  // namespace tpl::sql::codegen
