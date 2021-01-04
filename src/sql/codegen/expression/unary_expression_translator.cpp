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
      return !input.As<bool>();
    case KnownOperator::IsNull:
      return edsl::IsValNull(input);
    case KnownOperator::IsNotNull:
      return edsl::IsValNotNull(input);
    case KnownOperator::ExtractYear:
      return edsl::ExtractYear(input.As<ast::x::DateVal>());
    case KnownOperator::ACos:
      return edsl::ACos(input.As<ast::x::RealVal>());
    case KnownOperator::ASin:
      return edsl::ASin(input.As<ast::x::RealVal>());
    case KnownOperator::ATan:
      return edsl::ATan(input.As<ast::x::RealVal>());
    case KnownOperator::Cos:
      return edsl::Cos(input.As<ast::x::RealVal>());
    case KnownOperator::Cot:
      return edsl::Cot(input.As<ast::x::RealVal>());
    case KnownOperator::Sin:
      return edsl::Sin(input.As<ast::x::RealVal>());
    case KnownOperator::Tan:
      return edsl::Tan(input.As<ast::x::RealVal>());
    default:
      throw NotImplementedException(fmt::format("Translation of unary operator expression type {}",
                                                KnownOperatorToString(op, true)));
  }
}

}  // namespace tpl::sql::codegen
