#include "sql/codegen/expression/binary_expression_translator.h"

#include "spdlog/fmt/fmt.h"

#include "common/exception.h"
#include "sql/codegen/codegen.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/consumer_context.h"

namespace tpl::sql::codegen {

BinaryExpressionTranslator::BinaryExpressionTranslator(const planner::BinaryExpression &expr,
                                                       CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {
  // Prepare the left and right expression subtrees for translation.
  compilation_context->Prepare(*expr.GetLeft());
  compilation_context->Prepare(*expr.GetRight());
}

edsl::ValueVT BinaryExpressionTranslator::DeriveValue(ConsumerContext *ctx,
                                                      const ColumnValueProvider *cvp) const {
  auto left_val = ctx->DeriveValue(*GetBinaryExpression().GetLeft(), cvp);
  auto right_val = ctx->DeriveValue(*GetBinaryExpression().GetRight(), cvp);

  switch (const auto op = GetExpressionAs<planner::BinaryExpression>().GetOp(); op) {
    case KnownOperator::Add:
      return edsl::ArithmeticOp(parsing::Token::Type::PLUS, left_val, right_val);
    case KnownOperator::Sub:
      return edsl::ArithmeticOp(parsing::Token::Type::MINUS, left_val, right_val);
    case KnownOperator::Mul:
      return edsl::ArithmeticOp(parsing::Token::Type::STAR, left_val, right_val);
    case KnownOperator::Div:
      return edsl::ArithmeticOp(parsing::Token::Type::SLASH, left_val, right_val);
    case KnownOperator::Rem:
      return edsl::ArithmeticOp(parsing::Token::Type::PERCENT, left_val, right_val);
    default: {
      throw NotImplementedException(
          fmt::format("Translation of arithmetic type {}", KnownOperatorToString(op, true)));
    }
  }
}

}  // namespace tpl::sql::codegen
