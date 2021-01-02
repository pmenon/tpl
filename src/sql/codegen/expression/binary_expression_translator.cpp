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

ast::Expression *BinaryExpressionTranslator::DeriveValue(
    ConsumerContext *context, const ColumnValueProvider *provider) const {
  auto left_val = context->DeriveValue(*GetBinaryExpression().GetLeft(), provider);
  auto right_val = context->DeriveValue(*GetBinaryExpression().GetRight(), provider);

  switch (const auto op = GetExpressionAs<planner::BinaryExpression>().GetOp(); op) {
    case KnownOperator::Add:
      return codegen_->BinaryOp(parsing::Token::Type::PLUS, left_val, right_val);
    case KnownOperator::Sub:
      return codegen_->BinaryOp(parsing::Token::Type::MINUS, left_val, right_val);
    case KnownOperator::Mul:
      return codegen_->BinaryOp(parsing::Token::Type::STAR, left_val, right_val);
    case KnownOperator::Div:
      return codegen_->BinaryOp(parsing::Token::Type::SLASH, left_val, right_val);
    case KnownOperator::Rem:
      return codegen_->BinaryOp(parsing::Token::Type::PERCENT, left_val, right_val);
    default: {
      throw NotImplementedException(
          fmt::format("Translation of arithmetic type {}", KnownOperatorToString(op, true)));
    }
  }
}

}  // namespace tpl::sql::codegen
