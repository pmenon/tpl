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

ast::Expression *UnaryExpressionTranslator::DeriveValue(ConsumerContext *context,
                                                        const ColumnValueProvider *provider) const {
  auto input = context->DeriveValue(*GetUnaryExpression().GetInput(), provider);
  switch (const auto op = GetUnaryExpression().GetOp()) {
    case KnownOperator::Neg:
      return codegen_->UnaryOp(parsing::Token::Type::MINUS, input);
    case KnownOperator::LogicalNot:
      return codegen_->UnaryOp(parsing::Token::Type::BANG, input);
    case KnownOperator::IsNull:
      return codegen_->CallBuiltin(ast::Builtin::IsValNull, {input});
    case KnownOperator::IsNotNull:
      return codegen_->UnaryOp(parsing::Token::Type::BANG,
                               codegen_->CallBuiltin(ast::Builtin::IsValNull, {input}));
    case KnownOperator::ExtractYear:
      return codegen_->CallBuiltin(ast::Builtin::ExtractYear, {input});
    case KnownOperator::ACos:
      return codegen_->CallBuiltin(ast::Builtin::ACos, {input});
    case KnownOperator::ASin:
      return codegen_->CallBuiltin(ast::Builtin::ASin, {input});
    case KnownOperator::ATan:
      return codegen_->CallBuiltin(ast::Builtin::ATan, {input});
    case KnownOperator::Cos:
      return codegen_->CallBuiltin(ast::Builtin::Cos, {input});
    case KnownOperator::Cot:
      return codegen_->CallBuiltin(ast::Builtin::Cot, {input});
    case KnownOperator::Sin:
      return codegen_->CallBuiltin(ast::Builtin::Sin, {input});
    case KnownOperator::Tan:
      return codegen_->CallBuiltin(ast::Builtin::Tan, {input});
    default:
      throw NotImplementedException(fmt::format("Translation of unary operator expression type {}",
                                                KnownOperatorToString(op, true)));
  }
}

}  // namespace tpl::sql::codegen
