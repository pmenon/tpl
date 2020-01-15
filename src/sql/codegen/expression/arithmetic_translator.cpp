#include "sql/codegen/expression/arithmetic_translator.h"

#include "common/exception.h"
#include "sql/codegen/codegen.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/consumer_context.h"

namespace tpl::sql::codegen {

ArithmeticTranslator::ArithmeticTranslator(const planner::OperatorExpression &expr,
                                           CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {
  // Prepare the left and right expression subtrees for translation.
  compilation_context->Prepare(*expr.GetChild(0));
  compilation_context->Prepare(*expr.GetChild(1));
}

ast::Expr *ArithmeticTranslator::DeriveValue(ConsumerContext *ctx) const {
  CodeGen * codegen = GetCodeGen();
  ast::Expr * left_val = ctx->DeriveValue(*GetExpression().GetChild(0));
  ast::Expr * right_val = ctx->DeriveValue(*GetExpression().GetChild(1));

  switch (auto expr_type = GetExpression().GetExpressionType(); expr_type) {
    case planner::ExpressionType::OPERATOR_PLUS:
      return codegen->BinaryOp(parsing::Token::Type::PLUS, left_val, right_val);
    case planner::ExpressionType::OPERATOR_MINUS:
      return codegen->BinaryOp(parsing::Token::Type::MINUS, left_val, right_val);
    case planner::ExpressionType::OPERATOR_MULTIPLY:
      return codegen->BinaryOp(parsing::Token::Type::STAR, left_val, right_val);
    case planner::ExpressionType::OPERATOR_DIVIDE:
      return codegen->BinaryOp(parsing::Token::Type::SLASH, left_val, right_val);
    case planner::ExpressionType::OPERATOR_MOD:
      return codegen->BinaryOp(parsing::Token::Type::PERCENT, left_val, right_val);
    default: {
      throw NotImplementedException("Translation of arithmetic type {}",
                                    planner::ExpressionTypeToString(expr_type, true));
    }
  }
}

}  // namespace tpl::sql::codegen
