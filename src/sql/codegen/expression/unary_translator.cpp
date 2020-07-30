#include "sql/codegen/expression/unary_translator.h"

#include "common/exception.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/consumer_context.h"

namespace tpl::sql::codegen {

UnaryTranslator::UnaryTranslator(const planner::OperatorExpression &expr,
                                 CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {
  compilation_context->Prepare(*expr.GetChild(0));
}

ast::Expr *UnaryTranslator::DeriveValue(ConsumerContext *ctx,
                                        const ColumnValueProvider *provider) const {
  auto codegen = GetCodeGen();
  auto input = ctx->DeriveValue(*GetExpression().GetChild(0), provider);

  parsing::Token::Type type;
  switch (GetExpression().GetExpressionType()) {
    case planner::ExpressionType::OPERATOR_UNARY_MINUS:
      type = parsing::Token::Type::MINUS;
      break;
    case planner::ExpressionType::OPERATOR_NOT:
      type = parsing::Token::Type::BANG;
      break;
    default:
      UNREACHABLE("Unsupported expression");
  }
  return codegen->UnaryOp(type, input);
}

}  // namespace tpl::sql::codegen
