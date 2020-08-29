#include "sql/codegen/expression/unary_translator.h"

#include "spdlog/fmt/fmt.h"

#include "common/exception.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/consumer_context.h"

namespace tpl::sql::codegen {

UnaryTranslator::UnaryTranslator(const planner::OperatorExpression &expr,
                                 CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {
  compilation_context->Prepare(*expr.GetChild(0));
}

ast::Expr *UnaryTranslator::DeriveValue(ConsumerContext *context,
                                        const ColumnValueProvider *provider) const {
  auto input = context->DeriveValue(*GetExpression().GetChild(0), provider);

  parsing::Token::Type type;
  switch (GetExpression().GetExpressionType()) {
    case planner::ExpressionType::OPERATOR_UNARY_MINUS:
      type = parsing::Token::Type::MINUS;
      break;
    case planner::ExpressionType::OPERATOR_NOT:
      type = parsing::Token::Type::BANG;
      break;
    default:
      throw NotImplementedException(
          fmt::format("Translation of unary expression type {}",
                      planner::ExpressionTypeToString(GetExpression().GetExpressionType(), false)));
  }

  return codegen_->UnaryOp(type, input);
}

}  // namespace tpl::sql::codegen
