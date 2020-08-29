#include "sql/codegen/expression/null_check_translator.h"

#include "spdlog/fmt/fmt.h"

#include "common/exception.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/consumer_context.h"

namespace tpl::sql::codegen {

NullCheckTranslator::NullCheckTranslator(const planner::OperatorExpression &expr,
                                         CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {
  compilation_context->Prepare(*expr.GetChild(0));
}

ast::Expr *NullCheckTranslator::DeriveValue(ConsumerContext *context,
                                            const ColumnValueProvider *provider) const {
  auto input = context->DeriveValue(*GetExpression().GetChild(0), provider);
  switch (auto type = GetExpression().GetExpressionType()) {
    case planner::ExpressionType::OPERATOR_IS_NULL:
      return codegen_->CallBuiltin(ast::Builtin::IsValNull, {input});
    case planner::ExpressionType::OPERATOR_IS_NOT_NULL:
      return codegen_->UnaryOp(parsing::Token::Type::BANG,
                               codegen_->CallBuiltin(ast::Builtin::IsValNull, {input}));
    default:
      throw NotImplementedException(
          fmt::format("operator expression type {}", planner::ExpressionTypeToString(type, false)));
  }
}

}  // namespace tpl::sql::codegen
