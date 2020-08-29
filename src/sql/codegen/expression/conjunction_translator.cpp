#include "sql/codegen/expression/conjunction_translator.h"

#include "spdlog/fmt/fmt.h"

#include "common/exception.h"
#include "sql/codegen/codegen.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/consumer_context.h"

namespace tpl::sql::codegen {

ConjunctionTranslator::ConjunctionTranslator(const planner::ConjunctionExpression &expr,
                                             CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {
  // Prepare the left and right expression subtrees for translation.
  compilation_context->Prepare(*expr.GetChild(0));
  compilation_context->Prepare(*expr.GetChild(1));
}

ast::Expr *ConjunctionTranslator::DeriveValue(ConsumerContext *context,
                                              const ColumnValueProvider *provider) const {
  auto left_val = context->DeriveValue(*GetExpression().GetChild(0), provider);
  auto right_val = context->DeriveValue(*GetExpression().GetChild(1), provider);

  switch (const auto expr_type = GetExpression().GetExpressionType(); expr_type) {
    case planner::ExpressionType::CONJUNCTION_AND:
      return codegen_->BinaryOp(parsing::Token::Type::AND, left_val, right_val);
    case planner::ExpressionType::CONJUNCTION_OR:
      return codegen_->BinaryOp(parsing::Token::Type::OR, left_val, right_val);
    default: {
      throw NotImplementedException(fmt::format("Translation of conjunction type {}",
                                                planner::ExpressionTypeToString(expr_type, true)));
    }
  }
}

}  // namespace tpl::sql::codegen
