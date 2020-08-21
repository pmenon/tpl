#include "sql/codegen/expression/comparison_translator.h"

#include "spdlog/fmt/fmt.h"

#include "common/exception.h"
#include "sql/codegen/codegen.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/consumer_context.h"

namespace tpl::sql::codegen {

ComparisonTranslator::ComparisonTranslator(const planner::ComparisonExpression &expr,
                                           CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {
  // Prepare the children.
  for (uint32_t i = 0; i < expr.GetChildrenSize(); i++) {
    compilation_context->Prepare(*expr.GetChild(i));
  }
}

ast::Expr *ComparisonTranslator::DeriveValue(ConsumerContext *context,
                                             const ColumnValueProvider *provider) const {
  auto left_val = context->DeriveValue(*GetExpression().GetChild(0), provider);
  auto right_val = context->DeriveValue(*GetExpression().GetChild(1), provider);

  switch (const auto expr_type = GetExpression().GetExpressionType(); expr_type) {
    case planner::ExpressionType::COMPARE_EQUAL:
      return codegen_->Compare(parsing::Token::Type::EQUAL_EQUAL, left_val, right_val);
    case planner::ExpressionType::COMPARE_GREATER_THAN:
      return codegen_->Compare(parsing::Token::Type::GREATER, left_val, right_val);
    case planner::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO:
      return codegen_->Compare(parsing::Token::Type::GREATER_EQUAL, left_val, right_val);
    case planner::ExpressionType::COMPARE_LESS_THAN:
      return codegen_->Compare(parsing::Token::Type::LESS, left_val, right_val);
    case planner::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO:
      return codegen_->Compare(parsing::Token::Type::LESS_EQUAL, left_val, right_val);
    case planner::ExpressionType::COMPARE_NOT_EQUAL:
      return codegen_->Compare(parsing::Token::Type::BANG_EQUAL, left_val, right_val);
    case planner::ExpressionType::COMPARE_LIKE:
      return codegen_->Like(left_val, right_val);
    case planner::ExpressionType::COMPARE_NOT_LIKE:
      return codegen_->NotLike(left_val, right_val);
    case planner::ExpressionType::COMPARE_BETWEEN: {
      auto lo = right_val, hi = context->DeriveValue(*GetExpression().GetChild(2), provider);
      return codegen_->BinaryOp(
          parsing::Token::Type::AND,
          codegen_->Compare(parsing::Token::Type::GREATER_EQUAL, left_val, lo),
          codegen_->Compare(parsing::Token::Type::LESS_EQUAL, left_val, hi));
    }
    default: {
      throw NotImplementedException(fmt::format("Translation of comparison type {}",
                                                planner::ExpressionTypeToString(expr_type, true)));
    }
  }
}

}  // namespace tpl::sql::codegen
