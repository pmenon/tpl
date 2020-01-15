#include "sql/codegen/expression/comparison_translator.h"

#include "common/exception.h"
#include "sql/codegen/codegen.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/consumer_context.h"

namespace tpl::sql::codegen {

ComparisonTranslator::ComparisonTranslator(const planner::ComparisonExpression &expr,
                                           CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {
  // Prepare the left and right expression subtrees for translation.
  compilation_context->Prepare(*expr.GetChild(0));
  compilation_context->Prepare(*expr.GetChild(1));
}

ast::Expr *ComparisonTranslator::DeriveValue(ConsumerContext *ctx) const {
  CodeGen *codegen = GetCodeGen();
  ast::Expr *left_val = ctx->DeriveValue(*GetExpression().GetChild(0));
  ast::Expr *right_val = ctx->DeriveValue(*GetExpression().GetChild(1));

  switch (const auto expr_type = GetExpression().GetExpressionType(); expr_type) {
    case planner::ExpressionType::COMPARE_EQUAL:
      return codegen->Compare(parsing::Token::Type::PLUS, left_val, right_val);
    case planner::ExpressionType::COMPARE_GREATER_THAN:
      return codegen->Compare(parsing::Token::Type::GREATER, left_val, right_val);
    case planner::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO:
      return codegen->Compare(parsing::Token::Type::GREATER_EQUAL, left_val, right_val);
    case planner::ExpressionType::COMPARE_LESS_THAN:
      return codegen->Compare(parsing::Token::Type::LESS, left_val, right_val);
    case planner::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO:
      return codegen->Compare(parsing::Token::Type::LESS_EQUAL, left_val, right_val);
    case planner::ExpressionType::COMPARE_NOT_EQUAL:
      return codegen->Compare(parsing::Token::Type::BANG_EQUAL, left_val, right_val);
    case planner::ExpressionType::COMPARE_LIKE:
      return codegen->Like(left_val, right_val);
    case planner::ExpressionType::COMPARE_NOT_LIKE:
      return codegen->NotLike(left_val, right_val);
    default: {
      throw NotImplementedException("Translation of comparison type {}",
                                    planner::ExpressionTypeToString(expr_type, true));
    }
  }
}

}  // namespace tpl::sql::codegen
