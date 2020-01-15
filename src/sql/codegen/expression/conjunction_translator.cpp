#include "sql/codegen/expression/conjunction_translator.h"

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

ast::Expr *ConjunctionTranslator::DeriveValue(ConsumerContext *ctx) const {
  CodeGen *codegen = GetCodeGen();
  ast::Expr *left_val = ctx->DeriveValue(*GetExpression().GetChild(0));
  ast::Expr *right_val = ctx->DeriveValue(*GetExpression().GetChild(1));

  switch (const auto expr_type = GetExpression().GetExpressionType(); expr_type) {
    case planner::ExpressionType::CONJUNCTION_AND:
      return codegen->BinaryOp(parsing::Token::Type::AND, left_val, right_val);
    case planner::ExpressionType::CONJUNCTION_OR:
      return codegen->BinaryOp(parsing::Token::Type::OR, left_val, right_val);
    default: {
      throw NotImplementedException("Translation of conjunction type {}",
                                    planner::ExpressionTypeToString(expr_type, true));
    }
  }
}

}  // namespace tpl::sql::codegen