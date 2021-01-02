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

ast::Expression *ConjunctionTranslator::DeriveValue(ConsumerContext *context,
                                                    const ColumnValueProvider *provider) const {
  auto left_val = context->DeriveValue(*GetExpression().GetChild(0), provider);
  auto right_val = context->DeriveValue(*GetExpression().GetChild(1), provider);

  switch (auto cmp_kind = GetExpressionAs<planner::ConjunctionExpression>().GetKind(); cmp_kind) {
    case planner::ConjunctionKind::AND:
      return codegen_->BinaryOp(parsing::Token::Type::AND, left_val, right_val);
    case planner::ConjunctionKind::OR:
      return codegen_->BinaryOp(parsing::Token::Type::OR, left_val, right_val);
    default: {
      throw NotImplementedException(fmt::format("Translation of conjunction type {}",
                                                planner::ConjunctionKindToString(cmp_kind)));
    }
  }
}

}  // namespace tpl::sql::codegen
