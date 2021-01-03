#include "sql/codegen/expression/comparison_translator.h"

#include "spdlog/fmt/fmt.h"

#include "common/exception.h"
#include "sql/codegen/codegen.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/consumer_context.h"
#include "sql/codegen/edsl/value_vt.h"

namespace tpl::sql::codegen {

ComparisonTranslator::ComparisonTranslator(const planner::ComparisonExpression &expr,
                                           CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {
  for (uint32_t i = 0; i < expr.GetChildrenSize(); i++) {
    compilation_context->Prepare(*expr.GetChild(i));
  }
}

edsl::ValueVT ComparisonTranslator::DeriveValue(ConsumerContext *context,
                                                const ColumnValueProvider *provider) const {
  auto left_val = context->DeriveValue(*GetChild(0), provider);
  auto right_val = context->DeriveValue(*GetChild(1), provider);

  switch (auto cmp_kind = GetExpressionAs<planner::ComparisonExpression>().GetKind(); cmp_kind) {
    case planner::ComparisonKind::EQUAL:
      return edsl::ComparisonOp(parsing::Token::Type::EQUAL_EQUAL, left_val, right_val);
    case planner::ComparisonKind::GREATER_THAN:
      return edsl::ComparisonOp(parsing::Token::Type::GREATER, left_val, right_val);
    case planner::ComparisonKind::GREATER_THAN_OR_EQUAL_TO:
      return edsl::ComparisonOp(parsing::Token::Type::GREATER_EQUAL, left_val, right_val);
    case planner::ComparisonKind::LESS_THAN:
      return edsl::ComparisonOp(parsing::Token::Type::LESS, left_val, right_val);
    case planner::ComparisonKind::LESS_THAN_OR_EQUAL_TO:
      return edsl::ComparisonOp(parsing::Token::Type::LESS_EQUAL, left_val, right_val);
    case planner::ComparisonKind::NOT_EQUAL:
      return edsl::ComparisonOp(parsing::Token::Type::BANG_EQUAL, left_val, right_val);
#if 0
      {
      auto lo = right_val, hi = context->DeriveValue(*GetExpression().GetChild(2), provider);
      return codegen_->BinaryOp(
          parsing::Token::Type::AND,
          codegen_->Compare(parsing::Token::Type::GREATER_EQUAL, left_val, lo),
          codegen_->Compare(parsing::Token::Type::LESS_EQUAL, left_val, hi));
    }
#endif
    default: {
      throw NotImplementedException(fmt::format("Translation of comparison type {}",
                                                planner::ComparisonKindToString(cmp_kind, true)));
    }
  }
}

}  // namespace tpl::sql::codegen
