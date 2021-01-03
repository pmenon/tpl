#include "sql/codegen/expression/like_translator.h"

#include "spdlog/fmt/fmt.h"

#include "common/exception.h"
#include "sql/codegen/codegen.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/consumer_context.h"
#include "sql/codegen/edsl/ops.h"
#include "sql/codegen/edsl/value.h"
#include "sql/codegen/edsl/value_vt.h"

namespace tpl::sql::codegen {

LikeTranslator::LikeTranslator(const planner::ComparisonExpression &expr,
                               CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {
  for (uint32_t i = 0; i < expr.GetChildrenSize(); i++) {
    compilation_context->Prepare(*expr.GetChild(i));
  }
}

edsl::ValueVT LikeTranslator::DeriveValue(ConsumerContext *context,
                                          const ColumnValueProvider *provider) const {
  edsl::Value<ast::x::StringVal> left_val(context->DeriveValue(*GetChild(0), provider));
  edsl::Value<ast::x::StringVal> right_val(context->DeriveValue(*GetChild(1), provider));

  switch (const auto cmp_kind = GetComparisonExpression().GetKind(); cmp_kind) {
    case planner::ComparisonKind::LIKE:
      return edsl::Like(left_val, right_val);
    case planner::ComparisonKind::NOT_LIKE:
      return edsl::NotLike(left_val, right_val);
    default: {
      throw NotImplementedException(fmt::format("Invalid LIKE comparison type: '{}'",
                                                planner::ComparisonKindToString(cmp_kind, true)));
    }
  }
}

}  // namespace tpl::sql::codegen
