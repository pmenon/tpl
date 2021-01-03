#include "sql/codegen/expression/conjunction_translator.h"

#include "spdlog/fmt/fmt.h"

#include "common/exception.h"
#include "sql/codegen/codegen.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/consumer_context.h"
#include "sql/codegen/edsl/boolean_ops.h"
#include "sql/codegen/edsl/value.h"
#include "sql/codegen/edsl/value_vt.h"

namespace tpl::sql::codegen {

ConjunctionTranslator::ConjunctionTranslator(const planner::ConjunctionExpression &expr,
                                             CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {
  // Prepare the left and right expression subtrees for translation.
  compilation_context->Prepare(*expr.GetChild(0));
  compilation_context->Prepare(*expr.GetChild(1));
}

edsl::ValueVT ConjunctionTranslator::DeriveValue(ConsumerContext *context,
                                                 const ColumnValueProvider *provider) const {
  edsl::Value<bool> left_val = context->DeriveValue(*GetChild(0), provider);
  edsl::Value<bool> right_val = context->DeriveValue(*GetChild(1), provider);

  switch (auto cmp_kind = GetExpressionAs<planner::ConjunctionExpression>().GetKind(); cmp_kind) {
    case planner::ConjunctionKind::AND:
      return left_val && right_val;
    case planner::ConjunctionKind::OR:
      return left_val || right_val;
    default: {
      throw NotImplementedException(fmt::format("Translation of conjunction type {}",
                                                planner::ConjunctionKindToString(cmp_kind)));
    }
  }
}

}  // namespace tpl::sql::codegen
