#include "sql/codegen/expression/derived_value_translator.h"

#include "sql/codegen/consumer_context.h"
#include "sql/codegen/operators/operator_translator.h"
#include "sql/planner/plannodes/abstract_plan_node.h"
#include "sql/planner/plannodes/output_schema.h"

namespace tpl::sql::codegen {

DerivedValueTranslator::DerivedValueTranslator(const planner::DerivedValueExpression &expr,
                                               CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {}

ast::Expression *DerivedValueTranslator::DeriveValue(ConsumerContext *context,
                                                     const ColumnValueProvider *provider) const {
  const auto &derived_expr = GetExpressionAs<planner::DerivedValueExpression>();
  return provider->GetChildOutput(context, derived_expr.GetTupleIdx(), derived_expr.GetValueIdx());
}

}  // namespace tpl::sql::codegen
