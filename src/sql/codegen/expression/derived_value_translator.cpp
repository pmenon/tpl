#include "sql/codegen/expression/derived_value_translator.h"

#include "sql/codegen/consumer_context.h"
#include "sql/codegen/operators/operator_translator.h"
#include "sql/planner/plannodes/abstract_plan_node.h"
#include "sql/planner/plannodes/output_schema.h"

namespace tpl::sql::codegen {

DerivedValueTranslator::DerivedValueTranslator(const planner::DerivedValueExpression &expr,
                                               CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {}

ast::Expr *DerivedValueTranslator::DeriveValue(ConsumerContext *ctx) const {
  const auto &derived_expr = GetExpressionAs<planner::DerivedValueExpression>();
  const auto child_plan = ctx->CurrentOp()->GetPlan().GetChild(derived_expr.GetTupleIdx());
  const auto child_col = child_plan->GetOutputSchema()->GetColumn(derived_expr.GetValueIdx());
  return ctx->DeriveValue(*child_col.GetExpr());
}

}  // namespace tpl::sql::codegen
