#include "sql/codegen/expression/case_translator.h"

#include "sql/codegen/compilation_context.h"
#include "sql/codegen/consumer_context.h"
#include "sql/codegen/edsl/ops.h"
#include "sql/codegen/edsl/value.h"
#include "sql/codegen/edsl/value_vt.h"
#include "sql/codegen/function_builder.h"
#include "sql/codegen/if.h"
#include "sql/planner/expressions/case_expression.h"

namespace tpl::sql::codegen {

CaseTranslator::CaseTranslator(const planner::CaseExpression &expr,
                               CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {
  // Prepare each clause.
  for (std::size_t i = 0; i < expr.GetWhenClauseSize(); i++) {
    compilation_context->Prepare(*expr.GetWhenClauseCondition(i));
    compilation_context->Prepare(*expr.GetWhenClauseResult(i));
  }
  // Prepare the default if one exists.
  if (const auto default_val = expr.GetDefaultClause()) {
    compilation_context->Prepare(*default_val);
  }
}

void CaseTranslator::GenerateCases(const edsl::VariableVT &ret, const std::size_t clause_idx,
                                   ConsumerContext *context,
                                   const ColumnValueProvider *provider) const {
  const auto &expr = GetExpressionAs<planner::CaseExpression>();

  // The function.
  FunctionBuilder *function = codegen_->GetCurrentFunction();

  // Base case.
  if (clause_idx == expr.GetWhenClauseSize()) {
    if (const auto default_val = expr.GetDefaultClause()) {
      function->Append(edsl::Assign(ret, context->DeriveValue(*default_val, provider)));
    } else {
      function->Append(edsl::InitSqlNull(ret));
    }
    return;
  }

  // if (when_clause) {
  //   case_result = when_clause_result()
  // }
  auto condition_gen = context->DeriveValue(*expr.GetWhenClauseCondition(clause_idx), provider);
  auto condition = condition_gen.IsSQLType()
                       ? edsl::ForceTruth(condition_gen.As<ast::x::BooleanVal>())
                       : condition_gen.As<bool>();
  If check_condition(function, condition);
  {  // Case-match.
    auto when_result = context->DeriveValue(*expr.GetWhenClauseResult(clause_idx), provider);
    function->Append(edsl::Assign(ret, when_result));
  }
  check_condition.Else();
  {  // Recurse.
    GenerateCases(ret, clause_idx + 1, context, provider);
  }
  check_condition.EndIf();
}

edsl::ValueVT CaseTranslator::DeriveValue(ConsumerContext *context,
                                          const ColumnValueProvider *provider) const {
  FunctionBuilder *function = codegen_->GetCurrentFunction();

  // var case_result: TYPE
  const auto &ret_type = GetExpression().GetReturnValueType();
  edsl::VariableVT ret(codegen_, "ret", codegen_->GetTPLType(ret_type.GetTypeId()));
  function->Append(edsl::Declare(ret));

  // Generate all clauses.
  GenerateCases(ret, 0, context, provider);

  // Done.
  return std::move(ret);
}

}  // namespace tpl::sql::codegen
