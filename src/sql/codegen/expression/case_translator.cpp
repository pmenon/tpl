#include "sql/codegen/expression/case_translator.h"

#include "sql/codegen/compilation_context.h"
#include "sql/codegen/consumer_context.h"
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

void CaseTranslator::GenerateCases(const ast::Identifier ret, const std::size_t clause_idx,
                                   ConsumerContext *context,
                                   const ColumnValueProvider *provider) const {
  const auto &expr = GetExpressionAs<planner::CaseExpression>();

  // The function.
  FunctionBuilder *function = codegen_->GetCurrentFunction();

  // Base case.
  if (clause_idx == expr.GetWhenClauseSize()) {
    if (const auto default_val = expr.GetDefaultClause()) {
      ast::Expr *default_result = context->DeriveValue(*default_val, provider);
      function->Append(codegen_->Assign(codegen_->MakeExpr(ret), default_result));
    } else {
      function->Append(codegen_->InitSqlNull(codegen_->MakeExpr(ret)));
    }
    return;
  }

  // if (when_clause) {
  //   case_result = when_clause_result()
  // }
  If condition(function, context->DeriveValue(*expr.GetWhenClauseCondition(clause_idx), provider));
  {
    ast::Expr *when_result = context->DeriveValue(*expr.GetWhenClauseResult(clause_idx), provider);
    function->Append(codegen_->Assign(codegen_->MakeExpr(ret), when_result));
  }
  condition.Else();
  {
    // Recurse.
    GenerateCases(ret, clause_idx + 1, context, provider);
  }
  condition.EndIf();
}

ast::Expr *CaseTranslator::DeriveValue(ConsumerContext *context,
                                       const ColumnValueProvider *provider) const {
  // var case_result: TYPE
  ast::Identifier ret = codegen_->MakeFreshIdentifier("case_result");
  codegen_->GetCurrentFunction()->Append(
      codegen_->DeclareVarNoInit(ret, codegen_->TplType(GetExpression().GetReturnValueType())));

  // Generate all clauses.
  GenerateCases(ret, 0, context, provider);

  // Done.
  return codegen_->MakeExpr(ret);
}

}  // namespace tpl::sql::codegen
