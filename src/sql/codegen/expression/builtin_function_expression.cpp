#include "sql/planner/expressions/builtin_function_expression.h"
#include "sql/codegen/expression/builtin_function_translator.h"

#include "common/exception.h"
#include "sql/codegen/codegen.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/work_context.h"

namespace tpl::sql::codegen {

BuiltinFunctionTranslator::BuiltinFunctionTranslator(const planner::BuiltinFunctionExpression &expr,
                                                     CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {
  for (const auto &child : expr.GetChildren()) {
    compilation_context->Prepare(*child);
  }
}

ast::Expr *BuiltinFunctionTranslator::DeriveValue(WorkContext *ctx,
                                                  const ColumnValueProvider *provider) const {
  auto codegen = GetCodeGen();
  std::vector<ast::Expr *> args;
  auto func_expr = GetExpressionAs<planner::BuiltinFunctionExpression>();
  for (const auto &child : func_expr.GetChildren()) {
    args.emplace_back(ctx->DeriveValue(*child, provider));
  }
  return codegen->CallBuiltin(func_expr.GetBuiltin(), args);
}
}  // namespace tpl::sql::codegen
