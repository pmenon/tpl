#include "sql/codegen/expression/builtin_function_translator.h"

#include "sql/codegen/codegen.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/consumer_context.h"

namespace tpl::sql::codegen {

BuiltinFunctionTranslator::BuiltinFunctionTranslator(const planner::BuiltinFunctionExpression &expr,
                                                     CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {
  for (const auto &child : expr.GetChildren()) {
    compilation_context->Prepare(*child);
  }
}

ast::Expr *BuiltinFunctionTranslator::DeriveValue(ConsumerContext *context,
                                                  const ColumnValueProvider *provider) const {
  auto func_expr = GetExpressionAs<planner::BuiltinFunctionExpression>();

  // Evaluate the arguments to the function.
  std::vector<ast::Expr *> args;
  args.reserve(func_expr.GetChildrenSize());
  for (const auto &child : func_expr.GetChildren()) {
    args.emplace_back(context->DeriveValue(*child, provider));
  }

  // Issue the function.
  return codegen_->CallBuiltin(func_expr.GetBuiltin(), args);
}

}  // namespace tpl::sql::codegen
