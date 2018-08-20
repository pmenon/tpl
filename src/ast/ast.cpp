#include "ast/ast.h"

namespace tpl::ast {

FunctionTypeRepr *FunctionDeclaration::type_repr() { return fun_->type_repr(); }

ExpressionStatement::ExpressionStatement(Expression *expr)
    : Statement(Kind::ExpressionStatement, expr->position()), expr_(expr) {}

FunctionLiteralExpression::FunctionLiteralExpression(
    FunctionTypeRepr *type_repr, BlockStatement *body)
    : Expression(Kind::FunctionLiteralExpression, type_repr->position()),
      type_repr_(type_repr),
      body_(body) {}

}  // namespace tpl::ast