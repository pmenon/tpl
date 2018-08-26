#include "ast/ast.h"

namespace tpl::ast {

FunctionTypeRepr *FunctionDecl::type_repr() { return fun_->type_repr(); }

ExpressionStmt::ExpressionStmt(Expression *expr)
    : Stmt(Kind::ExpressionStmt, expr->position()), expr_(expr) {}

FunctionLitExpr::FunctionLitExpr(FunctionTypeRepr *type_repr, BlockStmt *body)
    : Expression(Kind::FunctionLitExpr, type_repr->position()),
      type_repr_(type_repr),
      body_(body) {}

}  // namespace tpl::ast