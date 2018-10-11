#include "ast/ast.h"

namespace tpl::ast {

FunctionDecl::FunctionDecl(const SourcePosition &pos, Identifier name,
                           FunctionLitExpr *fun)
    : Decl(Kind::FunctionDecl, pos, name, fun->type_repr()), fun_(fun) {}

StructDecl::StructDecl(const SourcePosition &pos, Identifier name,
                       StructTypeRepr *type_repr)
    : Decl(Kind::StructDecl, pos, name, type_repr) {}

ExpressionStmt::ExpressionStmt(Expr *expr)
    : Stmt(Kind::ExpressionStmt, expr->position()), expr_(expr) {}

FunctionLitExpr::FunctionLitExpr(FunctionTypeRepr *type_repr, BlockStmt *body)
    : Expr(Kind::FunctionLitExpr, type_repr->position()),
      type_repr_(type_repr),
      body_(body) {}

}  // namespace tpl::ast