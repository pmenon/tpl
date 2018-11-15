#include "ast/ast.h"

#include "ast/type.h"

namespace tpl::ast {

FunctionDecl::FunctionDecl(const SourcePosition &pos, Identifier name,
                           FunctionLitExpr *func)
    : Decl(Kind::FunctionDecl, pos, name, func->type_repr()), func_(func) {}

StructDecl::StructDecl(const SourcePosition &pos, Identifier name,
                       StructTypeRepr *type_repr)
    : Decl(Kind::StructDecl, pos, name, type_repr) {}

ExpressionStmt::ExpressionStmt(Expr *expr)
    : Stmt(Kind::ExpressionStmt, expr->position()), expr_(expr) {}

FunctionLitExpr::FunctionLitExpr(FunctionTypeRepr *type_repr, BlockStmt *body)
    : Expr(Kind::FunctionLitExpr, type_repr->position()),
      type_repr_(type_repr),
      body_(body) {}

bool IndexExpr::IsArrayAccess() const {
  TPL_ASSERT(object() != nullptr, "Object cannot be NULL");
  TPL_ASSERT(object() != nullptr,
             "Cannot determine object type before type checking!");
  return object()->type()->IsArrayType();
}

bool IndexExpr::IsMapAccess() const {
  TPL_ASSERT(object() != nullptr, "Object cannot be NULL");
  TPL_ASSERT(object() != nullptr,
             "Cannot determine object type before type checking!");
  return object()->type()->IsMapType();
}

bool MemberExpr::IsSugaredArrow() const {
  TPL_ASSERT(object()->type() != nullptr,
             "Cannot determine sugared-arrow before type checking!");
  return object()->type()->IsPointerType();
}

}  // namespace tpl::ast