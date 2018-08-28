#pragma once

#include "ast/ast.h"

namespace tpl::ast {

/**
 * Base class for AST node visitors. Implemented using the Curiously Recurring
 * Template Pattern (CRTP) to avoid overhead of virtual function dispatch, and
 * because we keep a static, macro-based list of all possible AST nodes.
 *
 * Derived classes parameterize AstVisitor with itself, e.g.:
 *
 * class Derived : public AstVisitor<Derived> {
 *   ..
 * }
 *
 * All AST node visitations will get forwarded to the derived class.
 */
template <typename Impl, typename RetType = void>
class AstVisitor {
 public:
#define DISPATCH(Type) \
  return static_cast<Impl *>(this)->Visit##Type(static_cast<Type *>(node));

  RetType Visit(AstNode *node) {
    switch (node->kind()) {
      default: { llvm_unreachable("Impossible node type"); }
#define T(kind)               \
  case AstNode::Kind::kind: { \
    DISPATCH(kind)            \
  }
        AST_NODES(T)
#undef T
    }
  }

  RetType VisitDecl(UNUSED Decl *decl) { return RetType(); }
  RetType VisitStmt(UNUSED Stmt *stmt) { return RetType(); }
  RetType VisitExpr(UNUSED Expr *expr) { return RetType(); }

#define T(DeclType) \
  RetType Visit##DeclType(DeclType *node) { DISPATCH(Decl); }
  DECLARATION_NODES(T)
#undef T

#define T(StmtType) \
  RetType Visit##StmtType(StmtType *node) { DISPATCH(Stmt); }
  STATEMENT_NODES(T)
#undef T

#define T(ExprType) \
  RetType Visit##ExprType(ExprType *node) { DISPATCH(Expr); }
  EXPRESSION_NODES(T)
#undef T

#undef DISPATCH
};

}  // namespace tpl::ast