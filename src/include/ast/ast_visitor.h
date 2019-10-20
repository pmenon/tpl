#pragma once

#include "ast/ast.h"

namespace tpl::ast {

/**
 * Base class for AST node visitors. Uses the Curiously Recurring Template Pattern (CRTP) to avoid
 * overhead of virtual function dispatch, and because we keep a static, macro-based list of all
 * possible AST nodes.
 *
 * Derived classes parameterize AstVisitor with itself, e.g.:
 *
 * @code
 * class Derived : public AstVisitor<Derived> {
 *   ..
 * }
 * @endcode
 *
 * All AST node visitations will get forwarded to the derived class if they are implemented, and
 * fallback to this base class otherwise. Fallback methods walk up the node hierarchy.
 *
 * To easily define visitors for all nodes, use the AST_NODES() macro providing a function generator
 * macro as the argument.
 */
template <typename Subclass, typename RetType = void>
class AstVisitor {
 public:
  // Dispatch to a given type
#define DISPATCH(Type) return this->Impl()->Visit##Type(static_cast<Type *>(node));

  /**
   * Begin AST traversal at the given node.
   * @param node The node to begin traversal at.
   * @return Template-specific return type.
   */
  RetType Visit(AstNode *node) {
#define GENERATE_VISIT_CASE(NodeKind) \
  case AstNode::Kind::NodeKind:       \
    DISPATCH(NodeKind);

    // Main dispatch switch
    switch (node->kind()) {
      AST_NODES(GENERATE_VISIT_CASE)
      default:
        UNREACHABLE("Impossible node type");
    }

#undef GENERATE_VISIT_CASE
  }

  /**
   * No-op base implementation for all declaration nodes.
   * @param decl The declaration node.
   * @return No-arg constructed return.
   */
  RetType VisitDecl(UNUSED Decl *decl) { return RetType(); }

  /**
   * No-op base implementation for all statement nodes.
   * @param stmt The statement node.
   * @return No-arg constructed return.
   */
  RetType VisitStmt(UNUSED Stmt *stmt) { return RetType(); }

  /**
   * No-op base implementation for all expression nodes.
   * @param expr The expression node.
   * @return No-arg constructed return.
   */
  RetType VisitExpr(UNUSED Expr *expr) { return RetType(); }

  // Generate default visitors for declaration nodes that dispatch to base Decl
#define T(DeclType) \
  RetType Visit##DeclType(DeclType *node) { DISPATCH(Decl); }
  DECLARATION_NODES(T)
#undef T

  // Generate default visitors for statement nodes that dispatch to base Stmt
#define T(StmtType) \
  RetType Visit##StmtType(StmtType *node) { DISPATCH(Stmt); }
  STATEMENT_NODES(T)
#undef T

  // Generate default visitors for expression nodes that dispatch to base Expr
#define T(ExprType) \
  RetType Visit##ExprType(ExprType *node) { DISPATCH(Expr); }
  EXPRESSION_NODES(T)
#undef T

#undef DISPATCH

 protected:
  Subclass *Impl() { return static_cast<Subclass *>(this); }
};

}  // namespace tpl::ast
