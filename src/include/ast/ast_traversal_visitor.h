#pragma once

#include "ast/ast_visitor.h"
#include "common/common.h"
#include "common/macros.h"

namespace tpl::ast {

/**
 * A visitor that fully traverses the entire AST. It invokes AstTraversalVisitor::VisitNode()
 * on each node before proceeding with its subtree. Sub-classes may override
 * AstTraversalVisitor::VisitNode() (a dummy implementation exists here), or override the specific
 * Visit*() methods for the node they are interested in.
 *
 * Usage:
 * @code
 * // The ForStmtVisitor class will find ALL for-statement nodes in the input AST
 * class ForStmtVisitor : public AstTraversalVisitor<ForStmtVisitor> {
 *  public:
 *   ForStmtVisitor(ast::AstNode *root) : AstTraversalVisitor<ForStmtVisitor>(root) {}
 *
 *   void VisitForStmt(ast::ForStmt *stmt) {
 *     // Process stmt
 *   }
 * }
 * @endcode
 */
template <typename Subclass>
class AstTraversalVisitor : public AstVisitor<Subclass> {
 public:
  /**
   * Construct a visitor over the AST rooted at @em root.
   * @param root The root of the AST tree to begin visiting.
   */
  explicit AstTraversalVisitor(AstNode *root) : root_(root) {}

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(AstTraversalVisitor);

  /**
   * Run the traversal.
   */
  void Run() {
    TPL_ASSERT(root_ != nullptr, "Cannot run traversal on NULL tree");
    AstVisitor<Subclass>::Visit(root_);
  }

  // Declare all node visit methods here
#define DECLARE_VISIT_METHOD(type) void Visit##type(ast::type *node);
  AST_NODES(DECLARE_VISIT_METHOD)
#undef DECLARE_VISIT_METHOD

 protected:
  // Should this iterator visit the given node? This method can be implemented
  // in subclasses to skip some visiting some nodes. By default, we visit all
  // nodes.
  bool VisitNode(UNUSED AstNode *node) { return true; }

 private:
  AstNode *root_;
};

// ---------------------------------------------------------
//
// Implementation below
//
// ---------------------------------------------------------

#define PROCESS_NODE(node)                \
  do {                                    \
    if (!this->Impl()->VisitNode(node)) { \
      return;                             \
    }                                     \
  } while (false)

#define RECURSE(call) this->Impl()->call

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitBadExpr(BadExpr *node) {
  PROCESS_NODE(node);
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitFieldDeclaration(FieldDeclaration *node) {
  PROCESS_NODE(node);
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitFunctionDeclaration(FunctionDeclaration *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->GetFunctionLiteral()));
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitIdentifierExpr(IdentifierExpr *node) {
  PROCESS_NODE(node);
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitArrayTypeRepr(ArrayTypeRepr *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->GetElementType()));
  if (node->HasLength()) {
    RECURSE(Visit(node->GetLength()));
  }
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitBlockStmt(BlockStmt *node) {
  PROCESS_NODE(node);
  for (auto *stmt : node->GetStatements()) {
    RECURSE(Visit(stmt));
  }
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitStructDeclaration(StructDeclaration *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->GetTypeRepr()));
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitVariableDeclaration(VariableDeclaration *node) {
  PROCESS_NODE(node);
  if (node->HasDeclaredType()) {
    RECURSE(Visit(node->GetTypeRepr()));
  }
  if (node->HasInitialValue()) {
    RECURSE(Visit(node->GetInitialValue()));
  }
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitUnaryOpExpr(UnaryOpExpr *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->GetInput()));
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitReturnStmt(ReturnStmt *node) {
  PROCESS_NODE(node);
  if (node->GetReturnValue() != nullptr) {
    RECURSE(Visit(node->GetReturnValue()));
  }
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitCallExpr(CallExpr *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->GetFunction()));
  for (auto *arg : node->GetArguments()) {
    RECURSE(Visit(arg));
  }
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitImplicitCastExpr(ImplicitCastExpr *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->GetInput()));
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitAssignmentStmt(AssignmentStmt *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->GetDestination()));
  RECURSE(Visit(node->GetSource()));
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitFile(File *node) {
  PROCESS_NODE(node);
  for (auto *decl : node->GetDeclarations()) {
    RECURSE(Visit(decl));
  }
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitFunctionLiteralExpr(FunctionLiteralExpr *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->GetTypeRepr()));
  RECURSE(Visit(node->GetBody()));
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitForStmt(ForStmt *node) {
  PROCESS_NODE(node);
  if (node->GetInit() != nullptr) {
    RECURSE(Visit(node->GetInit()));
  }
  if (node->GetCondition() != nullptr) {
    RECURSE(Visit(node->GetCondition()));
  }
  if (node->GetNext() != nullptr) {
    RECURSE(Visit(node->GetNext()));
  }
  RECURSE(Visit(node->GetBody()));
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitForInStmt(ForInStmt *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->Target()));
  RECURSE(Visit(node->Iterable()));
  RECURSE(Visit(node->GetBody()));
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitBinaryOpExpr(BinaryOpExpr *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->GetLeft()));
  RECURSE(Visit(node->GetRight()));
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitMapTypeRepr(MapTypeRepr *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->GetKeyType()));
  RECURSE(Visit(node->GetValueType()));
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitLiteralExpr(LiteralExpr *node) {
  PROCESS_NODE(node);
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitStructTypeRepr(StructTypeRepr *node) {
  PROCESS_NODE(node);
  for (auto *field : node->GetFields()) {
    RECURSE(Visit(field));
  }
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitDeclStmt(DeclStmt *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->GetDeclaration()));
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitMemberExpr(MemberExpr *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->GetObject()));
  RECURSE(Visit(node->GetMember()));
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitPointerTypeRepr(PointerTypeRepr *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->GetBase()));
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitComparisonOpExpr(ComparisonOpExpr *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->GetLeft()));
  RECURSE(Visit(node->GetRight()));
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitIfStmt(IfStmt *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->GetCondition()));
  RECURSE(Visit(node->GetThenStmt()));
  if (node->HasElseStmt()) {
    RECURSE(Visit(node->GetElseStmt()));
  }
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitExpressionStmt(ExpressionStmt *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->GetExpression()));
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitIndexExpr(IndexExpr *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->GetObject()));
  RECURSE(Visit(node->GetIndex()));
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitFunctionTypeRepr(FunctionTypeRepr *node) {
  PROCESS_NODE(node);
  for (auto *param : node->GetParameters()) {
    RECURSE(Visit(param));
  }
  RECURSE(Visit(node->GetReturnType()));
}

}  // namespace tpl::ast
