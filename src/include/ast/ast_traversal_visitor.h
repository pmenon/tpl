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
 * // The ForStatementVisitor class will find ALL for-statement nodes in the input AST
 * class ForStatementVisitor : public AstTraversalVisitor<ForStatementVisitor> {
 *  public:
 *   ForStatementVisitor(ast::AstNode *root) : AstTraversalVisitor<ForStatementVisitor>(root) {}
 *
 *   void VisitForStatement(ast::ForStatement *stmt) {
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
inline void AstTraversalVisitor<Subclass>::VisitBadExpression(BadExpression *node) {
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
inline void AstTraversalVisitor<Subclass>::VisitIdentifierExpression(IdentifierExpression *node) {
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
inline void AstTraversalVisitor<Subclass>::VisitBlockStatement(BlockStatement *node) {
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
inline void AstTraversalVisitor<Subclass>::VisitUnaryOpExpression(UnaryOpExpression *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->GetInput()));
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitReturnStatement(ReturnStatement *node) {
  PROCESS_NODE(node);
  if (node->GetReturnValue() != nullptr) {
    RECURSE(Visit(node->GetReturnValue()));
  }
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitCallExpression(CallExpression *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->GetFunction()));
  for (auto *arg : node->GetArguments()) {
    RECURSE(Visit(arg));
  }
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitImplicitCastExpression(
    ImplicitCastExpression *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->GetInput()));
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitAssignmentStatement(AssignmentStatement *node) {
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
inline void AstTraversalVisitor<Subclass>::VisitFunctionLiteralExpression(
    FunctionLiteralExpression *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->GetTypeRepr()));
  RECURSE(Visit(node->GetBody()));
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitForStatement(ForStatement *node) {
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
inline void AstTraversalVisitor<Subclass>::VisitForInStatement(ForInStatement *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->Target()));
  RECURSE(Visit(node->Iterable()));
  RECURSE(Visit(node->GetBody()));
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitBinaryOpExpression(BinaryOpExpression *node) {
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
inline void AstTraversalVisitor<Subclass>::VisitLiteralExpression(LiteralExpression *node) {
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
inline void AstTraversalVisitor<Subclass>::VisitDeclarationStatement(DeclarationStatement *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->GetDeclaration()));
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitMemberExpression(MemberExpression *node) {
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
inline void AstTraversalVisitor<Subclass>::VisitComparisonOpExpression(
    ComparisonOpExpression *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->GetLeft()));
  RECURSE(Visit(node->GetRight()));
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitIfStatement(IfStatement *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->GetCondition()));
  RECURSE(Visit(node->GetThenStatement()));
  if (node->HasElseStatement()) {
    RECURSE(Visit(node->GetElseStatement()));
  }
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitExpressionStatement(ExpressionStatement *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->GetExpression()));
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitIndexExpression(IndexExpression *node) {
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
