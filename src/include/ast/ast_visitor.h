#pragma once

#include "ast/ast.h"

namespace tpl::ast {

/**
 * Base class for AST node visitors. Implements Curiously Recurring Template
 * Pattern (CRTP) to avoid virtual function call invocation overhead from using
 * regular inheritance. Here, derived classes subclass AstVisitor passing
 * the derived class's name as the template argument, as so:
 *
 * class Derived : public AstVisitor<Derived> {
 *   ..
 * }
 *
 * All AST node visitations will get forwarded to the derived class.
 */
template <typename Subclass>
class AstVisitor {
 public:
  void Visit(AstNode *node) { impl().Visit(node); }

 protected:
  Subclass &impl() { return *static_cast<Subclass *>(this); }
};

#define GEN_VISIT_CASE(kind)                       \
  case AstNode::Kind::kind: {                      \
    impl().Visit##kind(static_cast<kind *>(node)); \
    break;                                         \
  }

#define GEN_VISIT_METHOD                                \
  void Visit(AstNode *node) {                           \
    switch (node->kind()) { AST_NODES(GEN_VISIT_CASE) } \
  }

}  // namespace tpl::ast