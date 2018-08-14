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
template <typename Subclass>
class AstVisitor {
 public:
  void Visit(AstNode *node) { impl().Visit(node); }

 protected:
  Subclass &impl() { return *static_cast<Subclass *>(this); }
};

#define GEN_VISIT_CASE(kind)                              \
  case ::tpl::ast::AstNode::Kind::kind: {                 \
    return impl().Visit##kind(static_cast<::tpl::ast::kind *>(node)); \
  }

#define GEN_VISITOR_SWITCH() \
  switch (node->kind()) { AST_NODES(GEN_VISIT_CASE) }

#define DEFINE_AST_VISITOR_METHOD() \
  void Visit(::tpl::ast::AstNode *node) { GEN_VISITOR_SWITCH() }

}  // namespace tpl::ast