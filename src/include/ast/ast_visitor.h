#pragma once

#include "ast/ast.h"

namespace tpl {

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
 *
 * @tparam Subclass
 */
template <typename Subclass>
class AstVisitor {
 public:
  void Visit(AstNode *node) { impl()->Visit(node); }

 protected:
  Subclass *impl() { return reinterpret_cast<Subclass *>(this); }
};

#define GEN_VISIT_CASE(NodeType)                            \
  case AstNode::Type::NodeType: {                           \
    impl()->Visit##NodeType(static_cast<NodeType *>(node)); \
  }

#define GEN_VISIT_METHOD                                     \
  void Visit(AstNode *node) {                                \
    switch (node->node_type()) { AST_NODES(GEN_VISIT_CASE) } \
  }

}  // namespace tpl