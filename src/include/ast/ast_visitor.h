#pragma once

#include "ast/ast.h"

namespace tpl {

/**
 *
 * @tparam Subclass
 */
template <typename Subclass>
class AstVisitor {
 public:
  void Visit(AstNode *node) { impl()->Visit(node); }

 private:
  Subclass *impl() { return reinterpret_cast<Subclass *>(this); }
};

#define GEN_VISIT_CASE(NodeType)   \
  case AstNode::Type::NodeType: {  \
    impl()->Visit##NodeType(node); \
  }

#define GEN_VISITORS                                         \
  void Visit(AstNode *node) {                                \
    switch (node->node_type()) { AST_NODES(GEN_VISIT_CASE) } \
  }


}  // namespace tpl