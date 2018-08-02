#pragma once

#include "ast/ast_visitor.h"

namespace tpl {

class PrettyPrint : public AstVisitor<PrettyPrint> {
 public:
  explicit PrettyPrint(AstNode *root) : root_(root) {}

  void Print() {
    Visit(root_);
  }

  // Declare all node visit methods here
#define DECLARE_VISIT_METHOD(NodeType) \
  void Visit##NodeType(NodeType *node);

  AST_NODES(DECLARE_VISIT_METHOD)
#undef DECLARE_VISIT_METHOD

  // Generate primary visit method
  GEN_VISIT_METHOD

 private:
  AstNode *root_;
};

}  // namespace tpl