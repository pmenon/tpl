#pragma once

#include "ast/ast.h"
#include "ast/ast_visitor.h"

namespace tpl::parsing {

class TypeChecker : public ast::AstVisitor<TypeChecker> {
 public:
  explicit TypeChecker(ast::AstNode *root) : root_(root) {}

  bool Run();

  // Declare all node visit methods here
#define DECLARE_VISIT_METHOD(type) void Visit##type(ast::type *node);
  AST_NODES(DECLARE_VISIT_METHOD)
#undef DECLARE_VISIT_METHOD

  // Generate primary visit method
  DEFINE_AST_VISITOR_METHOD()

 private:
  ast::AstNode *root_;
};

}  // namespace tpl::parsing