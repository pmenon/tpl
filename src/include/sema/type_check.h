#pragma once

#include "ast/ast.h"
#include "ast/ast_visitor.h"
#include "sema/error_reporter.h"
#include "sema/scope.h"
#include "util/region.h"

namespace tpl::sema {

class TypeChecker : public ast::AstVisitor<TypeChecker> {
 public:
  TypeChecker(util::Region &region, ErrorReporter &error_reporter);

  bool Run(ast::AstNode *root);

  // Declare all node visit methods here
#define DECLARE_VISIT_METHOD(type) void Visit##type(ast::type *node);
  AST_NODES(DECLARE_VISIT_METHOD)
#undef DECLARE_VISIT_METHOD

  // Generate primary visit method
  DEFINE_AST_VISITOR_METHOD()

  ErrorReporter &error_reporter() { return error_reporter_; }

 private:
  // Return the current scope
  Scope *scope() { return scope_; }

 private:
  util::Region &region_;

  ErrorReporter &error_reporter_;

  Scope *scope_;
};

}  // namespace tpl::sema