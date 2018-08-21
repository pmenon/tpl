#pragma once

#include "ast/ast.h"
#include "ast/ast_context.h"
#include "ast/ast_visitor.h"
#include "sema/error_reporter.h"
#include "sema/scope.h"
#include "util/region.h"

namespace tpl::sema {

class TypeChecker : public ast::AstVisitor<TypeChecker> {
 public:
  explicit TypeChecker(ast::AstContext &ctx);

  // Run the type checker on the provided AST. Ensures proper types of all
  // statements and expressions, and also annotates the AST with type correct
  // type information.
  bool Run(ast::AstNode *root);

  // Declare all node visit methods here
#define DECLARE_VISIT_METHOD(type) void Visit##type(ast::type *node);
  AST_NODES(DECLARE_VISIT_METHOD)
#undef DECLARE_VISIT_METHOD

  // Generate primary visit method
  DEFINE_AST_VISITOR_METHOD()

 private:
  ast::Type *Resolve(ast::Expression *expr) {
    Visit(expr);
    return expr->type();
  }

  ast::AstContext &ast_context() const { return ctx_; }

  util::Region &region() const { return region_; }

  ErrorReporter &error_reporter() const { return error_reporter_; }

  ast::FunctionLiteralExpression *current_function() const {
    return curr_func_;
  }

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Scoping
  ///
  //////////////////////////////////////////////////////////////////////////////

  // Return the current scope
  Scope *scope() { return scope_; }

  Scope *OpenScope(Scope::Kind scope_kind) {
    auto *scope = new (region_) Scope(region_, scope_, scope_kind);
    scope_ = scope;
    return scope;
  }

  void CloseScope(UNUSED Scope *expected_top) {
    TPL_ASSERT(scope() == expected_top);
    scope_ = scope_->outer();
  }

  /*
   *
   */
  class FunctionScope {
   public:
    FunctionScope(TypeChecker &check, ast::FunctionLiteralExpression *func)
        : check_(check), prev_func_(nullptr) {
      prev_func_ = check.curr_func_;
      check.curr_func_ = func;
    }

    ~FunctionScope() {
      // Reset the function
      check_.curr_func_ = prev_func_;
    }

   private:
    // The type checking instance
    TypeChecker &check_;

    // The previous function
    ast::FunctionLiteralExpression *prev_func_;
  };

 private:
  ast::AstContext &ctx_;

  util::Region &region_;

  ErrorReporter &error_reporter_;

  Scope *scope_;

  ast::FunctionLiteralExpression *curr_func_;
};

}  // namespace tpl::sema