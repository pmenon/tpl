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
  TypeChecker(ast::AstContext &ctx);

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

  Scope *NewScope(Scope::Kind scope_kind) {
    return new (region_) Scope(region_, scope_, scope_kind);
  }

  /*
   *
   */
  class BlockScope {
   public:
    BlockScope(Scope **scope_stack, Scope *scope)
        : scope_stack_(scope_stack), prev_scope_(*scope_stack) {}

    ~BlockScope() { *scope_stack_ = prev_scope_; }

   private:
    Scope **scope_stack_;
    Scope *prev_scope_;
  };

  /*
   *
   */
  class FunctionScope {
   public:
    FunctionScope(TypeChecker &check, ast::FunctionLiteralExpression *func,
                  Scope *scope)
        : check_(check), prev_func_(nullptr), prev_scope_(nullptr) {
      prev_func_ = check.curr_func_;
      check.curr_func_ = func;

      prev_scope_ = check.scope_;
      check.scope_ = scope;
    }

    ~FunctionScope() {
      // Reset the function
      check_.curr_func_ = prev_func_;

      // Reset the scope
      check_.scope_ = prev_scope_;
    }

   private:
    // The type checking instance
    TypeChecker &check_;

    // The previous function
    ast::FunctionLiteralExpression *prev_func_;

    // The previous scope
    Scope *prev_scope_;
  };

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Errors
  ///
  //////////////////////////////////////////////////////////////////////////////

  template <typename... ArgTypes>
  void ReportError(const SourcePosition &pos,
                   const ErrorMessage<ArgTypes...> &msg, ArgTypes &&... args) {
    error_reporter().Report(pos, msg, std::move(args)...);
  }

 private:
  ast::AstContext &ctx_;

  util::Region &region_;

  ErrorReporter &error_reporter_;

  Scope *scope_;

  ast::FunctionLiteralExpression *curr_func_;
};

}  // namespace tpl::sema