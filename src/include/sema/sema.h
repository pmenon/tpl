#pragma once

#include "ast/ast.h"
#include "ast/ast_visitor.h"
#include "sema/error_reporter.h"
#include "sema/scope.h"

namespace tpl {

namespace ast {
class AstContext;
}  // namespace ast

namespace sema {

class Sema : public ast::AstVisitor<Sema> {
 public:
  explicit Sema(ast::AstContext &ctx);

  DISALLOW_COPY_AND_MOVE(Sema);

  // Run the type checker on the provided AST. Ensures proper types of all
  // statements and expressions, and also annotates the AST with correct
  // type information.
  bool Run(ast::AstNode *root);

  // Declare all node visit methods here
#define DECLARE_VISIT_METHOD(type) void Visit##type(ast::type *node);
  AST_NODES(DECLARE_VISIT_METHOD)
#undef DECLARE_VISIT_METHOD

  // Generate primary visit method
  DEFINE_AST_VISITOR_METHOD()

 private:
  ast::Type *Resolve(ast::Expr *expr) {
    Visit(expr);
    return expr->type();
  }

  ast::AstContext &ast_context() const { return ctx_; }

  ErrorReporter &error_reporter() const { return error_reporter_; }

  ast::FunctionLitExpr *current_function() const { return curr_func_; }

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Scoping
  ///
  //////////////////////////////////////////////////////////////////////////////

  Scope *current_scope() { return scope_; }

  void EnterScope(Scope::Kind scope_kind) {
    if (num_cached_scopes_ > 0) {
      Scope *scope = scope_cache_[--num_cached_scopes_].release();
      TPL_ASSERT(scope != nullptr, "Cached scope was null");
      scope->Init(current_scope(), scope_kind);
      scope_ = scope;
    } else {
      scope_ = new Scope(current_scope(), scope_kind);
    }
  }

  void ExitScope() {
    Scope *scope = current_scope();
    scope_ = scope->outer();

    if (num_cached_scopes_ < kScopeCacheSize) {
      scope_cache_[num_cached_scopes_++].reset(scope);
    } else {
      delete scope;
    }
  }

  class SemaScope {
   public:
    SemaScope(Sema &check, Scope::Kind scope_kind)
        : check_(check), exited_(false) {
      check.EnterScope(scope_kind);
    }

    ~SemaScope() { Exit(); }

    void Exit() {
      if (!exited_) {
        check_.ExitScope();
        exited_ = true;
      }
    }

    Sema &check() { return check_; }

   private:
    Sema &check_;
    bool exited_;
  };

  /*
   *
   */
  class FunctionSemaScope {
   public:
    FunctionSemaScope(Sema &check, ast::FunctionLitExpr *func)
        : prev_func_(check.current_function()),
          block_scope_(check, Scope::Kind::Function) {
      check.curr_func_ = func;
    }

    ~FunctionSemaScope() { Exit(); }

    void Exit() {
      block_scope_.Exit();
      block_scope_.check().curr_func_ = prev_func_;
    }

   private:
    ast::FunctionLitExpr *prev_func_;
    SemaScope block_scope_;
  };

 private:
  // The context
  ast::AstContext &ctx_;

  // The error reporter
  ErrorReporter &error_reporter_;

  // The current active scope
  Scope *scope_;

  // A cache of scopes to reduce allocations
  static constexpr const uint32_t kScopeCacheSize = 4;
  uint64_t num_cached_scopes_;
  std::unique_ptr<Scope> scope_cache_[kScopeCacheSize] = {nullptr};

  ast::FunctionLitExpr *curr_func_;
};

}  // namespace sema
}  // namespace tpl