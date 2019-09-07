#pragma once

#include <memory>

#include "ast/ast.h"
#include "ast/ast_visitor.h"
#include "ast/builtins.h"
#include "sema/error_reporter.h"
#include "sema/scope.h"

namespace tpl {

namespace ast {
class Context;
}  // namespace ast

namespace sql {
class Schema;
}  // namespace sql

namespace sema {

/**
 * This is the main class that performs semantic analysis of TPL programs. It
 * traverses an untyped TPL abstract syntax tree (AST), fills in types based on
 * declarations, derives types of expressions and ensures correctness of all
 * operations in the TPL program.
 *
 * Usage:
 * @code
 * sema::Sema check(context);
 * bool has_errors = check.Run(ast);
 * if (has_errors) {
 *   // handle errors
 * }
 * @encode
 */
class Sema : public ast::AstVisitor<Sema> {
 public:
  /**
   * Construct using the given context
   * @param ctx The context used to acquire memory for new ASTs and the
   *            diagnostic error reporter.
   */
  explicit Sema(ast::Context *ctx);

  /**
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(Sema);

  /**
   * Run the type checker on the provided AST rooted at \a root. Ensures proper
   * types of all statements and expressions, and also annotates the AST with
   * correct type information.
   * @return True if type-checking found errors; false otherwise
   */
  bool Run(ast::AstNode *root);

  // Declare all node visit methods here
#define DECLARE_AST_VISIT_METHOD(type) void Visit##type(ast::type *node);
  AST_NODES(DECLARE_AST_VISIT_METHOD)
#undef DECLARE_AST_VISIT_METHOD

  ast::Context *context() const { return ctx_; }

  ErrorReporter *error_reporter() const { return error_reporter_; }

 private:
  // Resolve the type of the input expression
  ast::Type *Resolve(ast::Expr *expr) {
    Visit(expr);
    return expr->type();
  }

  // Create a builtin type
  ast::Type *GetBuiltinType(uint16_t builtin_kind);

  struct CheckResult {
    ast::Type *result_type;
    ast::Expr *left;
    ast::Expr *right;
  };

  void ReportIncorrectCallArg(ast::CallExpr *call, uint32_t index, ast::Type *expected);

  void ReportIncorrectCallArg(ast::CallExpr *call, uint32_t index, const char *expected);

  // Implicitly cast the input expression into the target type using the
  // provided cast kind, also setting the type of the casted expression result.
  ast::Expr *ImplCastExprToType(ast::Expr *expr, ast::Type *target_type, ast::CastKind cast_kind);

  // Check the number of arguments to the call; true if good, false otherwise
  bool CheckArgCount(ast::CallExpr *call, uint32_t expected_arg_count);
  bool CheckArgCountAtLeast(ast::CallExpr *call, uint32_t expected_arg_count);

  // Check boolean logic operands: and, or
  CheckResult CheckLogicalOperands(parsing::Token::Type op, const SourcePosition &pos,
                                   ast::Expr *left, ast::Expr *right);

  // Check operands to an arithmetic operation: +, -, *, etc.
  CheckResult CheckArithmeticOperands(parsing::Token::Type op, const SourcePosition &pos,
                                      ast::Expr *left, ast::Expr *right);

  CheckResult CheckComparisonOperands(parsing::Token::Type op, const SourcePosition &pos,
                                      ast::Expr *left, ast::Expr *right);

  // Check the assignment of the expression to a variable or the target type.
  // Return true if the assignment is valid, and false otherwise.
  // Will also apply an implicit cast to make the assignment valid.
  bool CheckAssignmentConstraints(ast::Type *target_type, ast::Expr *&expr);

  // Dispatched from VisitCall() to handle builtin functions
  void CheckBuiltinCall(ast::CallExpr *call);
  void CheckBuiltinMapCall(ast::CallExpr *call);
  void CheckBuiltinSqlConversionCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinFilterCall(ast::CallExpr *call);
  void CheckBuiltinAggHashTableCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinAggHashTableIterCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinAggPartIterCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinAggregatorCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinJoinHashTableInit(ast::CallExpr *call);
  void CheckBuiltinJoinHashTableInsert(ast::CallExpr *call);
  void CheckBuiltinJoinHashTableBuild(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinJoinHashTableLookup(ast::CallExpr *call);
  void CheckBuiltinJoinHashTableFree(ast::CallExpr *call);
  void CheckBuiltinHashTableEntryIterCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinSorterInit(ast::CallExpr *call);
  void CheckBuiltinSorterInsert(ast::CallExpr *call);
  void CheckBuiltinSorterSort(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinSorterFree(ast::CallExpr *call);
  void CheckBuiltinSorterIterCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinExecutionContextCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinThreadStateContainerCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckMathTrigCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinSizeOfCall(ast::CallExpr *call);
  void CheckBuiltinPtrCastCall(ast::CallExpr *call);
  void CheckBuiltinTableIterCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinTableIterParCall(ast::CallExpr *call);
  void CheckBuiltinVPICall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinFilterManagerCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinVectorFilterExecCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinHashCall(ast::CallExpr *call, ast::Builtin builtin);

  // -------------------------------------------------------
  // Scoping
  // -------------------------------------------------------

  Scope *current_scope() { return scope_; }

  // Enter a new scope
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

  // Exit the current scope
  void ExitScope() {
    TPL_ASSERT(current_scope() != nullptr, "Mismatched scope exit");

    Scope *scope = current_scope();
    scope_ = scope->outer();

    if (num_cached_scopes_ < kScopeCacheSize) {
      scope_cache_[num_cached_scopes_++].reset(scope);
    } else {
      delete scope;
    }
  }

  /// RAII scope class to track the current scope
  class SemaScope {
   public:
    SemaScope(Sema *check, Scope::Kind scope_kind) : check_(check), exited_(false) {
      check->EnterScope(scope_kind);
    }

    ~SemaScope() { Exit(); }

    void Exit() {
      if (!exited_) {
        check_->ExitScope();
        exited_ = true;
      }
    }

    Sema *check() { return check_; }

   private:
    Sema *check_;
    bool exited_;
  };

  /// RAII scope class to capture both the current function and its scope
  class FunctionSemaScope {
   public:
    FunctionSemaScope(Sema *check, ast::FunctionLitExpr *func)
        : prev_func_(check->current_function()), block_scope_(check, Scope::Kind::Function) {
      check->curr_func_ = func;
    }

    ~FunctionSemaScope() { Exit(); }

    void Exit() {
      block_scope_.Exit();
      block_scope_.check()->curr_func_ = prev_func_;
    }

   private:
    ast::FunctionLitExpr *prev_func_;
    SemaScope block_scope_;
  };

  ast::FunctionLitExpr *current_function() const { return curr_func_; }

 private:
  // The context
  ast::Context *ctx_;

  // The error reporter
  ErrorReporter *error_reporter_;

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
