#pragma once

#include <memory>

#include "ast/ast.h"
#include "ast/ast_visitor.h"
#include "ast/builtins.h"
#include "sema/scope.h"

namespace tpl {

namespace ast {
class Context;
}  // namespace ast

namespace sql {
class Schema;
}  // namespace sql

namespace sema {

class ErrorReporter;

/**
 * This is the main class that performs semantic analysis of TPL programs. It traverses an untyped
 * TPL abstract syntax tree (AST), fills in types based on declarations, derives types of
 * expressions and ensures correctness of all operations in the TPL program.
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
   * Construct using the given context.
   * @param context The context in which type-checking occurs. Used to source AST allocations and
   *                report diagnostics during type-checking.
   */
  explicit Sema(ast::Context *context);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(Sema);

  /**
   * Run the type checker on the provided AST rooted at @em root. Ensures proper types of all
   * statements and expressions, and also annotates the AST with correct type information.
   * @return True if type-checking found errors; false otherwise
   */
  bool Run(ast::AstNode *root);

  // Declare all node visit methods here
#define DECLARE_AST_VISIT_METHOD(type) void Visit##type(ast::type *node);
  AST_NODES(DECLARE_AST_VISIT_METHOD)
#undef DECLARE_AST_VISIT_METHOD

 private:
  // Resolve the type of the input expression.
  ast::Type *Resolve(ast::Expr *expr) {
    Visit(expr);
    return expr->GetType();
  }

  // Create a builtin type.
  ast::Type *GetBuiltinType(uint16_t builtin_kind);

  struct CheckResult {
    ast::Type *result_type;
    ast::Expr *left;
    ast::Expr *right;
  };

  // Report an incorrect call argument for the given call expression.
  void ReportIncorrectCallArg(ast::CallExpr *call, uint32_t index, ast::Type *expected);
  void ReportIncorrectCallArg(ast::CallExpr *call, uint32_t index, const char *expected);

  // Implicitly cast the input expression into the target type using the
  // provided cast kind, also setting the type of the casted expression result.
  ast::Expr *ImplCastExprToType(ast::Expr *expr, ast::Type *target_type, ast::CastKind cast_kind);

  // Check the number of arguments to the call; true if good, false otherwise.
  bool CheckArgCount(ast::CallExpr *call, uint32_t expected_arg_count);
  bool CheckArgCountAtLeast(ast::CallExpr *call, uint32_t expected_arg_count);

  // Check boolean logic operands: and, or.
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

  // Dispatched from VisitCall() to handle builtin functions.
  void CheckBuiltinCall(ast::CallExpr *call);
  void CheckSqlConversionCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckNullValueCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinStringLikeCall(ast::CallExpr *call);
  void CheckBuiltinDateFunctionCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinConcat(ast::CallExpr *call);
  void CheckBuiltinAggHashTableCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinAggHashTableIterCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinAggPartIterCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinAggregatorCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinJoinHashTableInit(ast::CallExpr *call);
  void CheckBuiltinJoinHashTableInsert(ast::CallExpr *call);
  void CheckBuiltinJoinHashTableBuild(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinJoinHashTableLookup(ast::CallExpr *call);
  void CheckBuiltinJoinHashTableFree(ast::CallExpr *call);
  void CheckBuiltinHashTableEntryCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinSorterInit(ast::CallExpr *call);
  void CheckBuiltinSorterInsert(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinSorterSort(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinSorterFree(ast::CallExpr *call);
  void CheckBuiltinSorterIterCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinExecutionContextCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinThreadStateContainerCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckMathTrigCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinSizeOfCall(ast::CallExpr *call);
  void CheckBuiltinOffsetOfCall(ast::CallExpr *call);
  void CheckBuiltinPtrCastCall(ast::CallExpr *call);
  void CheckBuiltinIntCast(ast::CallExpr *call);
  void CheckBuiltinTableIterCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinTableIterParCall(ast::CallExpr *call);
  void CheckBuiltinVPICall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinFilterManagerCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinVectorFilterCall(ast::CallExpr *call);
  void CheckBuiltinCompactStorageWriteCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinCompactStorageReadCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinHashCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckResultBufferCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckCSVReaderCall(ast::CallExpr *call, ast::Builtin builtin);

  // -------------------------------------------------------
  // Scoping
  // -------------------------------------------------------

  // Enter a new scope.
  void EnterScope(Scope::Kind scope_kind) {
    if (num_cached_scopes_ > 0) {
      Scope *scope = scope_cache_[--num_cached_scopes_].release();
      TPL_ASSERT(scope != nullptr, "Cached scope was null");
      scope->Init(scope_, scope_kind);
      scope_ = scope;
    } else {
      scope_ = new Scope(scope_, scope_kind);
    }
  }

  // Exit the current scope.
  void ExitScope() {
    TPL_ASSERT(scope_ != nullptr, "Mismatched scope exit.");

    Scope *scope = scope_;
    scope_ = scope->GetOuter();

    if (num_cached_scopes_ < kScopeCacheSize) {
      scope_cache_[num_cached_scopes_++].reset(scope);
    } else {
      delete scope;
    }
  }

  /**
   * RAII class to automatically enter and exit a new scope during its lifetime. Callers can
   * control the type of scope that is created.
   */
  class SemaScope {
   public:
    SemaScope(Sema *check, Scope::Kind scope_kind) : check_(check), exited_(false) {
      check->EnterScope(scope_kind);
    }

    // Destructor. Exits the current scope.
    ~SemaScope() { Exit(); }

    // Manually exit the current scope, if not already exited.
    void Exit() {
      if (!exited_) {
        check_->ExitScope();
        exited_ = true;
      }
    }

    // Access the semantic check instance.
    Sema *Check() { return check_; }

   private:
    // The checker.
    Sema *check_;
    // Flag indicating if the scope has already exited.
    bool exited_;
  };

  /**
   * RAII scope class to capture both the current function and its scope.
   */
  class FunctionSemaScope {
   public:
    FunctionSemaScope(Sema *check, ast::FunctionLiteralExpr *func)
        : prev_func_(check->GetCurrentFunction()), block_scope_(check, Scope::Kind::Function) {
      check->curr_func_ = func;
    }

    // Destructor. Exits the current scope.
    ~FunctionSemaScope() { Exit(); }

    // Manually exit the current scope, if not already exited.
    void Exit() {
      block_scope_.Exit();
      block_scope_.Check()->curr_func_ = prev_func_;
    }

   private:
    // The outer-nested function being type-checked.
    ast::FunctionLiteralExpr *prev_func_;
    SemaScope block_scope_;
  };

  // Return the function that's currently getting type-checked.
  ast::FunctionLiteralExpr *GetCurrentFunction() const { return curr_func_; }

 private:
  // By default, we keep pre-allocate four scopes for four levels of nesting.
  // This seems to be good for the TPL programs we generate, but adjust as
  // need be.
  static constexpr const uint32_t kScopeCacheSize = 4;

  // The context.
  ast::Context *context_;
  // The error reporter.
  ErrorReporter *error_reporter_;
  // The current active scope.
  Scope *scope_;
  // A cache of scopes to reduce allocations.
  uint64_t num_cached_scopes_;
  std::unique_ptr<Scope> scope_cache_[kScopeCacheSize] = {nullptr};
  // The current in-flight function being type-checked.
  ast::FunctionLiteralExpr *curr_func_;
};

}  // namespace sema
}  // namespace tpl
