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

  void CloseScope(Scope *scope) {
    TPL_ASSERT(scope == scope_);
    scope_ = scope->outer();
  }

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
  util::Region &region_;

  ErrorReporter &error_reporter_;

  Scope *scope_;
};

}  // namespace tpl::sema