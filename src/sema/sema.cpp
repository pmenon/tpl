#include "sema/sema.h"

#include "ast/builtins.h"
#include "ast/context.h"
#include "ast/type.h"
#include "sema/error_reporter.h"

namespace tpl::sema {

Sema::Sema(ast::Context *context)
    : context_(context),
      error_reporter_(context->GetErrorReporter()),
      scope_(nullptr),
      num_cached_scopes_(0),
      curr_func_(nullptr) {
  // Fill scope cache.
  for (auto &scope : scope_cache_) {
    scope = std::make_unique<Scope>(nullptr, Scope::Kind::File);
  }
  num_cached_scopes_ = kScopeCacheSize;
}

// Main entry point to semantic analysis and type checking an AST
bool Sema::Run(ast::AstNode *root) {
  Visit(root);
  return error_reporter_->HasErrors();
}

ast::Type *Sema::GetBuiltinType(const uint16_t builtin_kind) {
  return ast::BuiltinType::Get(context_, static_cast<ast::BuiltinType::Kind>(builtin_kind));
}

}  // namespace tpl::sema
