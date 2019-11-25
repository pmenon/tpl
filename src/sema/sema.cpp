#include "sema/sema.h"

#include "ast/context.h"
#include "ast/type.h"
#include "ast/builtins.h"

namespace tpl::sema {

Sema::Sema(ast::Context *ctx)
    : ctx_(ctx),
      error_reporter_(ctx->error_reporter()),
      scope_(nullptr),
      num_cached_scopes_(0),
      curr_func_(nullptr) {}

// Main entry point to semantic analysis and type checking an AST
bool Sema::Run(ast::AstNode *root) {
  Visit(root);
  return error_reporter()->HasErrors();
}

ast::Type *Sema::GetBuiltinType(const uint16_t builtin_kind) {
  return ast::BuiltinType::Get(context(), static_cast<ast::BuiltinType::Kind>(builtin_kind));
}

}  // namespace tpl::sema
