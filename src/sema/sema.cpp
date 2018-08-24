#include "sema/sema.h"

#include "ast/type.h"
#include "ast/ast_context.h"

namespace tpl::sema {

Sema::Sema(ast::AstContext &ctx)
    : ctx_(ctx),
      error_reporter_(ctx.error_reporter()),
      scope_(nullptr),
      num_cached_scopes_(0),
      curr_func_(nullptr) {}

bool Sema::Run(ast::AstNode *root) {
  Visit(root);
  return error_reporter().has_errors();
}

}  // namespace tpl::sema