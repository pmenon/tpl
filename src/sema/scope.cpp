#include "sema/scope.h"

#include "ast/ast.h"

namespace tpl::sema {

bool Scope::Declare(ast::Decl *decl, ast::Type *type) {
  ast::Type *curr_decl = Lookup(decl->name());
  if (curr_decl != nullptr) {
    return false;
  }
  decls_.insert(std::make_pair(decl->name(), type));
  return true;
}

ast::Type *Scope::Lookup(ast::Identifier name) const {
  for (const Scope *scope = this; scope != nullptr; scope = scope->outer()) {
    ast::Type *decl_type = scope->LookupLocal(name);
    if (decl_type != nullptr) {
      return decl_type;
    }
  }

  // Not in any scope
  return nullptr;
}

ast::Type *Scope::LookupLocal(ast::Identifier name) const {
  auto iter = decls_.find(name);
  return (iter == decls_.end() ? nullptr : iter->second);
}

}  // namespace tpl::sema