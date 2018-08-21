#include "sema/scope.h"

namespace tpl::sema {

bool Scope::Declare(ast::Identifier name, ast::Type *type) {
  ast::Type *curr_decl = Lookup(name);
  if (curr_decl != nullptr) {
    return false;
  }
  table_.emplace(name, type);
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
  auto iter = table_.find(name);
  return (iter == table_.end() ? nullptr : iter->second);
}

}  // namespace tpl::sema