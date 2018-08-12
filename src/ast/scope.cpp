#include "ast/scope.h"

namespace tpl::ast {

Declaration *Scope::Declare(const AstString *name, Declaration *decl) {
  Declaration *curr_decl = Lookup(name);
  if (curr_decl != nullptr) {
    return curr_decl;
  }
  declarations_.emplace(name, decl);
  return decl;
}

Declaration *Scope::Lookup(const AstString *name) const {
  for (const Scope *scope = this ; scope != nullptr; scope = scope->outer()) {
    Declaration *decl = scope->LookupLocal(name);
    if (decl != nullptr) {
      return decl;
    }
  }

  // Not in any scope
  return nullptr;
}

Declaration *Scope::LookupLocal(const AstString *name) const {
  auto iter = declarations_.find(name);
  return (iter == declarations_.end() ? nullptr : iter->second);
}

}  // namespace tpl::ast