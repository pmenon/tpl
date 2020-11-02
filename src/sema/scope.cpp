#include "sema/scope.h"

#include "ast/ast.h"

namespace tpl::sema {

bool Scope::Declare(ast::Identifier decl_name, ast::Type *type) {
  ast::Type *curr_decl = Lookup(decl_name);
  if (curr_decl != nullptr) {
    return false;
  }
  declarations_[decl_name] = type;
  return true;
}

ast::Type *Scope::Lookup(ast::Identifier name) const {
  for (const Scope *scope = this; scope != nullptr; scope = scope->GetOuter()) {
    if (ast::Type *decl_type = scope->LookupLocal(name)) {
      return decl_type;
    }
  }
  return nullptr;
}

ast::Type *Scope::LookupLocal(ast::Identifier name) const {
  const auto iter = declarations_.find(name);
  return iter == declarations_.end() ? nullptr : iter->second;
}

}  // namespace tpl::sema
