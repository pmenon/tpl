#pragma once

#include "llvm/ADT/DenseMap.h"

#include "ast/ast.h"
#include "util/region_containers.h"

namespace tpl {

namespace ast {
class Type;
}  // namespace ast

namespace sema {

class Scope {
 public:
  enum class Kind : uint8_t { Block, Function, File, Loop };

  Scope(Scope *outer, Kind scope_kind)
      : outer_(outer), scope_kind_(scope_kind) {}

  // Declare an element with the given name and type in this scope. Return true
  // if successful and false if an element with the given name already exits in
  // the local scope.
  bool Declare(ast::Declaration *decl, ast::Type *type);

  ast::Type *Lookup(ast::Identifier name) const;
  ast::Type *LookupLocal(ast::Identifier name) const;

  Scope *outer() const { return outer_; }

 private:
  Scope *outer_;

  Kind scope_kind_;

  llvm::DenseMap<ast::Identifier, ast::Type *> decls_;
};

}  // namespace sema
}  // namespace tpl