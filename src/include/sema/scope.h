#pragma once

#include "llvm/ADT/DenseMap.h"

#include "ast/identifier.h"
#include "common/common.h"

namespace tpl {

namespace ast {
class Decl;
class Type;
}  // namespace ast

namespace sema {

/**
 * Scopes are transient structures used during parsing to resolve named identifiers to their types.
 */
class Scope {
 public:
  /**
   * The kinds of scopes.
   */
  enum class Kind : uint8_t { Block, Function, File, Loop };

  /**
   * Create a new scope that wraps an existing outer scope.
   * @param outer The outer scope.
   * @param scope_kind The kind of scope.
   */
  Scope(Scope *outer, Kind scope_kind) { Init(outer, scope_kind); }

  /**
   * Reinitialize this scope to wrap the provided outer scope and using the provided scope kind.
   * @param outer The outer scope.
   * @param scope_kind The kind of scope.
   */
  void Init(Scope *outer, Kind scope_kind) {
    outer_ = outer;
    scope_kind_ = scope_kind;
    declarations_.clear();
  }

  /**
   * Declare an element with the given name and type in this scope.
   * @param decl_name The name of the declaration.
   * @param type The type of the declaration.
   * @return True if no other declaration in the current scope with the provided name exists; false
   *         if a declaration with the given name already exists in the local scope.
   */
  bool Declare(ast::Identifier decl_name, ast::Type *type);

  /**
   * Lookup a declaration with the provided name. Resolution is performed through all available
   * scopes beginning at this inner most scope and chaining through each outer scope.
   * @param name The name of the declaration to lookup.
   * @return The resolved type of the declaration one exists; false if no such declaration exists.
   */
  ast::Type *Lookup(ast::Identifier name) const;

  /**
   * Lookup a declaration with the provided name, but only in the inner-most local scope.
   * @param name The name of the declaration to lookup.
   * @return The resolved type of the declaration one exists; false if no such declaration exists.
   */
  ast::Type *LookupLocal(ast::Identifier name) const;

  /**
   * @return The immediate outer scope.
   */
  Scope *GetOuter() const { return outer_; }

 private:
  // The outer scope.
  Scope *outer_;
  // The scope kind.
  Kind scope_kind_;
  // The mapping of identifiers to their types.
  llvm::DenseMap<ast::Identifier, ast::Type *> declarations_;
};

}  // namespace sema
}  // namespace tpl
