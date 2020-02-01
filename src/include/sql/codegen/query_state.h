#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "ast/identifier.h"
#include "common/common.h"
#include "common/macros.h"
#include "sql/codegen/ast_fwd.h"

namespace tpl::sql::codegen {

class CodeGen;

/**
 * Encapsulates some state in a TPL struct. Typically there is a "build" phase where operators may
 * declare named entries through DeclareStateEntry(), after which the state is "sealed" marking it
 * as frozen. After the state has been sealed, it is immutable.
 *
 * Accessing the state is done through opaque identifiers returned through DeclareStructEntry(). It
 * is not possible, nor should it ever be possible, to reference a state member through name. This
 * is because StateManager is allowed to rename the entries it contains to ensure uniqueness.
 */
class QueryState {
 public:
  using Slot = uint32_t;

  class StateAccess {
   public:
    virtual ~StateAccess() = default;
    virtual ast::Expr *GetStatePtr(CodeGen *codegen) const = 0;
  };

  class DefaultStateAccess : public StateAccess {
   public:
    ast::Expr *GetStatePtr(CodeGen *codegen) const override;
  };

  static const DefaultStateAccess kDefaultStateAccess;

  /**
   * Create a new empty state.
   */
  QueryState();

  /**
   * Create a new empty state using the provided state accessor that's able to load the query state
   * pointer in any given context.
   * @param access The state accessor.
   */
  explicit QueryState(const StateAccess &access);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(QueryState);

  /**
   * Declare a state entry with the provided name and type in the execution runtime query state.
   * @param name The name of the element.
   * @param type_repr The TPL type representation of the element.
   * @return The slot where the inserted state exists.
   */
  Slot DeclareStateEntry(CodeGen *codegen, const std::string &name, ast::Expr *type_repr);

  /**
   * Seal the state and build the final structure. After this point, additional state elements
   * cannot be added.
   * @param codegen The code generation instance.
   * @return The finalized structure declaration.
   */
  ast::StructDecl *ConstructFinalType(CodeGen *codegen, ast::Identifier name);

  /**
   * @return True if the state has been finalized and sealed.
   */
  bool IsSealed() const { return state_type_ != nullptr; }

  /**
   * @return The query state pointer from the current code generation context.
   */
  ast::Expr *GetStatePointer(CodeGen *codegen) const { return access_.GetStatePtr(codegen); }

  /**
   * Access an element in this state by its slot.
   * @param codegen The code generation instance.
   * @return The state entry at the given slot.
   */
  ast::Expr *GetStateEntry(CodeGen *codegen, Slot slot) const;

  /**
   * Get a pointer to the state element at the given slot.
   * @param codegen The code generation instance.
   * @return A pointer to the state entry at the given slot.
   */
  ast::Expr *GetStateEntryPtr(CodeGen *codegen, Slot slot) const;

  /**
   * @return The finalized type of the runtime query state; null if the state hasn't been finalized.
   */
  ast::StructDecl *GetType() const { return state_type_; }

  /**
   * @return The size of the constructed state type, in bytes. This is only possible
   */
  std::size_t GetSize() const;

 private:
  // Metadata for a single state entry
  struct SlotInfo {
    // The declared name of the state in the struct. This is the name of the
    // element as it actually appears in the struct after name collision
    // resolution.
    ast::Identifier name;
    // The type representation for the state.
    ast::Expr *type_repr;
    // Constructor.
    SlotInfo(ast::Identifier _name, ast::Expr *_type_repr) : name(_name), type_repr(_type_repr) {}
  };

 private:
  // State access object;
  const StateAccess &access_;
  // All state metadata
  std::vector<SlotInfo> slots_;
  // The finalized type
  ast::StructDecl *state_type_;
};

}  // namespace tpl::sql::codegen
