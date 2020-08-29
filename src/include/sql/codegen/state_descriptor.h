#pragma once

#include <functional>
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
class StateDescriptor {
 public:
  // A slot in a state structure.
  using Slot = std::size_t;

  // Function to provide the instance of a state structure in a given context.
  using InstanceProvider = std::function<ast::Expr *(CodeGen *)>;

  /**
   * Create a new empty state using the provided name for the final constructed TPL type. The
   * provided state accessor can be used to load an instance of this state in a given context.
   * @param type_name The name to give the final constructed type for this state.
   * @param access A generic accessor to an instance of this state, used to access state elements.
   */
  StateDescriptor(ast::Identifier type_name, InstanceProvider access);

  /**
   * Declare a state entry with the provided name and type in the execution runtime query state.
   * @param codegen The code-generation instance.
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
  ast::StructDecl *ConstructFinalType(CodeGen *codegen);

  /**
   * @return The query state pointer from the current code generation context.
   */
  ast::Expr *GetStatePointer(CodeGen *codegen) const;

  /**
   * Return the value of the state entry at the given slot.
   * @param codegen The code generation instance.
   * @param slot The slot of the state to read.
   * @return The value of the state entry at the given slot.
   */
  ast::Expr *GetStateEntry(CodeGen *codegen, Slot slot) const;

  /**
   * Return a pointer to the value of the state entry at the given slot.
   * @param codegen The code generation instance.
   * @param slot The slot of the state to read.
   * @return The a pointer to the state entry at the given slot.
   */
  ast::Expr *GetStateEntryPtr(CodeGen *codegen, Slot slot) const;

  /**
   * Return an expression representing the offset of the entry at the given slot in this state.
   * @param codegen The code generation instance.
   * @param slot The slot of the state to read.
   * @return The offset of the state entry at the given slot in this state, in bytes.
   */
  ast::Expr *GetStateEntryOffset(CodeGen *codegen, Slot slot) const;

  /**
   * @return The finalized type of the runtime query state; null if the state hasn't been finalized.
   */
  ast::StructDecl *GetType() const { return state_type_; }

  /**
   * @return The name of the state's type.
   */
  ast::Identifier GetTypeName() const { return name_; }

  /**
   * @return The size of the constructed state type, in bytes. This is only possible
   */
  std::size_t GetSize() const;

 private:
  // Metadata for a single state entry.
  struct SlotInfo {
    // The unique name of the element in the state.
    ast::Identifier name;
    // The type representation for the state.
    ast::Expr *type_repr;
    // Constructor.
    SlotInfo(ast::Identifier name, ast::Expr *type_repr) : name(name), type_repr(type_repr) {}
  };

 private:
  // The name of the state type.
  ast::Identifier name_;
  // State access object.
  InstanceProvider access_;
  // All state metadata
  std::vector<SlotInfo> slots_;
  // The finalized type
  ast::StructDecl *state_type_;
};

}  // namespace tpl::sql::codegen
