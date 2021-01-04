#pragma once

#include <functional>
#include <string_view>
#include <vector>

#include "ast/identifier.h"
#include "common/common.h"
#include "common/macros.h"
#include "sql/codegen/ast_fwd.h"
#include "sql/codegen/edsl/struct.h"
#include "sql/codegen/edsl/value.h"
#include "sql/codegen/edsl/value_vt.h"

namespace tpl::sql::codegen {

class CodeGen;

/**
 * Encapsulates all execution state needed during query processing. Typically there is a "build"
 * phase where operators may declare named entries through DeclareStateEntry(), after which
 * state is "sealed" marking it as frozen. After the state has been sealed, it is immutable.
 *
 * Accessing the state is done through opaque identifiers returned through DeclareStructEntry(). It
 * is not possible, nor should it ever be possible, to reference a state member through name. This
 * is because StateManager is allowed to rename the entries it contains to ensure uniqueness.
 */
class ExecutionState {
 public:
  // A slot in a state structure.
  using RTSlot = edsl::Struct::RTSlot;

  template <edsl::traits::TPLType T>
  using Slot = edsl::Struct::Slot<T>;

  // Function to provide the instance of a state structure in a given context.
  using InstanceProvider = std::function<edsl::ValueVT(CodeGen *)>;

  /**
   * Create a new empty state using the provided name for the final constructed TPL type. The
   * provided state accessor can be used to load an instance of this state in a given context.
   * @param name The name to give the final constructed type for this state.
   * @param access A generic accessor to an instance of this state, used to access state elements.
   */
  ExecutionState(CodeGen *codegen, std::string_view name, InstanceProvider access);

  /**
   * Declare a state entry with the provided name and type in the runtime query state.
   * @param codegen The code-generation instance.
   * @param name The name of the element.
   * @param type The type of the element.
   * @return The slot where the inserted state exists.
   */
  RTSlot DeclareStateEntry(std::string_view name, ast::Type *type);

  /**
   * Declare a state entry with the provided name and template type in the runtime query state.
   * @param codegen The code-generation instance.
   * @param name The name of the element.
   * @param type The type of the element.
   * @return The slot where the inserted state exists.
   */
  template <edsl::traits::TPLType T>
  Slot<T> DeclareStateEntry(std::string_view name) {
    return struct_.AddMember<T>(name);
  }

  /**
   * Seal the state and build the final structure. After this point, additional state elements
   * cannot be added. Only the first call to this function has an effect. All calls after the first
   * return the declaration constructed on the first call.
   * @param codegen The code generation instance.
   * @return The finalized structure declaration.
   */
  void ConstructFinalType();

  /**
   * @return The query state pointer from the current code generation context.
   */
  edsl::ValueVT GetStatePtr(CodeGen *codegen) const;

  /**
   * Return the value of the state entry at the given slot.
   * @param codegen The code generation instance.
   * @param slot The slot of the state to read.
   * @return The value of the state entry at the given slot.
   */
  edsl::ReferenceVT GetStateEntryGeneric(CodeGen *codegen, RTSlot slot) const;

  /**
   * Return the value of the state entry at the given slot assuming the given templated type. An
   * assertion is triggered if the types do not match.
   * @tparam T The template type of the state element.
   * @param codegen The code generation instance.
   * @param slot The slot of the state to read.
   * @return The value of the state entry at the given slot.
   */
  template <edsl::traits::TPLType T>
  edsl::Reference<T> GetStateEntry(CodeGen *codegen, Slot<T> slot) const {
    return struct_.Member<T>(GetStatePtr(codegen), slot);
  }

  /**
   * Return a pointer to the value of the state entry at the given slot.
   * @param slot The slot of the state to read.
   * @return The a pointer to the state entry at the given slot.
   */
  edsl::ValueVT GetStateEntryPtrGeneric(CodeGen *codegen, RTSlot slot) const;

  /**
   * Return a pointer to the value of the state entry at the given slot.
   * @param slot The slot of the state to read.
   * @return The a pointer to the state entry at the given slot.
   */
  template <edsl::traits::TPLType T>
  edsl::Value<T *> GetStateEntryPtr(CodeGen *codegen, Slot<T> slot) const {
    return struct_.MemberPtr<T>(GetStatePtr(codegen), slot);
  }

  /**
   * Return an expression representing the offset of the entry at the given slot in this state.
   * @param codegen The code generation instance.
   * @param slot The slot of the state to read.
   * @return The offset of the state entry at the given slot in this state, in bytes.
   */
  edsl::Value<uint32_t> GetStateEntryOffset(CodeGen *codegen, RTSlot slot) const;

  /**
   * @return The finalized type of the runtime query state; null if the state hasn't been finalized.
   */
  ast::Type *GetType() const { return struct_.GetType(); }

  /**
   * @return A pointer to the finalized query state type.
   */
  ast::Type *GetPointerToType() const { return struct_.GetPtrToType(); }

  /**
   * @return The name of the state's type.
   */
  ast::Identifier GetTypeName() const { return struct_.GetName(); }

  /**
   * @return The size of the constructed state type, in bytes. This is only possible
   */
  std::size_t GetSizeRaw() const;

  /**
   * @return The EDSL value of the size of the constructed state, in bytes.
   */
  edsl::Value<uint32_t> GetSize() const { return struct_.GetSize(); }

 private:
  // The struct capturing all state.
  edsl::Struct struct_;
  // State access object.
  InstanceProvider access_;
};

}  // namespace tpl::sql::codegen
