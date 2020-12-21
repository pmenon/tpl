#pragma once

#include <type_traits>

#include "ast/ast.h"
#include "ast/ast_node_factory.h"
#include "ast/type_builder.h"
#include "sql/codegen/codegen.h"
#include "sql/codegen/edsl/traits.h"
#include "sql/codegen/function_builder.h"

namespace tpl::sql::codegen::edsl {

// Forward-declare.
template <traits::TPLType T>
class Reference;

/**
 * A wrapper class holding an RValue of a type known at C++ compile time. It's a simple container
 * holding a pointer to the AstNode type it represents, whose type must match the template type.
 *
 * The templated type can only be one of three things:
 * 1. A TPL atom type, i.e., int32, JoinHashTable, StringVal, etc.
 * 2. A pointer to a valid type.
 * 3. A homogeneous array of valid types.
 *
 * This class is safe to pass around by value. It is the core class for the EDSL API used to build
 * up the AST tree.
 *
 * @tparam T The C++ type the value represents.
 */
template <traits::TPLType T>
class Value {
 public:
  /**
   * Construct a value from the given AST node. Trivial wrapper.
   * @param codegen The code generator.
   * @param val The node representing the value.
   */
  Value(CodeGen *codegen, ast::Expression *val) : codegen_(codegen), val_(val) {
    TPL_ASSERT(val->GetType() == codegen->GetType<T>(), "Expression and template type mismatch!");
  }

  /**
   * Dereference. This is enabled when the templated type is T*, but where T is not void*.
   * @return A reference to the value at the pointer address.
   */
  template <typename Enable = void,
            std::enable_if_t<std::is_same_v<Enable, void> && std::is_pointer_v<T>> * = nullptr>
  Reference<std::remove_pointer_t<T>> operator*() const {
    return Reference<std::remove_pointer_t<T>>(codegen_, codegen_->Deref(val_));
  }

  /**
   * Member access. This is enabled when the templated type is a T* where T is a C++ type.
   * @return A reference to the underlying object.
   */
  template <typename Enable = void,
            std::enable_if_t<std::is_same_v<Enable, void> && std::is_pointer_v<T> &&
                             traits::is_cpp_class_v<std::remove_pointer_t<T>>> * = nullptr>
  Reference<std::remove_pointer_t<T>> *operator->() const {
    using PointeeElementType = std::remove_pointer_t<T>;
    static_assert(sizeof(Reference<PointeeElementType>) == 16, "Unexpected size of Reference");
    return reinterpret_cast<Reference<PointeeElementType> *>(const_cast<CodeGen **>(&codegen_));
  }

  /**
   * Array access. This is enabled only when the templated type is an array T[] with either known
   * or unknown size.
   * @tparam I The type of the index value.
   * @param index The index of the element to access.
   * @return A reference to the element in this array at the given index.
   */
  template <typename I, std::enable_if_t<std::is_array_v<T> &&
                                         traits::is_primitive_int_type<I>::value> * = nullptr>
  Reference<std::remove_pointer_t<std::decay_t<T>>> operator[](const Value<I> &index) const {
    using ArrayElementType = std::remove_pointer_t<std::decay_t<T>>;
    return Reference<ArrayElementType>(codegen_, codegen_->ArrayAccess(GetRaw(), index.GetRaw()));
  }

  /**
   * Array access. This is enabled only when the templated type is an array T[] with either known
   * or unknown size.
   * @tparam I The type of the index value.
   * @param index The index of the element to access.
   * @return A reference to the element in this array at the given index.
   */
  template <typename I, std::enable_if_t<std::is_array_v<T> &&
                                         traits::is_primitive_int_type<I>::value> * = nullptr>
  Reference<std::remove_pointer_t<std::decay_t<T>>> operator[](I index) const {
    using ArrayElementType = std::remove_pointer_t<std::decay_t<T>>;
    return Reference<ArrayElementType>(codegen_,
                                       codegen_->ArrayAccess(GetRaw(), codegen_->Const32(index)));
  }

  /**
   * @return The code generator instance.
   */
  CodeGen *GetCodeGen() const noexcept { return codegen_; }

  /**
   * @return The underlying AST node representing this value.
   */
  ast::Expression *GetRaw() const noexcept { return val_; }

 private:
  // The code generator. Immutable by design.
  CodeGen *const codegen_;
  // The handle to the AST node representing the value. Immutable because
  // there isn't a reason to modify it after construction; and it catches
  // errors like a = b which should use Assign(a, b).
  ast::Expression *const val_;
};

/**
 * Stores a reference to a type T. References can be written to using Assign(). Since a reference
 * inherits from Value<T>, a reference can also be used any language construct that expects a Value.
 * In those scenarios, the value stored in the address is used.
 *
 * Like Value<T>, this cannot be reused in the AST tree unless it is actually a Variable<T>.
 * @tparam T A value TPL type.
 */
template <traits::TPLType T>
class Reference : public Value<T> {
 public:
  /**
   * Create a reference to the given value. The provided value must be a pointer type.
   * @param codegen The code generator instance.
   * @param val The value to reference.
   */
  Reference(CodeGen *codegen, ast::Expression *val) : Value<T>(codegen, val) {}

  /**
   * @return The reference to the variable.
   */
  ast::Expression *GetRef() const noexcept { return Value<T>::GetRaw(); }

  /**
   * @return The address of this reference.
   */
  Value<T *> Addr() const {
    CodeGen *codegen = Value<T>::GetCodeGen();
    return Value<T *>(codegen, codegen->AddressOf(Value<T>::GetRaw()));
  }

 protected:
  /**
   * Constructor used exclusively by Variable.
   * @param codegen The code generator instance.
   * @param name The name of the variable.
   * @param type
   */
  Reference(CodeGen *codegen, ast::Identifier name, ast::Type *type)
      : Value<T>(codegen, codegen->MakeExpr(name, type)) {}
};

/**
 * A local variable of type T. Variable<T> inherits Reference<T> so it can be used in all language
 * constructs that expect a Reference<T> or Value<T>.
 *
 * A variable instance does not imply it has been declared in TPL code. Use Declare() to declare it.
 * Also note that you can declare it with and without initialization.
 *
 * @tparam T The type.
 */
template <traits::TPLType T>
class Variable : public Reference<T> {
 public:
  /**
   * Declare a variable with the given name.
   * @param codegen The code generator instance.
   * @param name The name of the variable.
   */
  Variable(CodeGen *codegen, ast::Identifier name)
      : Variable(codegen, name, codegen->GetType<T>()) {}

  /**
   * Declare a variable with the given name.
   * @param codegen The code generator instance.
   * @param name The name of the variable.
   */
  Variable(CodeGen *codegen, std::string_view name)
      : Variable(codegen, codegen->MakeIdentifier(name)) {}

  /**
   * @return The name of this variable.
   */
  ast::Identifier GetName() const noexcept { return name_; }

  /**
   * @return The type of the variable.
   */
  ast::Type *GetType() const noexcept { return type_; }

 private:
  // Create a variable with the given name and type.
  Variable(CodeGen *codegen, ast::Identifier name, ast::Type *type)
      : Reference<T>(codegen, name, type), name_(name), type_(type) {}

 private:
  // The name of the variable. Immutable by design.
  const ast::Identifier name_;
  // The type. Immutable by design.
  ast::Type *const type_;
};

/**
 * Declare an uninitialized variable.
 * @tparam T The type of variable.
 * @param var The variable to declare.
 */
template <typename T>
void Declare(const Variable<T> &var) {
  CodeGen *codegen = var.GetCodeGen();
  FunctionBuilder *function = codegen->GetCurrentFunction();
  function->Append(codegen->DeclareVar(var.GetName(), var.GetRaw()->GetType()));
}

/**
 * Declare a variable with the given initial value.
 * @tparam T The type of the variable and value.
 * @param var The variable to declare.
 * @param value The value to assign the variable at declaration time.
 */
template <typename T>
void Declare(const Variable<T> &var, const Value<T> &value) {
  CodeGen *codegen = var.GetCodeGen();
  FunctionBuilder *function = codegen->GetCurrentFunction();
  function->Append(codegen->DeclareVarWithInit(var.GetName(), value.GetRaw()));
}

/**
 * Assign a value to the given reference element.
 * @tparam T The type of elements being assigned.
 * @param lhs The destination of the assignment.
 * @param rhs The source value of the assignment.
 * @return The result (unusable) value.
 */
template <typename T>
void Assign(const Reference<T> &lhs, const Value<T> &rhs) {
  CodeGen *codegen = lhs.GetCodeGen();
  FunctionBuilder *function = codegen->GetCurrentFunction();
  function->Append(codegen->Assign(lhs.GetRef(), rhs.GetRaw()));
}

}  // namespace tpl::sql::codegen::edsl
