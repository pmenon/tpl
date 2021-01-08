#pragma once

#include "ast/ast.h"
#include "common/exception.h"
#include "common/macros.h"
#include "sql/codegen/codegen.h"
#include "sql/codegen/edsl/value.h"

namespace tpl::sql::codegen::edsl {

// Forward declare.
class ReferenceVT;

/**
 * Like Value<T>, ValueVT is a wrapper around an AST node. The difference is that ValueVT does not
 * know the underlying type at C++ build type. We use VT as an acronym for variable-typed.
 */
class ValueVT {
 public:
  /**
   * Create a new value represented by the given AST node.
   * @param codegen The code generator instance.
   * @param val The underlying value.
   */
  ValueVT(CodeGen *codegen, ast::Expression *val) : codegen_(codegen), val_(val) {
    TPL_ASSERT(val != nullptr, "No input expression provided!");
    TPL_ASSERT(val->GetType() != nullptr, "Value MUST have a type at the point of instantiation!");
  }

  /**
   * Construct a variable-typed value from the strongly typed value.
   * @tparam T The type of the value.
   * @param that The value to wrap.
   */
  template <typename T>
  ValueVT(const Value<T> &that) : codegen_(that.GetCodeGen()), val_(that.GetRaw()) {}

  /**
   * @return True if this value has a type and its type is equivalent to the provided type.
   */
  bool HasType(ast::Type *type) const noexcept { return val_->GetType() == type; }

  /**
   * @return The type of this value.
   */
  ast::Type *GetType() const noexcept { return val_->GetType(); }

  /**
   * @return True it the type of this value is a SQL type; false otherwise.
   */
  bool IsSQLType() const noexcept { return GetType()->IsSqlValueType(); }

  /**
   * @return The raw value.
   */
  ast::Expression *GetRaw() const noexcept { return val_; }

  /**
   * @return The code generator instance.
   */
  CodeGen *GetCodeGen() const noexcept { return codegen_; }

  /**
   * The most important operation on variable-typed values is the conversion to a typed Value<T>.
   * At best, we can trigger a runtime assertion if the types do not match up.
   * @tparam T The C++ build-time type to cast to.
   * @return The value.
   */
  template <typename T>
  Value<T> As() const {
    TPL_ASSERT(GetType() == codegen_->GetType<T>(), "Type mismatch: Cannot recast to typed value!");
    return Value<T>(codegen_, val_);
  }

  /**
   * Dereference this value. Triggers assertion if the value is not dereference-able.
   * @return The dereferenced value.
   */
  ReferenceVT operator*() const;

  /**
   * Array access. Triggers an assertion if the value is not an array type.
   * @param index The index of the element to access.
   * @return A reference to the element at the given index in this array, if this is an array.
   */
  ReferenceVT operator[](const ValueVT &index);

  /**
   * Array access. Triggers an assertion if the value is not an array type.
   * @tparam I The type the index value represents. Must be a primitive integer!
   * @param index The index of the element to access.
   * @return A reference to the element at the given index in this array, if this is an array.
   */
  template <typename I, std::enable_if_t<traits::is_primitive_int_type<I>::value> * = nullptr>
  ReferenceVT operator[](const Value<I> &index);

  /**
   * Array access. Triggers an assertion if the value is not an array type.
   * @tparam I The type the index value represents. Must be a primitive integer!
   * @param index The index of the element to access.
   * @return A reference to the element at the given index in this array, if this is an array.
   */
  template <typename I, std::enable_if_t<traits::is_primitive_int_type<I>::value> * = nullptr>
  ReferenceVT operator[](I index) const;

 private:
  // The code generator.
  CodeGen *const codegen_;
  // The immutable expression representing the value.
  ast::Expression *const val_;
};

/**
 * Like Reference<T>, ReferenceVT represents a reference to an underlying assign-able value. Use
 * this class to perform assignments.
 */
class ReferenceVT : public ValueVT {
 public:
  /**
   * Create a reference to this value.
   * @param codegen The code generator instance.
   * @param val The raw value to reference.
   */
  ReferenceVT(CodeGen *codegen, ast::Expression *val) : ValueVT(codegen, val) {
    TPL_ASSERT(val->GetType() != nullptr, "Value being referenced must have a type!");
    TPL_ASSERT(val->GetType()->IsPointerType(), "Value being referenced isn't a pointer!");
  }

  /**
   * Convert this untyped reference into a strongly-typed reference. We can't ensure the conversion
   * is valid at C++ build-time, but we trigger a runtime assertion if the types do not match up.
   * @tparam T The C++ build-time type to cast to.
   * @return The value.
   */
  template <typename T>
  Reference<T> As() const noexcept {
    TPL_ASSERT(GetType() == GetCodeGen()->GetType<T>(),
               "Type mismatch: cannot recast to typed reference!");
    return Reference<T>(GetCodeGen(), GetRaw());
  }

  /**
   * @return The address of this reference.
   */
  ValueVT Addr() const { return ValueVT(GetCodeGen(), GetCodeGen()->AddressOf(GetRaw())); }

 protected:
  /**
   * Constructor used exclusively by VariableVT.
   * @param codegen The code generator instance.
   * @param name The name of the variable.
   * @param type The type of the variable.
   */
  ReferenceVT(CodeGen *codegen, ast::Identifier name, ast::Type *type)
      : ValueVT(codegen, codegen->MakeExpr(name, type)) {}
};

inline ReferenceVT ValueVT::operator*() const {
  if (!GetType()->IsPointerType()) {
    throw CodeGenerationException("Value is not a pointer type; cannot be dereferenced!");
  }
  return ReferenceVT(codegen_, val_);
}

inline ReferenceVT ValueVT::operator[](const ValueVT &index) {
  if (!GetType()->IsArrayType()) {
    throw CodeGenerationException("Value is not an array type; cannot use array access!");
  }
  return ReferenceVT(codegen_, codegen_->ArrayAccess(val_, index.GetRaw()));
}

template <typename I, std::enable_if_t<traits::is_primitive_int_type<I>::value> *>
inline ReferenceVT ValueVT::operator[](const Value<I> &index) {
  if (!GetType()->IsArrayType()) {
    throw CodeGenerationException("Attempted array access of non-array type!");
  }
  return ReferenceVT(codegen_, codegen_->ArrayAccess(val_, index.GetRaw()));
}

template <typename I, std::enable_if_t<traits::is_primitive_int_type<I>::value> *>
inline ReferenceVT ValueVT::operator[](I index) const {
  if (!GetType()->IsArrayType()) {
    throw CodeGenerationException("Attempted array access of non-array type!");
  }
  return ReferenceVT(codegen_, codegen_->ArrayAccess(val_, index));
}

/**
 * Like Variable<T>, VariableVT represents a variable in TPL code, but whose type is not known at
 * C++ build time.
 */
class VariableVT : public ReferenceVT {
 public:
  /**
   * Declare a variable with the given name.
   * @param codegen The code generator instance.
   * @param name The name of the variable.
   * @param type The type of the variable.
   */
  VariableVT(CodeGen *codegen, ast::Identifier name, ast::Type *type)
      : ReferenceVT(codegen, name, type), name_(name) {}

  /**
   * Declare a variable with the given name.
   * @param codegen The code generator instance.
   * @param name The name of the variable.
   * @param type The type of the variable.
   */
  VariableVT(CodeGen *codegen, std::string_view name, ast::Type *type)
      : VariableVT(codegen, codegen->MakeFreshIdentifier(name), type) {}

  /**
   * @return The name of this variable.
   */
  ast::Identifier GetName() const { return name_; }

 private:
  // The name of the variable. Immutable by design.
  const ast::Identifier name_;
};

/**
 * Create a NIL value.
 * @param codegen The code generator.
 * @param type The type of NIL to create.
 * @return A typed NIL value.
 */
inline ValueVT Nil(CodeGen *codegen, ast::Type *type) {
  // TODO(pmenon): HACK HACK HACK
  auto nil = codegen->Nil();
  nil->SetType(type);
  return ValueVT(codegen, nil);
}

/**
 * Declare the given value.
 * @param var The variable to declare.
 * @return A void value representing the declaration.
 */
inline Value<void> Declare(const VariableVT &var) {
  return Value<void>(var.GetCodeGen()->DeclareVar(var.GetName(), var.GetType()));
}

/**
 * Declare the given variable using the @em value as the initial value.
 * @param var The variable to declare.
 * @param value The initial value.
 * @return A void value representing the declaration.
 */
inline Value<void> Declare(const VariableVT &var, const ValueVT &value) {
  TPL_ASSERT(var.GetType() == value.GetType(), "Type mismatch in declaration!");
  CodeGen *codegen = var.GetCodeGen();
  return Value<void>(codegen->DeclareVarWithInit(var.GetName(), value.GetRaw()));
}

/**
 * Assign the given value @em rhs to the reference @em lhs.
 * @param lhs The destination of the assignment.
 * @param rhs The source value of the assignment.
 */
inline Value<void> Assign(const ReferenceVT &lhs, const ValueVT &rhs) {
  return Value<void>(lhs.GetCodeGen()->Assign(lhs.GetRaw(), rhs.GetRaw()));
}

/**
 * @return The arithmetic negation of the input value.
 */
inline ValueVT Negate(const ValueVT &input) {
  CodeGen *codegen = input.GetCodeGen();
  return ValueVT(codegen, codegen->Neg(input.GetRaw()));
}

/**
 * Perform an binary arithmetic operation on @em lhs and @rhs and return the result.
 * @param op The operation to perform.
 * @param lhs The left input to the operation.
 * @param rhs The right input to the operation.
 * @return The result of the operation.
 */
inline ValueVT ArithmeticOp(parsing::Token::Type op, const ValueVT &lhs, const ValueVT &rhs) {
  CodeGen *codegen = lhs.GetCodeGen();
  return ValueVT(codegen, codegen->ArithmeticOp(op, lhs.GetRaw(), rhs.GetRaw()));
}

/**
 * Perform the binary comparison @em op between @em lhs and @em rhs, returning a boolean result.
 * @pre The provided operation must be a comparison between compatible types, otherwise an assertion
 * is raised.
 * @param op The comparison to perform.
 * @param lhs The left input to the comparison.
 * @param rhs The right input to the comparison.
 * @return The boolean result of the comparison.
 */
inline Value<bool> ComparisonOp(parsing::Token::Type op, const ValueVT &lhs, const ValueVT &rhs) {
  CodeGen *codegen = lhs.GetCodeGen();
  return Value<bool>(codegen, codegen->ComparisonOp(op, lhs.GetRaw(), rhs.GetRaw()));
}

/**
 * @return True if the input pointer is nil; false otherwise.
 */
inline Value<bool> IsNilPtr(const ValueVT &ptr) {
  CodeGen *codegen = ptr.GetCodeGen();
  return Value<bool>(codegen, codegen->CompareEq(ptr.GetRaw(), codegen->Nil()));
}

/**
 * Re-cast the pointer input @em ptr to the type @em type. Both inputs must be pointer types.
 * @pre The type of the input and the target type must both be pointers.
 * @tparam T The type of the pointer to cast.
 * @param type The (pointer) type to cast the input into.
 * @param ptr The pointer value to case.
 * @return The result of the cast operation.
 */
template <typename T, typename = std::enable_if_t<std::is_pointer_v<T>>>
inline ValueVT PtrCast(ast::Type *type, const Value<T> &ptr) {
  CodeGen *codegen = ptr.GetCodeGen();
  return ValueVT(codegen, codegen->PtrCast(type, ptr.GetRaw()));
}

/**
 * Re-cast the pointer input @em ptr to the type @em type. Both inputs must be pointer types.
 * @pre The type of the input and the target type must both be pointers.
 * @tparam T The type of the pointer to cast.
 * @param type The (pointer) type to cast the input into.
 * @param ptr The pointer value to case.
 * @return The result of the cast operation.
 */
inline ValueVT PtrCast(ast::Type *type, const ValueVT &ptr) {
  CodeGen *codegen = ptr.GetCodeGen();
  return ValueVT(codegen, codegen->PtrCast(type, ptr.GetRaw()));
}

/**
 * Re-cast the pointer input @em ptr to the templated type. Both inputs must be pointer types.
 * @pre The type of the input and the target type must both be pointers.
 * @tparam T The type of the pointer to cast.
 * @param type The (pointer) type to cast the input into.
 * @param ptr The pointer value to case.
 * @return The result of the cast operation.
 */
template <typename T, typename = std::enable_if_t<std::is_pointer_v<T>>>
inline Value<T> PtrCast(const ValueVT &ptr) {
  CodeGen *codegen = ptr.GetCodeGen();
  return Value<T>(codegen, codegen->PtrCast(codegen->template GetType<T>(), ptr.GetRaw()));
}

/**
 * Perform an integer cast from the input @em val to the template type T.
 * @tparam T The target (integer) type to cast to.
 * @param val The integral value to cast.
 * @return The result of the cast.
 */
template <typename T>
inline Value<T> IntCast(const ValueVT &val) {
  CodeGen *codegen = val.GetCodeGen();
  return Value<T>(codegen, codegen->IntCast(codegen->GetType<T>(), val.GetRaw()));
}

}  // namespace tpl::sql::codegen::edsl
