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
    TPL_ASSERT(val->GetType() != nullptr, "Value MUST have a type at the point of instantiation!");
  }

  /**
   * Construct a variable-typed value from the strongly typed value.
   * @tparam T The type of the value.
   * @param v The value to wrap.
   */
  template <typename T>
  ValueVT(const Value<T> &v) : val_(v.GetRaw()) {}

  /**
   * @return True if this value has a type and its type is equivalent to the provided type.
   */
  bool HasType(ast::Type *type) const noexcept { return val_->GetType() == type; }

  /**
   * @return The type of this value.
   */
  ast::Type *GetType() const noexcept { return val_->GetType(); }

  /**
   * @return True if the type of this value is a struct; false otherwise.
   */
  bool IsStructType() const noexcept { return GetType()->IsStructType(); }

  /**
   * @return True if the type of this value is a pointer; false otherwise;
   */
  bool IsPointerType() const noexcept { return GetType()->IsPointerType(); }

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
  operator Value<T>() const {
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
  ReferenceVT(CodeGen *codegen, ast::Expression *val) : ValueVT(codegen, codegen->Deref(val)) {
    TPL_ASSERT(val->GetType() != nullptr, "Value being referenced must have a type!");
    TPL_ASSERT(val->GetType()->IsPointerType(), "Value being referenced isn't a pointer!");
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
    throw CodeGenerationException("Value is not an array type; cannot use array access!");
  }
  return ReferenceVT(codegen_, codegen_->ArrayAccess(val_, index.GetRaw()));
}

template <typename I, std::enable_if_t<traits::is_primitive_int_type<I>::value> *>
inline ReferenceVT ValueVT::operator[](I index) const {
  if (!GetType()->IsArrayType()) {
    throw CodeGenerationException("Value is not an array type; cannot use array access!");
  }
  return ReferenceVT(codegen_, codegen_->ArrayAccess(val_, codegen_->Const32(index)));
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
      : VariableVT(codegen, codegen->MakeIdentifier(name), type) {}

  /**
   * @return The name of this variable.
   */
  ast::Identifier GetName() const { return name_; }

 private:
  // The name of the variable. Immutable by design.
  const ast::Identifier name_;
};

/**
 * Assign the given value @em rhs to the reference @em lhs.
 * @param lhs The destination of the assignment.
 * @param rhs The source value of the assignment.
 */
inline void Assign(const ReferenceVT &lhs, const ValueVT &rhs) {
  CodeGen *codegen = lhs.GetCodeGen();
  codegen->GetCurrentFunction()->Append(codegen->Assign(lhs.GetRaw(), rhs.GetRaw()));
}

}  // namespace tpl::sql::codegen::edsl
