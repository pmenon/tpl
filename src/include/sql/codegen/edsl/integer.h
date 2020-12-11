#pragma once

#include "sql/codegen/edsl/builtin_expression.h"
#include "sql/codegen/edsl/fwd.h"
#include "sql/codegen/edsl/literal.h"
#include "sql/codegen/edsl/traits.h"
#include "sql/codegen/edsl/value.h"
#include "sql/codegen/function_builder.h"

namespace tpl::sql::codegen::edsl {

/**
 * The base type for any integer-based value.
 * @tparam T The meta-type of the integer value.
 */
template <typename T, typename _CppType>
class IntegerBase : public Value<T> {
 public:
  /**
   * The base type.
   */
  using Base = Value<T>;

  /**
   * The ETL expression type.
   */
  using ValueType = T;

  /**
   * The raw C++ type that can be assigned to this value.
   */
  using CppType = _CppType;

  /**
   * Bring in base overloads.
   */
  using Base::Eval;
  using Base::GetCodeGen;
  using Base::operator=;

  /**
   * Assign the given raw C++ value to this value.
   * @param val The value to assign.
   * @return This integer.
   */
  ValueType &operator=(CppType val) { return (*Derived() = Literal<ValueType>(GetCodeGen(), val)); }

  /**
   * Pre-increment this integer by one.
   * @return This integer.
   */
  ValueType &operator++() {
    CodeGen *codegen = GetCodeGen();
    FunctionBuilder *function = codegen->GetCurrentFunction();
    function->Append(codegen->Assign(Eval(), (*Derived() + 1).Eval()));
    return *Derived();
  }

  /**
   * Generate all the common operation-assignment ops, e.g., +=, -=, *=, /=, %=, >>=, <<=, etc.
   */
#define GEN_ASSIGN_OP(OP)                                                                         \
  /**                                                                                             \
   * Assignment operation with a C++ value. Convert to a Literal expression and dispatch to       \
   * version using generic ETL expression.                                                        \
   * @param val The C++ value to assign this integer.                                             \
   * @return This updated integer.                                                                \
   */                                                                                             \
  ValueType &operator OP##=(CppType val) {                                                        \
    return (*Derived() OP## = Literal<ValueType>(GetCodeGen(), val));                             \
  }                                                                                               \
                                                                                                  \
  /**                                                                                             \
   * Assignment operation with a generic ETL expression. The template guard ensures we're         \
   * assigning compatible types. In this context "compatible" means the ETL value type of the     \
   * expression must exactly match the type of the this integer. No implicit casting is           \
   * performed.                                                                                   \
   * @tparam E The ETL expression type. The value type must match this integer.                   \
   * @param val The value to assign this integer.                                                 \
   */                                                                                             \
  template <typename E, typename = std::enable_if_t<IsETLExpr<E> && SameValueType<ValueType, E>>> \
  ValueType &operator OP##=(E &&val) {                                                            \
    CodeGen *codegen = GetCodeGen();                                                              \
    FunctionBuilder *function = codegen->GetCurrentFunction();                                    \
    function->Append(codegen->Assign(Eval(), (*Derived() OP val).Eval()));                        \
    return *Derived();                                                                            \
  }

  GEN_ASSIGN_OP(+)
  GEN_ASSIGN_OP(-)
  GEN_ASSIGN_OP(*)
  GEN_ASSIGN_OP(/)
  GEN_ASSIGN_OP(%)
  GEN_ASSIGN_OP(>>)
  GEN_ASSIGN_OP(<<)
  GEN_ASSIGN_OP(&)
  GEN_ASSIGN_OP(|)

#undef GEN_ASSIGN_OP

  /**
   * Case this integer to the provided target type, also an integer type.
   * @tparam Target The integer type to cast to.
   * @return The result of the integer cast.
   */
  template <typename Target, typename = std::enable_if_t<trait_details::IsInteger<Target>::value>>
  auto Cast() {
    // If the target type matches the current type, optimize into no-op.
    if constexpr (std::is_same_v<Target, ValueType>) {
      return *Derived();
    } else {
      return BuiltinExpression<Target, ValueType>(ast::Builtin::IntCast, *Derived());
    }
  }

 private:
  /** @return The derived type.  */
  ValueType *Derived() { return static_cast<ValueType *>(this); }

  /** @return The derived type.  */
  const ValueType *Derived() const { return static_cast<const ValueType *>(this); }

 protected:
  // Declare a variable with the given expression value.
  template <typename E, typename = std::enable_if_t<IsETLExpr<E>>>
  IntegerBase(CodeGen *codegen, ast::Identifier name, E &&val)
      : Base(codegen, name, std::forward<E>(val)) {}

  // Declare a variable with the given raw C++ value.
  IntegerBase(CodeGen *codegen, ast::Identifier name, CppType val)
      : Base(codegen, name, Literal<ValueType>(codegen, val)) {}
};

#define GEN_INTEGER_TYPE(Class, _CppType)                                                       \
  class Class : public IntegerBase<Class, _CppType> {                                           \
   public:                                                                                      \
    using Base = IntegerBase<Class, _CppType>;                                                  \
    using Base::Eval;                                                                           \
    using Base::GetCodeGen;                                                                     \
    using Base::operator=;                                                                      \
    template <typename E, typename = std::enable_if_t<IsETLExpr<E> && SameValueType<Class, E>>> \
    Class(CodeGen *codegen, ast::Identifier name, E &&val)                                      \
        : Base(codegen, name, std::forward<E>(val)) {}                                          \
    template <typename E, typename = std::enable_if_t<IsETLExpr<E> && SameValueType<Class, E>>> \
    Class(CodeGen *codegen, std::string_view name, E &&val)                                     \
        : Base(codegen, codegen->MakeFreshIdentifier(name), std::forward<E>(val)) {}            \
    Class(CodeGen *codegen, ast::Identifier name, _CppType val) : Base(codegen, name, val) {}   \
    Class(CodeGen *codegen, std::string_view name, _CppType val)                                \
        : Base(codegen, codegen->MakeFreshIdentifier(name), val) {}                             \
  };                                                                                            \
  template <>                                                                                   \
  struct Traits<Class> {                                                                        \
    /** The EDSL value type of the expression. */                                               \
    using ValueType = Class;                                                                    \
    /** The raw C++ type of the expression. */                                                  \
    using CppType = _CppType;                                                                   \
    /** Indicates if T is an ETL type. */                                                       \
    static constexpr bool kIsETL = true;                                                        \
  };

GEN_INTEGER_TYPE(UInt8, uint8_t)
GEN_INTEGER_TYPE(UInt16, uint16_t)
GEN_INTEGER_TYPE(UInt32, uint32_t)
GEN_INTEGER_TYPE(UInt64, uint64_t)
GEN_INTEGER_TYPE(Int8, int8_t)
GEN_INTEGER_TYPE(Int16, int16_t)
GEN_INTEGER_TYPE(Int32, int32_t)
GEN_INTEGER_TYPE(Int64, int64_t)

#undef GEN_INTEGER_TYPE

}  // namespace tpl::sql::codegen::edsl
