#pragma once

#include "sql/codegen/edsl/builtin_op.h"
#include "sql/codegen/edsl/element_base.h"
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
  ValueType &operator=(CppType val) { return this->operator=(Literal<T>(GetCodeGen(), val)); }

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

  template <typename Target, typename = std::enable_if_t<trait_details::IsInteger<Target>::value>>
  auto Cast() {
    return BuiltinOperation<Target, ValueType>(ast::Builtin::IntCast, *Derived());
  }

 private:
  ValueType *Derived() { return static_cast<ValueType *>(this); }
  const ValueType *Derived() const { return static_cast<const ValueType *>(this); }

 protected:
  // Declare a variable with the given expression value.
  template <typename E, typename = std::enable_if_t<Traits<E>::kIsETL>>
  IntegerBase(CodeGen *codegen, ast::Identifier name, E &&val)
      : Base(codegen, name, std::forward<E>(val)) {}

  // Declare a variable with the given raw C++ value.
  IntegerBase(CodeGen *codegen, ast::Identifier name, CppType val)
      : Base(codegen, name, Literal<T>(codegen, val)) {}
};

#define GEN_INTEGER_TYPE(Class, _CppType)                                                     \
  class Class : public IntegerBase<Class, _CppType> {                                         \
   public:                                                                                    \
    using Base = IntegerBase<Class, _CppType>;                                                \
    using Base::Eval;                                                                         \
    using Base::operator=;                                                                    \
    template <typename E, typename = std::enable_if<Traits<E>::kIsETL>>                       \
    Class(CodeGen *codegen, ast::Identifier name, E &&val)                                    \
        : Base(codegen, name, std::forward<E>(val)) {}                                        \
    template <typename E, typename = std::enable_if<Traits<E>::kIsETL>>                       \
    Class(CodeGen *codegen, std::string_view name, E &&val)                                   \
        : Base(codegen, codegen->MakeFreshIdentifier(name), std::forward<E>(val)) {}          \
    Class(CodeGen *codegen, ast::Identifier name, _CppType val) : Base(codegen, name, val) {} \
    Class(CodeGen *codegen, std::string_view name, _CppType val)                              \
        : Base(codegen, codegen->MakeFreshIdentifier(name), val) {}                           \
  };                                                                                          \
  template <>                                                                                 \
  struct Traits<Class> {                                                                      \
    /** The EDSL value type of the expression. */                                             \
    using ValueType = Class;                                                                  \
    /** The raw C++ type of the expression. */                                                \
    using CppType = _CppType;                                                                 \
    /** Indicates if T is an ETL type. */                                                     \
    static constexpr bool kIsETL = true;                                                      \
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
