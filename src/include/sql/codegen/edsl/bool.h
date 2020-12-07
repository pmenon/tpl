#pragma once

#include <type_traits>
#include <variant>

#include "sql/codegen/codegen.h"
#include "sql/codegen/edsl/literal.h"
#include "sql/codegen/edsl/traits.h"
#include "sql/codegen/edsl/value.h"
#include "sql/codegen/function_builder.h"

namespace tpl::sql::codegen::edsl {

/**
 * The base type for any integer-based value.
 * @tparam T The meta-type of the integer value.
 */
class Bool : public Value<Bool> {
 public:
  /**
   * The base type.
   */
  using Base = Value<Bool>;

  /**
   * The raw C++ type that can be assigned to this value.
   */
  using CppType = bool;

  /**
   * Create an unnamed boolean whose value is the result of the given expression.
   * @tparam E The expression type.
   * @param val The value.
   */
  template <typename E,
            typename = std::enable_if_t<Traits<E>::kIsETL && std::is_same_v<Bool, ValueT<E>>>>
  Bool(E &&val) : Base(std::forward<E>(val)) {}

  /**
   * Create a named boolean with the given name whose value is the result of the given expression.
   * @tparam E The expression type.
   * @param codegen The code generator instance.
   * @param name The name of the boolean.
   * @param val The initial value.
   */
  template <typename E,
            typename = std::enable_if_t<Traits<E>::kIsETL && std::is_same_v<Bool, ValueT<E>>>>
  Bool(CodeGen *codegen, std::string_view name, E &&val)
      : Bool(codegen, codegen->MakeFreshIdentifier(name), std::forward<E>(val)) {}

  /**
   * Create a named boolean with the given name whose value is the result of the given expression.
   * @tparam E The expression type.
   * @param codegen The code generator instance.
   * @param name The name of the boolean.
   * @param val The initial value.
   */
  Bool(CodeGen *codegen, ast::Identifier name, bool val)
      : Base(codegen, name, Literal<Bool>(codegen, val)) {}

  /**
   * Assign a raw C++ boolean value to this.
   * @param val The raw C++ value.
   * @return This value.
   */
  Bool &operator=(CppType val) { return this->operator=(Literal<Bool>(GetCodeGen(), val)); }
};

}  // namespace tpl::sql::codegen::edsl
