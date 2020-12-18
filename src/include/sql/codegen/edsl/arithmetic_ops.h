#pragma once

#include "sql/codegen/edsl/traits.h"
#include "sql/codegen/edsl/value.h"

namespace tpl::sql::codegen::edsl {

/**
 * Compute the addition of @em lhs and @em rhs.
 * @tparam T The type of the element being added.
 * @param lhs The left input to the addition.
 * @param rhs The right input to the addition.
 * @return The result of the addition.
 */
template <typename T, typename = std::enable_if_t<traits::supports_addsubmul<T>::value>>
Value<T> operator+(const Value<T> &lhs, const Value<T> &rhs) {
  return Value<T>(lhs.GetCodeGen(), lhs.GetCodeGen()->Add(lhs.GetRaw(), rhs.GetRaw()));
}

/**
 * Compute the subtraction of @em lhs and @em rhs.
 * @tparam T The type of the element being subtracted.
 * @param lhs The left input to the subtraction.
 * @param rhs The right input to the subtraction.
 * @return The result of the subtraction.
 */
template <typename T, typename = std::enable_if_t<traits::supports_addsubmul<T>::value>>
Value<T> operator-(const Value<T> &lhs, const Value<T> &rhs) {
  return Value<T>(lhs.GetCodeGen(), lhs.GetCodeGen()->Sub(lhs.GetRaw(), rhs.GetRaw()));
}

/**
 * Compute the multiplication of @em lhs and @em rhs.
 * @tparam T The type of the element being multiplied.
 * @param lhs The left input to the multiplication.
 * @param rhs The right input to the multiplication.
 * @return The result of the multiplication.
 */
template <typename T, typename = std::enable_if_t<traits::supports_addsubmul<T>::value>>
Value<T> operator*(const Value<T> &lhs, const Value<T> &rhs) {
  return Value<T>(lhs.GetCodeGen(), lhs.GetCodeGen()->Mul(lhs.GetRaw(), rhs.GetRaw()));
}

/**
 * Compute the division of @em lhs and @em rhs.
 * @tparam T The type of the element being divided.
 * @param lhs The left input to the division.
 * @param rhs The right input to the division.
 * @return The result of the division.
 */
template <typename T, typename = std::enable_if_t<traits::supports_div<T>::value>>
Value<T> operator/(const Value<T> &lhs, const Value<T> &rhs) {
  return Value<T>(lhs.GetCodeGen(), lhs.GetCodeGen()->Div(lhs.GetRaw(), rhs.GetRaw()));
}

/**
 * Compute the modulo remainder of @em lhs and @em rhs.
 * @tparam T The type of the element being operated on.
 * @param lhs The left input to the modulo operation.
 * @param rhs The right input to the modulo operation.
 * @return The result of the modulo.
 */
template <typename T, typename = std::enable_if_t<traits::supports_modulo<T>::value>>
Value<T> operator%(const Value<T> &lhs, const Value<T> &rhs) {
  return Value<T>(lhs.GetCodeGen(), lhs.GetCodeGen()->Mod(lhs.GetRaw(), rhs.GetRaw()));
}

}  // namespace tpl::sql::codegen::edsl
