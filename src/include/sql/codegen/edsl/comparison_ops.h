#pragma once

#include "sql/codegen/edsl/traits.h"
#include "sql/codegen/edsl/value.h"

namespace tpl::sql::codegen::edsl {

/**
 * Perform an equality comparison of @em lhs and @em rhs.
 * @tparam T The type of the element being compared.
 * @param lhs The left input to the comparison.
 * @param rhs The right input to the comparison.
 * @return The result of the comparison.
 */
template <typename T, typename = std::enable_if_t<traits::supports_equal<T>::value>>
Value<bool> operator==(const Value<T> &lhs, const Value<T> &rhs) {
  return Value<bool>(lhs.GetCodeGen(), lhs.GetCodeGen()->CompareEq(lhs.GetRaw(), rhs.GetRaw()));
}

/**
 * Perform an inequality comparison of @em lhs and @em rhs.
 * @tparam T The type of the element being compared.
 * @param lhs The left input to the comparison.
 * @param rhs The right input to the comparison.
 * @return The result of the comparison.
 */
template <typename T, typename = std::enable_if_t<traits::supports_equal<T>::value>>
Value<bool> operator!=(const Value<T> &lhs, const Value<T> &rhs) {
  return Value<bool>(lhs.GetCodeGen(), lhs.GetCodeGen()->CompareNe(lhs.GetRaw(), rhs.GetRaw()));
}

/**
 * Perform a less-than comparison of @em lhs and @em rhs.
 * @tparam T The type of the element being compared.
 * @param lhs The left input to the comparison.
 * @param rhs The right input to the comparison.
 * @return The result of the comparison.
 */
template <typename T, typename = std::enable_if_t<traits::supports_greater<T>::value>>
Value<bool> operator<(const Value<T> &lhs, const Value<T> &rhs) {
  return Value<bool>(lhs.GetCodeGen(), lhs.GetCodeGen()->CompareLt(lhs.GetRaw(), rhs.GetRaw()));
}

/**
 * Perform a less-than-or-equal-to comparison of @em lhs and @em rhs.
 * @tparam T The type of the element being compared.
 * @param lhs The left input to the comparison.
 * @param rhs The right input to the comparison.
 * @return The result of the comparison.
 */
template <typename T, typename = std::enable_if_t<traits::supports_greater<T>::value &&
                                                  traits::supports_equal<T>::value>>
Value<bool> operator<=(const Value<T> &lhs, const Value<T> &rhs) {
  return Value<bool>(lhs.GetCodeGen(), lhs.GetCodeGen()->CompareLe(lhs.GetRaw(), rhs.GetRaw()));
}

/**
 * Perform a greater-than comparison of @em lhs and @em rhs.
 * @tparam T The type of the element being compared.
 * @param lhs The left input to the comparison.
 * @param rhs The right input to the comparison.
 * @return The result of the comparison.
 */
template <typename T, typename = std::enable_if_t<traits::supports_greater<T>::value>>
Value<bool> operator>(const Value<T> &lhs, const Value<T> &rhs) {
  return Value<bool>(lhs.GetCodeGen(), lhs.GetCodeGen()->CompareGt(lhs.GetRaw(), rhs.GetRaw()));
}

/**
 * Perform a greater-than-or-equal-to comparison of @em lhs and @em rhs.
 * @tparam T The type of the element being compared.
 * @param lhs The left input to the comparison.
 * @param rhs The right input to the comparison.
 * @return The result of the comparison.
 */
template <typename T, typename = std::enable_if_t<traits::supports_greater<T>::value &&
                                                  traits::supports_equal<T>::value>>
Value<bool> operator>=(const Value<T> &lhs, const Value<T> &rhs) {
  return Value<bool>(lhs.GetCodeGen(), lhs.GetCodeGen()->CompareGe(lhs.GetRaw(), rhs.GetRaw()));
}

}  // namespace tpl::sql::codegen::edsl
