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
inline Value<bool> operator==(const Value<T> &lhs, const Value<T> &rhs) {
  return Value<bool>(lhs.GetCodeGen(), lhs.GetCodeGen()->CompareEq(lhs.GetRaw(), rhs.GetRaw()));
}

/**
 * Check if the provided pointer value @em lhs is a NULL pointer.
 * @tparam T The type of the element being compared.
 * @param lhs The left input to the comparison.
 * @return True if the left input is a NULL pointer; false otherwise.
 */
template <typename T, typename = std::enable_if_t<std::is_pointer_v<T>>>
inline Value<bool> operator==(const Value<T> &lhs, std::nullptr_t) {
  CodeGen *codegen = lhs.GetCodeGen();
  return Value<bool>(codegen, codegen->CompareEq(lhs.GetRaw(), codegen->Nil()));
}

/**
 * Check if the provided pointer value @em rhs is a NULL pointer.
 * @tparam T The type of the element being compared.
 * @param rhs The right input to the comparison.
 * @return True if the right input is a NULL pointer; false otherwise.
 */
template <typename T, typename = std::enable_if_t<std::is_pointer_v<T>>>
inline Value<bool> operator==(std::nullptr_t, const Value<T> &rhs) {
  return rhs == nullptr;
}

/**
 * Perform an inequality comparison of @em lhs and @em rhs.
 * @tparam T The type of the element being compared.
 * @param lhs The left input to the comparison.
 * @param rhs The right input to the comparison.
 * @return The result of the comparison.
 */
template <typename T, typename = std::enable_if_t<traits::supports_equal<T>::value>>
inline Value<bool> operator!=(const Value<T> &lhs, const Value<T> &rhs) {
  return Value<bool>(lhs.GetCodeGen(), lhs.GetCodeGen()->CompareNe(lhs.GetRaw(), rhs.GetRaw()));
}

/**
 * Check if the provided pointer value @em lhs is not a NULL pointer.
 * @tparam T The pointer type of the element being compared.
 * @param lhs The left input to the comparison.
 * @return True if the left input is not a NULL pointer; false otherwise.
 */
template <typename T, typename = std::enable_if_t<std::is_pointer_v<T>>>
inline Value<bool> operator!=(const Value<T> &lhs, std::nullptr_t) {
  CodeGen *codegen = lhs.GetCodeGen();
  return Value<bool>(codegen, codegen->CompareNe(lhs.GetRaw(), codegen->Nil()));
}

/**
 * Check if the provided pointer value @em rhs is not a NULL pointer.
 * @tparam T The pointer type of the element being compared.
 * @param rhs The right input to the comparison.
 * @return True if the right input is not a NULL pointer; false otherwise.
 */
template <typename T, typename = std::enable_if_t<std::is_pointer_v<T>>>
inline Value<bool> operator!=(std::nullptr_t, const Value<T> &rhs) {
  return rhs != nullptr;
}

/**
 * Perform a less-than comparison of @em lhs and @em rhs.
 * @tparam T The type of the element being compared.
 * @param lhs The left input to the comparison.
 * @param rhs The right input to the comparison.
 * @return The result of the comparison.
 */
template <typename T, typename = std::enable_if_t<traits::supports_greater<T>::value>>
inline Value<bool> operator<(const Value<T> &lhs, const Value<T> &rhs) {
  return Value<bool>(lhs.GetCodeGen(), lhs.GetCodeGen()->CompareLt(lhs.GetRaw(), rhs.GetRaw()));
}

/**
 * Perform a less-than comparison of the value @em lhs and the right literal @em val.
 * @tparam T The type of the element being compared.
 * @param lhs The left input to the comparison.
 * @param val The right literal to the comparison.
 * @return The result of the comparison.
 */
template <typename T, typename = std::enable_if_t<traits::supports_greater<T>::value>>
inline Value<bool> operator<(const Value<T> &lhs, T val) {
  return lhs < Literal<T>(lhs.GetCodeGen(), val);
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
inline Value<bool> operator<=(const Value<T> &lhs, const Value<T> &rhs) {
  return Value<bool>(lhs.GetCodeGen(), lhs.GetCodeGen()->CompareLe(lhs.GetRaw(), rhs.GetRaw()));
}

/**
 * Perform a less-than-or-equal-to comparison of the value @em lhs and the right literal @em val.
 * @tparam T The type of the element being compared.
 * @param lhs The left input to the comparison.
 * @param rhs The right literal to the comparison.
 * @return The result of the comparison.
 */
template <typename T, typename = std::enable_if_t<traits::supports_greater<T>::value &&
                                                  traits::supports_equal<T>::value>>
inline Value<bool> operator<=(const Value<T> &lhs, T rhs) {
  return lhs <= Literal<T>(lhs.GetCodeGen(), rhs);
}

/**
 * Perform a greater-than comparison of @em lhs and @em rhs.
 * @tparam T The type of the element being compared.
 * @param lhs The left input to the comparison.
 * @param rhs The right input to the comparison.
 * @return The result of the comparison.
 */
template <typename T, typename = std::enable_if_t<traits::supports_greater<T>::value>>
inline Value<bool> operator>(const Value<T> &lhs, const Value<T> &rhs) {
  return Value<bool>(lhs.GetCodeGen(), lhs.GetCodeGen()->CompareGt(lhs.GetRaw(), rhs.GetRaw()));
}

/**
 * Perform a greater-than comparison of the value @em lhs and the right literal @em val.
 * @tparam T The type of the element being compared.
 * @param lhs The left input to the comparison.
 * @param rhs The right literal to the comparison.
 * @return The result of the comparison.
 */
template <typename T, typename = std::enable_if_t<traits::supports_greater<T>::value>>
inline Value<bool> operator>(const Value<T> &lhs, T rhs) {
  return lhs > Literal<T>(lhs.GetCodeGen(), rhs);
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
inline Value<bool> operator>=(const Value<T> &lhs, const Value<T> &rhs) {
  return Value<bool>(lhs.GetCodeGen(), lhs.GetCodeGen()->CompareGe(lhs.GetRaw(), rhs.GetRaw()));
}

/**
 * Perform a greater-than-or-equal-to comparison of the value @em lhs and the right literal @em val.
 * @tparam T The type of the element being compared.
 * @param lhs The left input to the comparison.
 * @param rhs The right literal to the comparison.
 * @return The result of the comparison.
 */
template <typename T, typename = std::enable_if_t<traits::supports_greater<T>::value &&
                                                  traits::supports_equal<T>::value>>
inline Value<bool> operator>=(const Value<T> &lhs, T rhs) {
  return lhs >= Literal<T>(lhs.GetCodeGen(), rhs);
}

}  // namespace tpl::sql::codegen::edsl
