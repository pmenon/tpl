#pragma once

#include "sql/codegen/edsl/traits.h"
#include "sql/codegen/edsl/value.h"

namespace tpl::sql::codegen::edsl {

/**
 * Compute the arithmetic negation of the input @em input.
 * @tparam T The type of the element being negated.
 * @param input The input to the negation.
 * @return The result of the negation.
 */
template <typename T, typename = std::enable_if_t<traits::supports_addsubmul<T>::value>>
inline Value<T> operator-(const Value<T> &input) {
  CodeGen *codegen = input.GetCodeGen();
  return Value<T>(codegen, codegen->Neg(input.GetRaw()));
}

/**
 * Compute the addition of @em lhs and @em rhs.
 * @tparam T The type of the element being added.
 * @param lhs The left input to the addition.
 * @param rhs The right input to the addition.
 * @return The result of the addition.
 */
template <typename T, typename = std::enable_if_t<traits::supports_addsubmul<T>::value>>
inline Value<T> operator+(const Value<T> &lhs, const Value<T> &rhs) {
  CodeGen *codegen = lhs.GetCodeGen();
  return Value<T>(codegen, codegen->Add(lhs.GetRaw(), rhs.GetRaw()));
}

/**
 * Compute the addition of @em lhs and the literal value @em rhs.
 * @tparam T The type of the element being added.
 * @param lhs The left input to the addition.
 * @param rhs The right input to the addition.
 * @return The result of the addition.
 */
template <typename T, typename = std::enable_if_t<traits::supports_addsubmul<T>::value>>
inline Value<T> operator+(const Value<T> &lhs, T rhs) {
  return lhs + Literal<T>(lhs.GetCodeGen(), rhs);
}

/**
 * Compute the addition of the literal value @em lhs and @em rhs.
 * @tparam T The type of the element being added.
 * @param lhs The left input to the addition.
 * @param rhs The right input to the addition.
 * @return The result of the addition.
 */
template <typename T, typename = std::enable_if_t<traits::supports_addsubmul<T>::value>>
inline Value<T> operator+(T lhs, const Value<T> &rhs) {
  return rhs + Literal<T>(rhs.GetCodeGen(), lhs);
}

/**
 * Compute the subtraction of @em lhs and @em rhs.
 * @tparam T The type of the element being subtracted.
 * @param lhs The left input to the subtraction.
 * @param rhs The right input to the subtraction.
 * @return The result of the subtraction.
 */
template <typename T, typename = std::enable_if_t<traits::supports_addsubmul<T>::value>>
inline Value<T> operator-(const Value<T> &lhs, const Value<T> &rhs) {
  CodeGen *codegen = lhs.GetCodeGen();
  return Value<T>(codegen, codegen->Sub(lhs.GetRaw(), rhs.GetRaw()));
}

/**
 * Compute the subtraction of @em lhs and the literal value @em rhs.
 * @tparam T The type of the element being subtracted.
 * @param lhs The left input to the subtraction.
 * @param rhs The right input to the subtraction.
 * @return The result of the subtraction.
 */
template <typename T, typename = std::enable_if_t<traits::supports_addsubmul<T>::value>>
inline Value<T> operator-(const Value<T> &lhs, T rhs) {
  return lhs - Literal<T>(lhs.GetCodeGen(), rhs);
}

/**
 * Compute the subtraction of the literal value @em lhs and @em rhs.
 * @tparam T The type of the element being subtracted.
 * @param lhs The left input to the subtraction.
 * @param rhs The right input to the subtraction.
 * @return The result of the subtraction.
 */
template <typename T, typename = std::enable_if_t<traits::supports_addsubmul<T>::value>>
inline Value<T> operator-(T lhs, const Value<T> &rhs) {
  return Literal<T>(rhs.GetCodeGen(), lhs) - rhs;
}

/**
 * Compute the multiplication of @em lhs and @em rhs.
 * @tparam T The type of the element being multiplied.
 * @param lhs The left input to the multiplication.
 * @param rhs The right input to the multiplication.
 * @return The result of the multiplication.
 */
template <typename T, typename = std::enable_if_t<traits::supports_addsubmul<T>::value>>
inline Value<T> operator*(const Value<T> &lhs, const Value<T> &rhs) {
  CodeGen *codegen = lhs.GetCodeGen();
  return Value<T>(codegen, codegen->Mul(lhs.GetRaw(), rhs.GetRaw()));
}

/**
 * Compute the multiplication of @em lhs and the literal value @em rhs.
 * @tparam T The type of the element being multiplied.
 * @param lhs The left input to the multiplication.
 * @param rhs The right input to the multiplication.
 * @return The result of the multiplication.
 */
template <typename T, typename = std::enable_if_t<traits::supports_addsubmul<T>::value>>
inline Value<T> operator*(const Value<T> &lhs, T rhs) {
  return lhs * Literal<T>(lhs.GetCodeGen(), rhs);
}

/**
 * Compute the multiplication of the literal value @em lhs and @em rhs.
 * @tparam T The type of the element being multiplied.
 * @param lhs The left input to the multiplication.
 * @param rhs The right input to the multiplication.
 * @return The result of the multiplication.
 */
template <typename T, typename = std::enable_if_t<traits::supports_addsubmul<T>::value>>
inline Value<T> operator*(T lhs, const Value<T> &rhs) {
  return rhs * Literal<T>(rhs.GetCodeGen(), lhs);
}

/**
 * Compute the division of @em lhs and @em rhs.
 * @tparam T The type of the element being divided.
 * @param lhs The left input to the division.
 * @param rhs The right input to the division.
 * @return The result of the division.
 */
template <typename T, typename = std::enable_if_t<traits::supports_div<T>::value>>
inline Value<T> operator/(const Value<T> &lhs, const Value<T> &rhs) {
  CodeGen *codegen = lhs.GetCodeGen();
  return Value<T>(codegen, codegen->Div(lhs.GetRaw(), rhs.GetRaw()));
}

/**
 * Compute the division of @em lhs and the literal value @em rhs.
 * @tparam T The type of the element being divided.
 * @param lhs The left input to the division.
 * @param rhs The right input to the division.
 * @return The result of the division.
 */
template <typename T, typename = std::enable_if_t<traits::supports_div<T>::value>>
inline Value<T> operator/(const Value<T> &lhs, T rhs) {
  TPL_ASSERT(rhs != 0, "Division by zero.");
  return lhs / Literal<T>(lhs.GetCodeGen(), rhs);
}

/**
 * Compute the modulo remainder of @em lhs and @em rhs.
 * @tparam T The type of the element being operated on.
 * @param lhs The left input to the modulo operation.
 * @param rhs The right input to the modulo operation.
 * @return The result of the modulo.
 */
template <typename T, typename = std::enable_if_t<traits::supports_modulo<T>::value>>
inline Value<T> operator%(const Value<T> &lhs, const Value<T> &rhs) {
  CodeGen *codegen = lhs.GetCodeGen();
  return Value<T>(codegen, codegen->Mod(lhs.GetRaw(), rhs.GetRaw()));
}

/**
 * Compute the modulo remainder of @em lhs and the literal value @em rhs.
 * @tparam T The type of the element being operated on.
 * @param lhs The left input to the modulo operation.
 * @param rhs The right input to the modulo operation.
 * @return The result of the modulo.
 */
template <typename T, typename = std::enable_if_t<traits::supports_modulo<T>::value>>
inline Value<T> operator%(const Value<T> &lhs, T rhs) {
  return lhs % Literal<T>(lhs.GetCodeGen(), rhs);
}

}  // namespace tpl::sql::codegen::edsl
