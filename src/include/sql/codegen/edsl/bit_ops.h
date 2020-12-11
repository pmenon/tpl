#pragma once

#include "sql/codegen/edsl/binary_expression.h"
#include "sql/codegen/edsl/builtin_expression.h"
#include "sql/codegen/edsl/traits.h"

namespace tpl::sql::codegen::edsl {

/**
 * Builds an expression representing the bitwise AND of @em lhs and @em rhs.
 * @tparam Lhs The left hand side expression.
 * @tparam Rhs The right hand side expression.
 * @param lhs The left input.
 * @param rhs The right input.
 * @return An expression representing the bitwise AND of @em lhs and @em rhs.
 */
template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<AllETLExpr<Lhs, Rhs> && SameValueType<Lhs, Rhs>>>
constexpr auto operator&(Lhs &&lhs, Rhs &&rhs) {
  return BinaryExpression<ValueT<Lhs>, Lhs, Rhs>(parsing::Token::Type::AMPERSAND, lhs, rhs);
}

/**
 * Builds an expression representing the bitwise AND of @em lhs and @em rhs (literal).
 * @tparam Lhs The left hand side expression.
 * @tparam Rhs The right hand side expression.
 * @param lhs The left input.
 * @param rhs The right input.
 * @return An expression representing the bitwise AND of @em lhs and @em rhs.
 */
template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<IsETLExpr<Lhs> && std::is_integral_v<Rhs>>>
constexpr auto operator&(Lhs &&lhs, Rhs rhs) {
  return BinaryExpression<ValueT<Lhs>, Lhs, Literal<ValueT<Lhs>>>(
      parsing::Token::Type::AMPERSAND, lhs, Literal<ValueT<Lhs>>(lhs.GetCodeGen(), rhs));
}

/**
 * Builds an expression representing the bitwise AND of @em lhs (scalar) and @em rhs.
 * @tparam Lhs The left hand side expression.
 * @tparam Rhs The right hand side expression.
 * @param lhs The left input.
 * @param rhs The right input.
 * @return An expression representing the bitwise AND of @em lhs and @em rhs.
 */
template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<std::is_integral_v<Lhs> && IsETLExpr<Rhs>>>
constexpr auto operator&(Lhs lhs, Rhs &&rhs) {
  return BinaryExpression<ValueT<Rhs>, Literal<ValueT<Rhs>>, Rhs>(
      parsing::Token::Type::AMPERSAND, Literal<ValueT<Rhs>>(rhs.GetCodeGen(), lhs), rhs);
}

/**
 * Builds an expression representing the bitwise OR of @em lhs and @em rhs.
 * @tparam Lhs The left hand side expression.
 * @tparam Rhs The right hand side expression.
 * @param lhs The left input.
 * @param rhs The right input.
 * @return An expression representing the bitwise OR of @em lhs and @em rhs.
 */
template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<AllETLExpr<Lhs, Rhs> && SameValueType<Lhs, Rhs>>>
constexpr auto operator|(Lhs &&lhs, Rhs &&rhs) {
  return BinaryExpression<ValueT<Lhs>, Lhs, Rhs>(parsing::Token::Type::BIT_OR, lhs, rhs);
}

/**
 * Builds an expression representing the bitwise OR of @em lhs and @em rhs (literal).
 * @tparam Lhs The left hand side expression.
 * @tparam Rhs The right hand side expression.
 * @param lhs The left input.
 * @param rhs The right input.
 * @return An expression representing the bitwise OR of @em lhs and @em rhs.
 */
template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<IsETLExpr<Lhs> && std::is_integral_v<Rhs>>>
constexpr auto operator|(Lhs &&lhs, Rhs rhs) {
  return BinaryExpression<ValueT<Lhs>, Lhs, Literal<ValueT<Lhs>>>(
      parsing::Token::Type::BIT_OR, lhs, Literal<ValueT<Lhs>>(lhs.GetCodeGen(), rhs));
}

/**
 * Builds an expression representing the bitwise OR of @em lhs (scalar) and @em rhs.
 * @tparam Lhs The left hand side expression.
 * @tparam Rhs The right hand side expression.
 * @param lhs The left input.
 * @param rhs The right input.
 * @return An expression representing the bitwise OR of @em lhs and @em rhs.
 */
template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<std::is_integral_v<Lhs> && IsETLExpr<Rhs>>>
constexpr auto operator|(Lhs lhs, Rhs &&rhs) {
  return BinaryExpression<ValueT<Rhs>, Literal<ValueT<Rhs>>, Rhs>(
      parsing::Token::Type::BIT_OR, Literal<ValueT<Rhs>>(rhs.GetCodeGen(), lhs), rhs);
}

/**
 * Builds an expression representing the bitwise XOR of @em lhs and @em rhs.
 * @tparam Lhs The left hand side expression.
 * @tparam Rhs The right hand side expression.
 * @param lhs The left input.
 * @param rhs The right input.
 * @return An expression representing the bitwise XOR of @em lhs and @em rhs.
 */
template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<AllETLExpr<Lhs, Rhs> && SameValueType<Lhs, Rhs>>>
constexpr auto operator^(Lhs &&lhs, Rhs &&rhs) {
  return BinaryExpression<ValueT<Lhs>, Lhs, Rhs>(parsing::Token::Type::BIT_XOR, lhs, rhs);
}

/**
 * Builds an expression representing the bitwise XOR of @em lhs and @em rhs (literal).
 * @tparam Lhs The left hand side expression.
 * @tparam Rhs The right hand side expression.
 * @param lhs The left input.
 * @param rhs The right input.
 * @return An expression representing the bitwise XOR of @em lhs and @em rhs.
 */
template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<IsETLExpr<Lhs> && std::is_integral_v<Rhs>>>
constexpr auto operator^(Lhs &&lhs, Rhs rhs) {
  return BinaryExpression<ValueT<Lhs>, Lhs, Literal<ValueT<Lhs>>>(
      parsing::Token::Type::BIT_XOR, lhs, Literal<ValueT<Lhs>>(lhs.GetCodeGen(), rhs));
}

/**
 * Builds an expression representing the bitwise XOR of @em lhs (scalar) and @em rhs.
 * @tparam Lhs The left hand side expression.
 * @tparam Rhs The right hand side expression.
 * @param lhs The left input.
 * @param rhs The right input.
 * @return An expression representing the bitwise XOR of @em lhs and @em rhs.
 */
template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<std::is_integral_v<Lhs> && IsETLExpr<Rhs>>>
constexpr auto operator^(Lhs lhs, Rhs &&rhs) {
  return BinaryExpression<ValueT<Rhs>, Literal<ValueT<Rhs>>, Rhs>(
      parsing::Token::Type::BIT_XOR, Literal<ValueT<Rhs>>(rhs.GetCodeGen(), lhs), rhs);
}

/**
 * Builds an expression representing the bitwise right-shift of @em lhs and @em rhs.
 * @tparam Lhs The left hand side expression.
 * @tparam Rhs The right hand side expression.
 * @param lhs The left input.
 * @param rhs The right input.
 * @return An expression representing the bitwise right-shift of @em lhs and @em rhs.
 */
template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<AllETLExpr<Lhs, Rhs> && SameValueType<Lhs, Rhs>>>
constexpr auto operator>>(Lhs &&lhs, Rhs &&rhs) {
  return BinaryExpression<ValueT<Lhs>, Lhs, Rhs>(parsing::Token::Type::BIT_SHR, lhs, rhs);
}

/**
 * Builds an expression representing the bitwise right-shift of @em lhs and @em rhs (literal).
 * @tparam Lhs The left hand side expression.
 * @tparam Rhs The right hand side expression.
 * @param lhs The left input.
 * @param rhs The right input.
 * @return An expression representing the bitwise right-shift of @em lhs and @em rhs.
 */
template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<IsETLExpr<Lhs> && std::is_integral_v<Rhs>>>
constexpr auto operator>>(Lhs &&lhs, Rhs rhs) {
  return BinaryExpression<ValueT<Lhs>, Lhs, Literal<ValueT<Lhs>>>(
      parsing::Token::Type::BIT_SHR, lhs, Literal<ValueT<Lhs>>(lhs.GetCodeGen(), rhs));
}

/**
 * Builds an expression representing the bitwise right-shift of @em lhs (scalar) and @em rhs.
 * @tparam Lhs The left hand side expression.
 * @tparam Rhs The right hand side expression.
 * @param lhs The left input.
 * @param rhs The right input.
 * @return An expression representing the bitwise right-shift of @em lhs and @em rhs.
 */
template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<std::is_integral_v<Lhs> && IsETLExpr<Rhs>>>
constexpr auto operator>>(Lhs lhs, Rhs &&rhs) {
  return BinaryExpression<ValueT<Rhs>, Literal<ValueT<Rhs>>, Rhs>(
      parsing::Token::Type::BIT_SHR, Literal<ValueT<Rhs>>(rhs.GetCodeGen(), lhs), rhs);
}

/**
 * Builds an expression representing the bitwise left-shift of @em lhs and @em rhs.
 * @tparam Lhs The left hand side expression.
 * @tparam Rhs The right hand side expression.
 * @param lhs The left input.
 * @param rhs The right input.
 * @return An expression representing the bitwise left-shift of @em lhs and @em rhs.
 */
template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<AllETLExpr<Lhs, Rhs> && SameValueType<Lhs, Rhs>>>
constexpr auto operator<<(Lhs &&lhs, Rhs &&rhs) {
  return BinaryExpression<ValueT<Lhs>, Lhs, Rhs>(parsing::Token::Type::BIT_SHL, lhs, rhs);
}

/**
 * Builds an expression representing the bitwise left-shift of @em lhs and @em rhs (literal).
 * @tparam Lhs The left hand side expression.
 * @tparam Rhs The right hand side expression.
 * @param lhs The left input.
 * @param rhs The right input.
 * @return An expression representing the bitwise left-shift of @em lhs and @em rhs.
 */
template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<IsETLExpr<Lhs> && std::is_integral_v<Rhs>>>
constexpr auto operator<<(Lhs &&lhs, Rhs rhs) {
  return BinaryExpression<ValueT<Lhs>, Lhs, Literal<ValueT<Lhs>>>(
      parsing::Token::Type::BIT_SHL, lhs, Literal<ValueT<Lhs>>(lhs.GetCodeGen(), rhs));
}

/**
 * Builds an expression representing the bitwise left-shift of @em lhs (scalar) and @em rhs.
 * @tparam Lhs The left hand side expression.
 * @tparam Rhs The right hand side expression.
 * @param lhs The left input.
 * @param rhs The right input.
 * @return An expression representing the bitwise left-shift of @em lhs and @em rhs.
 */
template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<std::is_integral_v<Lhs> && IsETLExpr<Rhs>>>
constexpr auto operator<<(Lhs lhs, Rhs &&rhs) {
  return BinaryExpression<ValueT<Rhs>, Literal<ValueT<Rhs>>, Rhs>(
      parsing::Token::Type::BIT_SHL, Literal<ValueT<Rhs>>(rhs.GetCodeGen(), lhs), rhs);
}

/**
 * Count the number of trailing zeros in the given input. The input must be an unsigned integer.
 * @tparam T Must be an ETL unsigned integer type.
 * @param input The input expression.
 * @return An expression representing the @cttz() TPL call.
 */
template <typename T, typename = std::enable_if_t<IsETLExpr<T>>>
constexpr auto Cttz(T &&input) {
  return BuiltinExpression<UInt32, T>(ast::Builtin::Cttz, input);
}

/**
 * Count the number of leading zeros in the given input. The input must be an unsigned integer.
 * @tparam T Must be an ETL unsigned integer type.
 * @param input The input expression.
 * @return An expression representing the @ctlz() TPL call.
 */
template <typename T, typename = std::enable_if_t<IsETLExpr<T>>>
constexpr auto Ctlz(T &&input) {
  return BuiltinExpression<UInt32, T>(ast::Builtin::Ctlz, input);
}

}  // namespace tpl::sql::codegen::edsl
