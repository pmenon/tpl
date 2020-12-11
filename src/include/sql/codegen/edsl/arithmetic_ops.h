#pragma once

#include "parsing/token.h"
#include "sql/codegen/edsl/binary_expression.h"
#include "sql/codegen/edsl/literal.h"
#include "sql/codegen/edsl/traits.h"

namespace tpl::sql::codegen::edsl {

/**
 * Builds an expression representing the addition of @em lhs and @em rhs.
 * @tparam Lhs The left hand side expression.
 * @tparam Rhs The right hand side expression.
 * @param lhs The left input.
 * @param rhs The right input.
 * @return An expression representing the addition of @em lhs and @em rhs.
 */
template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<AllETLExpr<Lhs, Rhs> && SameValueType<Lhs, Rhs>>>
constexpr auto operator+(Lhs &&lhs, Rhs &&rhs) {
  return BinaryExpression<ValueT<Lhs>, Lhs, Rhs>(parsing::Token::Type::PLUS, lhs, rhs);
}

/**
 * Builds an expression representing the addition of @em lhs and @em rhs (literal).
 * @tparam Lhs The left hand side expression.
 * @tparam Rhs The right hand side expression.
 * @param lhs The left input.
 * @param rhs The right input.
 * @return An expression representing the addition of @em lhs and @em rhs.
 */
template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<IsETLExpr<Lhs> && std::is_integral_v<Rhs>>>
constexpr auto operator+(Lhs &&lhs, Rhs rhs) {
  return BinaryExpression<ValueT<Lhs>, Lhs, Literal<ValueT<Lhs>>>(
      parsing::Token::Type::PLUS, lhs, Literal<ValueT<Lhs>>(lhs.GetCodeGen(), rhs));
}

/**
 * Builds an expression representing the addition of @em lhs (scalar) and @em rhs.
 * @tparam Lhs The left hand side expression.
 * @tparam Rhs The right hand side expression.
 * @param lhs The left input.
 * @param rhs The right input.
 * @return An expression representing the addition of @em lhs and @em rhs.
 */
template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<std::is_integral_v<Lhs> && IsETLExpr<Rhs>>>
constexpr auto operator+(Lhs lhs, Rhs &&rhs) {
  return BinaryExpression<ValueT<Rhs>, Literal<ValueT<Rhs>>, Rhs>(
      parsing::Token::Type::PLUS, Literal<ValueT<Rhs>>(rhs.GetCodeGen(), lhs), rhs);
}

/**
 * Builds an expression representing the subtraction of @em lhs and @em rhs.
 * @tparam Lhs The left hand side expression.
 * @tparam Rhs The right hand side expression.
 * @param lhs The left input.
 * @param rhs The right input.
 * @return An expression representing the addition of @em lhs and @em rhs.
 */
template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<AllETLExpr<Lhs, Rhs> && SameValueType<Lhs, Rhs>>>
constexpr auto operator-(Lhs &&lhs, Rhs &&rhs) {
  return BinaryExpression<ValueT<Lhs>, Lhs, Rhs>(parsing::Token::Type::MINUS, lhs, rhs);
}

/**
 * Builds an expression representing the subtraction of @em lhs and @em rhs (literal).
 * @tparam Lhs The left hand side expression.
 * @tparam Rhs The right hand side expression.
 * @param lhs The left input.
 * @param rhs The right input.
 * @return An expression representing the addition of @em lhs and @em rhs.
 */
template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<IsETLExpr<Lhs> && std::is_integral_v<Rhs>>>
constexpr auto operator-(Lhs &&lhs, Rhs rhs) {
  return BinaryExpression<ValueT<Lhs>, Lhs, Literal<ValueT<Lhs>>>(
      parsing::Token::Type::MINUS, lhs, Literal<ValueT<Lhs>>(lhs.GetCodeGen(), rhs));
}

/**
 * Builds an expression representing the subtraction of @em lhs (scalar) and @em rhs.
 * @tparam Lhs The left hand side expression.
 * @tparam Rhs The right hand side expression.
 * @param lhs The left input.
 * @param rhs The right input.
 * @return An expression representing the addition of @em lhs and @em rhs.
 */
template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<std::is_integral_v<Lhs> && IsETLExpr<Rhs>>>
constexpr auto operator-(Lhs lhs, Rhs &&rhs) {
  return BinaryExpression<ValueT<Rhs>, Literal<ValueT<Rhs>>, Rhs>(
      parsing::Token::Type::MINUS, Literal<ValueT<Rhs>>(rhs.GetCodeGen(), lhs), rhs);
}

/**
 * Builds an expression representing the multiplication of @em lhs and @em rhs.
 * @tparam Lhs The left hand side expression.
 * @tparam Rhs The right hand side expression.
 * @param lhs The left input.
 * @param rhs The right input.
 * @return An expression representing the multiplication of @em lhs and @em rhs.
 */
template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<AllETLExpr<Lhs, Rhs> && SameValueType<Lhs, Rhs>>>
constexpr auto operator*(Lhs &&lhs, Rhs &&rhs) {
  return BinaryExpression<ValueT<Lhs>, Lhs, Rhs>(parsing::Token::Type::STAR, lhs, rhs);
}

/**
 * Builds an expression representing the multiplication of @em lhs and @em rhs (literal).
 * @tparam Lhs The left hand side expression.
 * @tparam Rhs The right hand side expression.
 * @param lhs The left input.
 * @param rhs The right input.
 * @return An expression representing the multiplication of @em lhs and @em rhs.
 */
template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<IsETLExpr<Lhs> && std::is_integral_v<Rhs>>>
constexpr auto operator*(Lhs &&lhs, Rhs rhs) {
  return BinaryExpression<ValueT<Lhs>, Lhs, Literal<ValueT<Lhs>>>(
      parsing::Token::Type::STAR, lhs, Literal<ValueT<Lhs>>(lhs.GetCodeGen(), rhs));
}

/**
 * Builds an expression representing the multiplication of @em lhs (scalar) and @em rhs.
 * @tparam Lhs The left hand side expression.
 * @tparam Rhs The right hand side expression.
 * @param lhs The left input.
 * @param rhs The right input.
 * @return An expression representing the multiplication of @em lhs and @em rhs.
 */
template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<std::is_integral_v<Lhs> && IsETLExpr<Rhs>>>
constexpr auto operator*(Lhs lhs, Rhs &&rhs) {
  return BinaryExpression<ValueT<Rhs>, Literal<ValueT<Rhs>>, Rhs>(
      parsing::Token::Type::STAR, Literal<ValueT<Rhs>>(rhs.GetCodeGen(), lhs), rhs);
}

/**
 * Builds an expression representing the division of @em lhs and @em rhs.
 * @tparam Lhs The left hand side expression.
 * @tparam Rhs The right hand side expression.
 * @param lhs The left input.
 * @param rhs The right input.
 * @return An expression representing the division of @em lhs and @em rhs.
 */
template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<AllETLExpr<Lhs, Rhs> && SameValueType<Lhs, Rhs>>>
constexpr auto operator/(Lhs &&lhs, Rhs &&rhs) {
  return BinaryExpression<ValueT<Lhs>, Lhs, Rhs>(parsing::Token::Type::SLASH, lhs, rhs);
}

/**
 * Builds an expression representing the division of @em lhs and @em rhs (literal).
 * @tparam Lhs The left hand side expression.
 * @tparam Rhs The right hand side expression.
 * @param lhs The left input.
 * @param rhs The right input.
 * @return An expression representing the division of @em lhs and @em rhs.
 */
template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<IsETLExpr<Lhs> && std::is_integral_v<Rhs>>>
constexpr auto operator/(Lhs &&lhs, Rhs rhs) {
  return BinaryExpression<ValueT<Lhs>, Lhs, Literal<ValueT<Lhs>>>(
      parsing::Token::Type::SLASH, lhs, Literal<ValueT<Lhs>>(lhs.GetCodeGen(), rhs));
}

/**
 * Builds an expression representing the division of @em lhs (scalar) and @em rhs.
 * @tparam Lhs The left hand side expression.
 * @tparam Rhs The right hand side expression.
 * @param lhs The left input.
 * @param rhs The right input.
 * @return An expression representing the division of @em lhs and @em rhs.
 */
template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<std::is_integral_v<Lhs> && IsETLExpr<Rhs>>>
constexpr auto operator/(Lhs lhs, Rhs &&rhs) {
  return BinaryExpression<ValueT<Rhs>, Literal<ValueT<Rhs>>, Rhs>(
      parsing::Token::Type::SLASH, Literal<ValueT<Rhs>>(rhs.GetCodeGen(), lhs), rhs);
}

/**
 * Builds an expression representing the modulo of @em lhs and @em rhs.
 * @tparam Lhs The left hand side expression.
 * @tparam Rhs The right hand side expression.
 * @param lhs The left input.
 * @param rhs The right input.
 * @return An expression representing the modulo of @em lhs and @em rhs.
 */
template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<AllETLExpr<Lhs, Rhs> && SameValueType<Lhs, Rhs>>>
constexpr auto operator%(Lhs &&lhs, Rhs &&rhs) {
  return BinaryExpression<ValueT<Lhs>, Lhs, Rhs>(parsing::Token::Type::PERCENT, lhs, rhs);
}

/**
 * Builds an expression representing the modulo of @em lhs and @em rhs (literal).
 * @tparam Lhs The left hand side expression.
 * @tparam Rhs The right hand side expression.
 * @param lhs The left input.
 * @param rhs The right input.
 * @return An expression representing the modulo of @em lhs and @em rhs.
 */
template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<IsETLExpr<Lhs> && std::is_integral_v<Rhs>>>
constexpr auto operator%(Lhs &&lhs, Rhs rhs) {
  return BinaryExpression<ValueT<Lhs>, Lhs, Literal<ValueT<Lhs>>>(
      parsing::Token::Type::PERCENT, lhs, Literal<ValueT<Lhs>>(lhs.GetCodeGen(), rhs));
}

/**
 * Builds an expression representing the modulo of @em lhs (scalar) and @em rhs.
 * @tparam Lhs The left hand side expression.
 * @tparam Rhs The right hand side expression.
 * @param lhs The left input.
 * @param rhs The right input.
 * @return An expression representing the modulo of @em lhs and @em rhs.
 */
template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<std::is_integral_v<Lhs> && IsETLExpr<Rhs>>>
constexpr auto operator%(Lhs lhs, Rhs &&rhs) {
  return BinaryExpression<ValueT<Rhs>, Literal<ValueT<Rhs>>, Rhs>(
      parsing::Token::Type::PERCENT, Literal<ValueT<Rhs>>(rhs.GetCodeGen(), lhs), rhs);
}

}  // namespace tpl::sql::codegen::edsl