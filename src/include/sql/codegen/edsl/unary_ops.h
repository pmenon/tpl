#pragma once

#include "sql/codegen/edsl/traits.h"
#include "sql/codegen/edsl/unary_expression.h"

namespace tpl::sql::codegen::edsl {

/**
 * Take the address of an ETL value.
 * TODO(pmenon): Address can only be taken on ETL value class, not temporary expressions.
 *
 * @tparam T The template type.
 * @param input The input to take the address of.
 * @return An expression representing the address of the given value.
 */
template <typename T, typename = std::enable_if_t<IsETLExpr<T>>>
constexpr auto operator&(T &&input) {
  return UnaryExpression<Ptr<ValueT<T>>, T>(parsing::Token::Type::AMPERSAND, input);
}

/**
 * Take the bit-wise compliment of the input.
 * @tparam T The template type.
 * @param input The input to take the address of.
 * @return An expression representing the bit-wise compliment of the given value.
 */
template <typename T,
          typename = std::enable_if_t<IsETLExpr<T> && trait_details::IsInteger<ValueT<T>>::value>>
constexpr auto operator~(T &&input) {
  return UnaryExpression<ValueT<T>, T>(parsing::Token::Type::BIT_NOT, input);
}

}  // namespace tpl::sql::codegen::edsl
