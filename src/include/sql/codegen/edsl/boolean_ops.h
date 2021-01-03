#pragma once

#include "sql/codegen/edsl/value.h"

namespace tpl::sql::codegen::edsl {

/**
 * Perform a logical conjunction between @em lhs and @em rhs.
 * @param lhs The left input to the conjunction.
 * @param rhs The right input to the conjunction.
 * @return The result of the conjunction.
 */
inline Value<bool> operator&&(const Value<bool> &lhs, const Value<bool> &rhs) {
  return Value<bool>(lhs.GetCodeGen(), lhs.GetCodeGen()->LogicalAnd(lhs.GetRaw(), rhs.GetRaw()));
}

/**
 * Perform a logical conjunction between @em lhs and @em rhs.
 * @param lhs The left input to the conjunction.
 * @param rhs The right input to the conjunction.
 * @return The result of the conjunction.
 */
inline Value<bool> operator||(const Value<bool> &lhs, const Value<bool> &rhs) {
  return Value<bool>(lhs.GetCodeGen(), lhs.GetCodeGen()->LogicalOr(lhs.GetRaw(), rhs.GetRaw()));
}

/**
 * Compute the boolean negation of the input @em input boolean.
 * @tparam T The type of the element being negated.
 * @param input The input to the negation.
 * @return The result of the negation.
 */
inline Value<bool> operator!(const Value<bool> &input) {
  return Value<bool>(input.GetCodeGen(), input.GetCodeGen()->LogicalNot(input.GetRaw()));
}

}  // namespace tpl::sql::codegen::edsl
