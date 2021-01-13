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
  CodeGen *codegen = lhs.GetCodeGen();
  if (auto [left_lit, right_lit] = std::make_tuple(lhs.GetRaw()->SafeAs<ast::LiteralExpression>(),
                                                   rhs.GetRaw()->SafeAs<ast::LiteralExpression>());
      left_lit != nullptr && right_lit != nullptr) {
    return Value<bool>(codegen, codegen->Literal(left_lit->BoolVal() && right_lit->BoolVal()));
  } else if (left_lit != nullptr) {
    return Value<bool>(codegen, !left_lit->BoolVal() ? codegen->Literal(false) : rhs.GetRaw());
  } else if (right_lit != nullptr) {
    return Value<bool>(codegen, !right_lit->BoolVal() ? codegen->Literal(false) : lhs.GetRaw());
  } else {
    return Value<bool>(codegen, codegen->LogicalAnd(lhs.GetRaw(), rhs.GetRaw()));
  }
}

/**
 * Perform a logical conjunction between @em lhs and @em rhs.
 * @param lhs The left input to the conjunction.
 * @param rhs The right input to the conjunction.
 * @return The result of the conjunction.
 */
inline Value<bool> operator||(const Value<bool> &lhs, const Value<bool> &rhs) {
  CodeGen *codegen = lhs.GetCodeGen();
  if (auto [left_lit, right_lit] = std::make_tuple(lhs.GetRaw()->SafeAs<ast::LiteralExpression>(),
                                                   rhs.GetRaw()->SafeAs<ast::LiteralExpression>());
      left_lit != nullptr && right_lit != nullptr) {
    return Value<bool>(codegen, codegen->Literal(left_lit->BoolVal() || right_lit->BoolVal()));
  } else if (left_lit != nullptr) {
    return Value<bool>(codegen, left_lit->BoolVal() ? codegen->Literal(true) : rhs.GetRaw());
  } else if (right_lit != nullptr) {
    return Value<bool>(codegen, right_lit->BoolVal() ? codegen->Literal(true) : lhs.GetRaw());
  } else {
    return Value<bool>(codegen, codegen->LogicalOr(lhs.GetRaw(), rhs.GetRaw()));
  }
}

/**
 * Compute the boolean negation of the input @em input boolean.
 * @tparam T The type of the element being negated.
 * @param input The input to the negation.
 * @return The result of the negation.
 */
inline Value<bool> operator!(const Value<bool> &input) {
  CodeGen *codegen = input.GetCodeGen();
  if (auto expr = input.GetRaw()->SafeAs<ast::LiteralExpression>()) {
    return Value<bool>(codegen, codegen->Literal(!expr->BoolVal()));
  } else {
    return Value<bool>(codegen, codegen->LogicalNot(input.GetRaw()));
  }
}

}  // namespace tpl::sql::codegen::edsl
