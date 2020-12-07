#pragma once

#include <utility>

#include "parsing/token.h"
#include "sql/codegen/edsl/element_base.h"
#include "sql/codegen/edsl/literal.h"

namespace tpl::sql::codegen::edsl {

/**
 * An arithmetic expression.
 * @tparam Lhs The template type of the left-hand input.
 * @tparam Rhs The template type of the right-hand input.
 */
template <typename Lhs, typename Rhs>
class ArithmeticOperation {
 public:
  /** The value type of the expression. */
  using ValueType = typename Traits<Lhs>::ValueType;

  /**
   * Create a new arithmetic operation of the given type, and applied to the provided left- and
   * right-hand side arguments.
   * @param op The type of operation.
   * @param lhs The left input.
   * @param rhs The right input.
   */
  ArithmeticOperation(parsing::Token::Type op, Lhs lhs, Rhs rhs)
      : codegen_(lhs.GetCodeGen()),
        op_(op),
        lhs_(std::forward<Lhs>(lhs)),
        rhs_(std::forward<Rhs>(rhs)) {}

  /**
   * @return The result of the application of the arithmetic operation on the input arguments.
   */
  [[nodiscard]] ast::Expression *Eval() const {
    return codegen_->BinaryOp(op_, lhs_.Eval(), rhs_.Eval());
  }

  /**
   * @return The code generator instance.
   */
  CodeGen *GetCodeGen() const noexcept { return codegen_; }

 private:
  // The code generator.
  CodeGen *codegen_;
  // The operation.
  parsing::Token::Type op_;
  // The left input.
  Lhs lhs_;
  // The right input.
  Rhs rhs_;
};

/**
 * Trait specialization for arithmetic operations.
 * @tparam Lhs The type of the left-hand input.
 * @tparam Rhs The type of the right-hand input.
 */
template <typename Lhs, typename Rhs>
struct Traits<ArithmeticOperation<Lhs, Rhs>> {
  /** The value type of the expression is the same as the left input. */
  using ValueType = typename Traits<Lhs>::ValueType;
  /** Arithmetic is an ETL expression. */
  static constexpr bool kIsETL = true;
};

template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<AllETLExpr<Lhs, Rhs> && SameValueType<Lhs, Rhs>>>
constexpr auto operator+(Lhs &&lhs, Rhs &&rhs) {
  return ArithmeticOperation(parsing::Token::Type::PLUS, lhs, rhs);
}

template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<IsETLExpr<Lhs> && std::is_integral_v<Rhs>>>
constexpr auto operator+(Lhs &&lhs, Rhs rhs) {
  return ArithmeticOperation(parsing::Token::Type::PLUS, lhs,
                             Literal<ValueT<Lhs>>(lhs.GetCodeGen(), rhs));
}

template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<AllETLExpr<Lhs, Rhs> && SameValueType<Lhs, Rhs>>>
constexpr auto operator-(Lhs lhs, Rhs rhs) {
  return ArithmeticOperation(parsing::Token::Type::MINUS, lhs, rhs);
}

template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<AllETLExpr<Lhs, Rhs> && SameValueType<Lhs, Rhs>>>
constexpr auto operator*(Lhs lhs, Rhs rhs) {
  return ArithmeticOperation(parsing::Token::Type::STAR, lhs, rhs);
}

template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<AllETLExpr<Lhs, Rhs> && SameValueType<Lhs, Rhs>>>
constexpr auto operator/(Lhs lhs, Rhs rhs) {
  return ArithmeticOperation(parsing::Token::Type::SLASH, lhs, rhs);
}

template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<AllETLExpr<Lhs, Rhs> && SameValueType<Lhs, Rhs>>>
constexpr auto operator%(Lhs lhs, Rhs rhs) {
  return ArithmeticOperation(parsing::Token::Type::PERCENT, lhs, rhs);
}

template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<AllETLExpr<Lhs, Rhs> && SameValueType<Lhs, Rhs>>>
constexpr auto operator&(Lhs lhs, Rhs rhs) {
  return ArithmeticOperation(parsing::Token::Type::AMPERSAND, lhs, rhs);
}

template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<AllETLExpr<Lhs, Rhs> && SameValueType<Lhs, Rhs>>>
constexpr auto operator|(Lhs lhs, Rhs rhs) {
  return ArithmeticOperation(parsing::Token::Type::BIT_OR, lhs, rhs);
}

template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<AllETLExpr<Lhs, Rhs> && SameValueType<Lhs, Rhs>>>
constexpr auto operator^(Lhs lhs, Rhs rhs) {
  return ArithmeticOperation(parsing::Token::Type::BIT_XOR, lhs, rhs);
}

template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<AllETLExpr<Lhs, Rhs> && SameValueType<Lhs, Rhs>>>
constexpr auto operator>>(Lhs lhs, Rhs rhs) {
  return ArithmeticOperation(parsing::Token::Type::BIT_SHR, lhs, rhs);
}

template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<AllETLExpr<Lhs, Rhs> && SameValueType<Lhs, Rhs>>>
constexpr auto operator<<(Lhs lhs, Rhs rhs) {
  return ArithmeticOperation(parsing::Token::Type::BIT_SHL, lhs, rhs);
}

}  // namespace tpl::sql::codegen::edsl