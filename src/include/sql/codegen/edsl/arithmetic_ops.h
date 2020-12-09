#pragma once

#include "parsing/token.h"
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
  return ArithmeticOperation(parsing::Token::Type::PLUS, lhs, rhs);
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
  return ArithmeticOperation(parsing::Token::Type::PLUS, lhs,
                             Literal<ValueT<Lhs>>(lhs.GetCodeGen(), rhs));
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
  return ArithmeticOperation(parsing::Token::Type::PLUS,
                             Literal<ValueT<Rhs>>(rhs.GetCodeGen(), lhs), rhs);
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
  return ArithmeticOperation(parsing::Token::Type::MINUS, lhs, rhs);
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
  return ArithmeticOperation(parsing::Token::Type::MINUS, lhs,
                             Literal<ValueT<Lhs>>(lhs.GetCodeGen(), rhs));
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
  return ArithmeticOperation(parsing::Token::Type::MINUS,
                             Literal<ValueT<Rhs>>(rhs.GetCodeGen(), lhs), rhs);
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
  return ArithmeticOperation(parsing::Token::Type::STAR, lhs, rhs);
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
  return ArithmeticOperation(parsing::Token::Type::STAR, lhs,
                             Literal<ValueT<Lhs>>(lhs.GetCodeGen(), rhs));
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
  return ArithmeticOperation(parsing::Token::Type::STAR,
                             Literal<ValueT<Rhs>>(rhs.GetCodeGen(), lhs), rhs);
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
  return ArithmeticOperation(parsing::Token::Type::SLASH, lhs, rhs);
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
  return ArithmeticOperation(parsing::Token::Type::SLASH, lhs,
                             Literal<ValueT<Lhs>>(lhs.GetCodeGen(), rhs));
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
  return ArithmeticOperation(parsing::Token::Type::SLASH,
                             Literal<ValueT<Rhs>>(rhs.GetCodeGen(), lhs), rhs);
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
  return ArithmeticOperation(parsing::Token::Type::PERCENT, lhs, rhs);
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
  return ArithmeticOperation(parsing::Token::Type::PERCENT, lhs,
                             Literal<ValueT<Lhs>>(lhs.GetCodeGen(), rhs));
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
  return ArithmeticOperation(parsing::Token::Type::PERCENT,
                             Literal<ValueT<Rhs>>(rhs.GetCodeGen(), lhs), rhs);
}

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
  return ArithmeticOperation(parsing::Token::Type::AMPERSAND, lhs, rhs);
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
  return ArithmeticOperation(parsing::Token::Type::AMPERSAND, lhs,
                             Literal<ValueT<Lhs>>(lhs.GetCodeGen(), rhs));
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
  return ArithmeticOperation(parsing::Token::Type::AMPERSAND,
                             Literal<ValueT<Rhs>>(rhs.GetCodeGen(), lhs), rhs);
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
  return ArithmeticOperation(parsing::Token::Type::BIT_OR, lhs, rhs);
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
  return ArithmeticOperation(parsing::Token::Type::BIT_OR, lhs,
                             Literal<ValueT<Lhs>>(lhs.GetCodeGen(), rhs));
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
  return ArithmeticOperation(parsing::Token::Type::BIT_OR,
                             Literal<ValueT<Rhs>>(rhs.GetCodeGen(), lhs), rhs);
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
  return ArithmeticOperation(parsing::Token::Type::BIT_XOR, lhs, rhs);
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
  return ArithmeticOperation(parsing::Token::Type::BIT_XOR, lhs,
                             Literal<ValueT<Lhs>>(lhs.GetCodeGen(), rhs));
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
  return ArithmeticOperation(parsing::Token::Type::BIT_XOR,
                             Literal<ValueT<Rhs>>(rhs.GetCodeGen(), lhs), rhs);
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
  return ArithmeticOperation(parsing::Token::Type::BIT_SHR, lhs, rhs);
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
  return ArithmeticOperation(parsing::Token::Type::BIT_SHR, lhs,
                             Literal<ValueT<Lhs>>(lhs.GetCodeGen(), rhs));
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
  return ArithmeticOperation(parsing::Token::Type::BIT_SHR,
                             Literal<ValueT<Rhs>>(rhs.GetCodeGen(), lhs), rhs);
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
  return ArithmeticOperation(parsing::Token::Type::BIT_SHL, lhs, rhs);
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
  return ArithmeticOperation(parsing::Token::Type::BIT_SHL, lhs,
                             Literal<ValueT<Lhs>>(lhs.GetCodeGen(), rhs));
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
  return ArithmeticOperation(parsing::Token::Type::BIT_SHL,
                             Literal<ValueT<Rhs>>(rhs.GetCodeGen(), lhs), rhs);
}

}  // namespace tpl::sql::codegen::edsl