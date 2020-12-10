#pragma once

#include "sql/codegen/edsl/fwd.h"

namespace tpl::sql::codegen::edsl {

/**
 * A relational expression.
 * @tparam Lhs The template type of the left-hand input.
 * @tparam Rhs The template type of the right-hand input.
 */
template <typename Lhs, typename Rhs>
class RelationalOperation {
 public:
  using ValueType = edsl::Bool;

  /**
   * Create a new relational operation of the given type, and applied to the provided left- and
   * right-hand side arguments.
   * @param op The type of operation.
   * @param lhs The left input.
   * @param rhs The right input.
   */
  RelationalOperation(parsing::Token::Type op, Lhs lhs, Rhs rhs)
      : codegen_(lhs.GetCodeGen()),
        op_(op),
        lhs_(std::forward<Lhs>(lhs)),
        rhs_(std::forward<Lhs>(rhs)) {}

  /**
   * @return The result of the application of the relational operation on the input arguments.
   */
  [[nodiscard]] ast::Expression *Eval() const {
    return codegen_->Compare(op_, lhs_.Eval(), rhs_.Eval());
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
 * Trait specialization for relational operations.
 * @tparam Lhs The type of the left-hand input.
 * @tparam Rhs The type of the right-hand input.
 */
template <typename Lhs, typename Rhs>
struct Traits<RelationalOperation<Lhs, Rhs>> {
  /** Relational operations produce primitive boolean values. */
  using ValueType = edsl::Bool;
  /** Arithmetic is an ETL expression. */
  static constexpr bool kIsETL = true;
};

/**
 * Builds an expression representing the equality of @em lhs and @em rhs.
 * @tparam Lhs The left hand side expression.
 * @tparam Rhs The right hand side expression.
 * @param lhs The left input.
 * @param rhs The right input.
 * @return An expression representing the equality of @em lhs and @em rhs.
 */
template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<AllETLExpr<Lhs, Rhs> && SameValueType<Lhs, Rhs>>>
auto operator==(Lhs &&lhs, Rhs &&rhs) {
  return RelationalOperation(parsing::Token::Type::EQUAL_EQUAL, lhs, rhs);
}

/**
 * Builds an expression representing the inequality of @em lhs and @em rhs.
 * @tparam Lhs The left hand side expression.
 * @tparam Rhs The right hand side expression.
 * @param lhs The left input.
 * @param rhs The right input.
 * @return An expression representing the inequality of @em lhs and @em rhs.
 */
template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<AllETLExpr<Lhs, Rhs> && SameValueType<Lhs, Rhs>>>
auto operator!=(Lhs &&lhs, Rhs &&rhs) {
  return RelationalOperation(parsing::Token::Type::BANG_EQUAL, lhs, rhs);
}

/**
 * Builds an expression representing the result of lhs >= rhs.
 * @tparam Lhs The left hand side expression.
 * @tparam Rhs The right hand side expression.
 * @param lhs The left input.
 * @param rhs The right input.
 * @return An expression representing the result of lhs >= rhs.
 */
template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<AllETLExpr<Lhs, Rhs> && SameValueType<Lhs, Rhs>>>
auto operator>=(Lhs &&lhs, Rhs &&rhs) {
  return RelationalOperation(parsing::Token::Type::GREATER_EQUAL, lhs, rhs);
}

/**
 * Builds an expression representing the result of lhs > rhs.
 * @tparam Lhs The left hand side expression.
 * @tparam Rhs The right hand side expression.
 * @param lhs The left input.
 * @param rhs The right input.
 * @return An expression representing the result of lhs > rhs.
 */
template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<AllETLExpr<Lhs, Rhs> && SameValueType<Lhs, Rhs>>>
auto operator>(Lhs &&lhs, Rhs &&rhs) {
  return RelationalOperation(parsing::Token::Type::GREATER, lhs, rhs);
}

/**
 * Builds an expression representing the result of lhs <= rhs.
 * @tparam Lhs The left hand side expression.
 * @tparam Rhs The right hand side expression.
 * @param lhs The left input.
 * @param rhs The right input.
 * @return An expression representing the result of lhs <= rhs.
 */
template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<AllETLExpr<Lhs, Rhs> && SameValueType<Lhs, Rhs>>>
auto operator<=(Lhs &&lhs, Rhs &&rhs) {
  return RelationalOperation(parsing::Token::Type::LESS_EQUAL, lhs, rhs);
}

/**
 * Builds an expression representing the result of lhs < rhs.
 * @tparam Lhs The left hand side expression.
 * @tparam Rhs The right hand side expression.
 * @param lhs The left input.
 * @param rhs The right input.
 * @return An expression representing the result of lhs < rhs.
 */
template <typename Lhs, typename Rhs,
          typename = std::enable_if_t<AllETLExpr<Lhs, Rhs> && SameValueType<Lhs, Rhs>>>
auto operator<(Lhs &&lhs, Rhs &&rhs) {
  return RelationalOperation(parsing::Token::Type::LESS, lhs, rhs);
}

}  // namespace tpl::sql::codegen::edsl
