#pragma once

#include "sql/codegen/ast_fwd.h"
#include "sql/codegen/codegen.h"
#include "sql/codegen/edsl/traits.h"

namespace tpl::sql::codegen::edsl {

/**
 * An generic binary expression.
 * @tparam T The ETL type of the expression.
 * @tparam Lhs The template type of the left-hand input.
 * @tparam Rhs The template type of the right-hand input.
 */
template <typename T, typename Lhs, typename Rhs>
class BinaryExpression {
 public:
  /** The value type of the expression. */
  using ValueType = T;

  /**
   * Create a new arithmetic operation of the given type, and applied to the provided left- and
   * right-hand side arguments.
   * @param op The type of operation.
   * @param lhs The left input.
   * @param rhs The right input.
   */
  BinaryExpression(parsing::Token::Type op, Lhs lhs, Rhs rhs)
      : codegen_(lhs.GetCodeGen()),
        op_(op),
        lhs_(std::forward<Lhs>(lhs)),
        rhs_(std::forward<Rhs>(rhs)) {}

  /**
   * @return The result of the application of the arithmetic operation on the input arguments.
   */
  ast::Expression *Eval() const { return codegen_->BinaryOp(op_, lhs_.Eval(), rhs_.Eval()); }

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
 * Trait specialization for binary expression.
 * @tparam T The ETL type of the binary expression.
 * @tparam Lhs The type of the left-hand input.
 * @tparam Rhs The type of the right-hand input.
 */
template <typename T, typename Lhs, typename Rhs>
struct Traits<BinaryExpression<T, Lhs, Rhs>> {
  /** The value type of the expression is the same as the left input. */
  using ValueType = T;
  /** Arithmetic is an ETL expression. */
  static constexpr bool kIsETL = true;
};

}  // namespace tpl::sql::codegen::edsl
