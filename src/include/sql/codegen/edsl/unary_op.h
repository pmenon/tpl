#pragma once

#include <utility>

#include "parsing/token.h"
#include "sql/codegen/edsl/literal.h"

namespace tpl::sql::codegen::edsl {

/**
 * An generic unary expression.
 * @tparam T The template type of the left-hand input.
 */
template <typename T, typename InputType>
class UnaryOperation {
 public:
  /** The value type of the expression. */
  using ValueType = T;

  /**
   * Create a new unary operation of the given type, and applied to the provided argument.
   * @param op The type of operation.
   * @param input The input expression.
   */
  UnaryOperation(parsing::Token::Type op, InputType input)
      : codegen_(input.GetCodeGen()), op_(op), input_(std::forward<InputType>(input)) {}

  /**
   * @return The result of the application of the unary operation on the input arguments.
   */
  [[nodiscard]] ast::Expression *Eval() const { return codegen_->UnaryOp(op_, input_.Eval()); }

  /**
   * @return The code generator instance.
   */
  CodeGen *GetCodeGen() const noexcept { return codegen_; }

 private:
  // The code generator.
  CodeGen *codegen_;
  // The operation.
  parsing::Token::Type op_;
  // The input.
  InputType input_;
};

/**
 * Trait specialization for unary operations.
 * @tparam The ETL value type of the unary expression.
 * @tparam InputType The type of the input operation.
 */
template <typename T, typename InputType>
struct Traits<UnaryOperation<T, InputType>> {
  /** The value type of the expression is the same as the left input. */
  using ValueType = T;
  /** Arithmetic is an ETL expression. */
  static constexpr bool kIsETL = true;
};

}  // namespace tpl::sql::codegen::edsl