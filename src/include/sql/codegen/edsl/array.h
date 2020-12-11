#pragma once

#include "common/exception.h"
#include "sql/codegen/edsl/binary_expression.h"
#include "sql/codegen/edsl/integer.h"
#include "sql/codegen/edsl/literal.h"
#include "sql/codegen/edsl/traits.h"
#include "sql/codegen/edsl/value.h"

namespace tpl::sql::codegen::edsl {

/**
 * An index access.
 * @tparam Lhs The template type of the left-hand input.
 * @tparam Rhs The template type of the right-hand input.
 */
template <typename Lhs, typename Rhs>
class IndexExpression {
 public:
  /**
   * The ETL type of this expression.
   */
  using ValueType = typename Lhs::ElementType;

  /**
   * Create a new arithmetic operation of the given type, and applied to the provided left- and
   * right-hand side arguments.
   * @param op The type of operation.
   * @param lhs The left input.
   * @param rhs The right input.
   */
  IndexExpression(Lhs lhs, Rhs rhs)
      : codegen_(lhs.GetCodeGen()), lhs_(std::forward<Lhs>(lhs)), rhs_(std::forward<Rhs>(rhs)) {}

  /**
   * @return The result of the application of the arithmetic operation on the input arguments.
   */
  ast::Expression *Eval() const { return codegen_->ArrayAccess(lhs_.Eval(), rhs_.Eval()); }

  /**
   *
   * @tparam E
   * @param e
   * @return
   */
  template <typename E, typename = std::enable_if_t<IsETLExpr<E> && SameValueType<ValueType, E>>>
  IndexExpression &operator=(E &&e) {
    FunctionBuilder *function = codegen_->GetCurrentFunction();
    function->Append(codegen_->Assign(Eval(), e.Eval()));
    return *this;
  };

  /**
   * @return The code generator instance.
   */
  CodeGen *GetCodeGen() const noexcept { return codegen_; }

 private:
  // The code generator.
  CodeGen *codegen_;
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
template <typename Lhs, typename Rhs>
struct Traits<IndexExpression<Lhs, Rhs>> {
  /** The value type of the expression is the same as the left input. */
  using ValueType = typename Lhs::ElementType;
  /** Arithmetic is an ETL expression. */
  static constexpr bool kIsETL = true;
};

/**
 * An array.
 * @tparam T The type of elements contained in the array.
 */
template <std::size_t N, typename T>
class Array : public Value<Array<N, T>> {
 public:
  /**
   * The base type.
   */
  using Base = Value<Array<N, T>>;

  /**
   * The ETL value type.
   */
  using ValueType = Array<N, T>;

  /**
   * The array element type.
   */
  using ElementType = T;

  /**
   * Include base overloads.
   */
  using Base::Eval;
  using Base::GetCodeGen;

  /**
   * Create an array.
   * @param codegen The code generator.
   * @param name The name of the array.
   */
  Array(CodeGen *codegen, std::string_view name) : Base(codegen, name) {}

  /**
   * Array access using a C++ index.
   * @param index The index of the element to read.
   * @return The element at the given index.
   */
  auto operator[](std::size_t index) const {
    if constexpr (N != 0 && index > N) {
      throw Exception(ExceptionType::CodeGen, "Out-of-bounds array access.");
    }
    return (*this)[Literal<UInt64>(GetCodeGen(), index)];
  }

  /**
   * Access the element at the given index in this array. No bounds check is performed.
   * @tparam E The type.
   * @param index The index of the element to read.
   * @return The element at the given index.
   */
  template <typename E, typename = std::enable_if_t<IsETLExpr<E> && SameValueType<UInt64, E>>>
  auto operator[](E &&index) const {
    return IndexExpression(*this, index);
  }
};

/**
 * Trait specialization for binary expression.
 * @tparam T The ETL type of the binary expression.
 * @tparam Lhs The type of the left-hand input.
 * @tparam Rhs The type of the right-hand input.
 */
template <std::size_t N, typename T>
struct Traits<Array<N, T>> {
  /** The value type of the expression is the same as the left input. */
  using ValueType = Array<N, T>;
  /** The C++ type. */
  using CppType = T[N];
  /** The element type. */
  using ElementType = T;
  /** The size of the array. */
  static constexpr std::size_t kSize = N;
  /** Arithmetic is an ETL expression. */
  static constexpr bool kIsETL = true;
};

}  // namespace tpl::sql::codegen::edsl
