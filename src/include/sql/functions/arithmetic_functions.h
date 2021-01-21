#pragma once

#include "common/exception.h"
#include "sql/functions/helpers.h"
#include "sql/operators/numeric_binary_operators.h"
#include "sql/operators/numeric_operators.h"
#include "sql/value.h"

namespace tpl::sql {

/**
 * Utility class to handle various arithmetic SQL functions.
 */
class ArithmeticFunctions : public AllStatic {
 public:
  /**
   * Perform *result = a+b, ignoring arithmetic overflow.
   * @tparam T The SQL types to operator on. Required to derived from sql::Val.
   * @param[out] result The location where the result of stored.
   * @param a The left input.
   * @param b The right input.
   */
  template <SQLValueType T>
  static void Add(T *result, const T &a, const T &b) {
    BinaryFunction::EvalFast(result, a, b, tpl::sql::Add<decltype(T::val)>{});
  }

  /**
   * Perform *result = a+b, storing the overflow flag in @em overflow.
   * @tparam T The SQL types to operator on. Required to derived from sql::Val.
   * @param[out] result The location where the result of stored.
   * @param a The left input.
   * @param b The right input.
   * @param[out] overflow Flag storing if an overflow occurred in the operation.
   */
  template <SQLValueType T>
  static void Add(T *result, const T &a, const T &b, bool *overflow) {
    BinaryFunction::EvalFast(result, a, b, [overflow]<typename U>(U x, U y) {
      U retval;
      *overflow = tpl::sql::AddWithOverflow<U>{}(x, y, &retval);
      return retval;
    });
  }

  /**
   * Perform *result = a-b, ignoring arithmetic overflow.
   * @tparam T The SQL types to operator on. Required to derived from sql::Val.
   * @param[out] result The location where the result of stored.
   * @param a The left input.
   * @param b The right input.
   */
  template <SQLValueType T>
  static void Sub(T *result, const T &a, const T &b) {
    BinaryFunction::EvalFast(result, a, b, tpl::sql::Subtract<decltype(T::val)>{});
  }

  /**
   * Perform *result = a-b, storing the overflow flag in @em overflow.
   * @tparam T The SQL types to operator on. Required to derived from sql::Val.
   * @param[out] result The location where the result of stored.
   * @param a The left input.
   * @param b The right input.
   * @param[out] overflow Flag storing if an overflow occurred in the operation.
   */
  template <SQLValueType T>
  static void Sub(T *result, const T &a, const T &b, bool *overflow) {
    BinaryFunction::EvalFast(result, a, b, [overflow]<typename U>(U x, U y) {
      U retval;
      *overflow = tpl::sql::SubtractWithOverflow<U>{}(x, y, &retval);
      return retval;
    });
  }

  /**
   * Perform *result = a*b, ignoring arithmetic overflow.
   * @tparam T The SQL types to operator on. Required to derived from sql::Val.
   * @param[out] result The location where the result of stored.
   * @param a The left input.
   * @param b The right input.
   */
  template <SQLValueType T>
  static void Mul(T *result, const T &a, const T &b) {
    BinaryFunction::EvalFast(result, a, b, tpl::sql::Multiply<decltype(T::val)>{});
  }

  /**
   * Perform *result = a*b, storing the overflow flag in @em overflow.
   * @tparam T The SQL types to operator on. Required to derived from sql::Val.
   * @param[out] result The location where the result of stored.
   * @param a The left input.
   * @param b The right input.
   * @param[out] overflow Flag storing if an overflow occurred in the operation.
   */
  template <SQLValueType T>
  static void Mul(T *result, const T &a, const T &b, bool *overflow) {
    BinaryFunction::EvalFast(result, a, b, [overflow]<typename U>(U x, U y) {
      U retval;
      *overflow = tpl::sql::MultiplyWithOverflow<U>{}(x, y, &retval);
      return retval;
    });
  }

  /**
   * Perform *result = a/b. If the divisor is zero, the output parameter @em div_by_zero is set and
   * the division is skipped.
   * @tparam T The SQL types to operator on. Required to derived from sql::Val.
   * @param[out] result The location where the result of stored.
   * @param a The left input.
   * @param b The right input.
   * @param div_by_zero Flag indicating if the divisor is zero.
   */
  template <SQLValueType T>
  static void Div(T *result, const T &a, const T &b, bool *div_by_zero) {
    BinaryFunction::EvalHideNull(
        result, a, b, tpl::sql::Divide<decltype(T::val)>{},
        [div_by_zero]<typename U>(U, U y) { return !(*div_by_zero = (y == U{0})); });
  }

  /**
   * Perform *result = a%b. If the divisor is zero, the output parameter @em div_by_zero is set and
   * the division is skipped.
   * @tparam T The SQL types to operator on. Required to derived from sql::Val.
   * @param[out] result The location where the result of stored.
   * @param a The left input.
   * @param b The right input.
   * @param div_by_zero Flag indicating if the divisor is zero.
   */
  template <SQLValueType T>
  static void Mod(T *result, const T &a, const T &b, bool *div_by_zero) {
    BinaryFunction::EvalHideNull(
        result, a, b, tpl::sql::Modulo<decltype(T::val)>{},
        [div_by_zero]<typename U>(U, U y) { return !(*div_by_zero = (y == U{0})); });
  }

  /**
   * Compute PI, lol.
   * @param[out] result The value of PI as a double-precision floating point value.
   */
  static void Pi(Real *result) { *result = Real(tpl::sql::Pi{}()); }

  /**
   * Compute e, lol.
   * @param[out] result The value of e as a double-precision floating point value.
   */
  static void E(Real *result) { *result = Real(tpl::sql::E{}()); }

  /**
   * Compute the absolute value of the input SQL value.
   * @tparam T The arithmetic SQL value type.
   * @param[out] result Where the result of the operation is stored.
   * @param input The input value.
   */
  template <SQLValueType T>
  static void Abs(T *result, const T &input) {
    UnaryFunction::EvalFast(result, input, tpl::sql::Abs<decltype(T::val)>{});
  }

  /**
   * Compute acos(input).
   * @param[out] result Where the result of the operation is stored.
   * @param input The input value.
   */
  static void Acos(Real *result, const Real &input);

  /**
   * Compute asin(input).
   * @param[out] result Where the result of the operation is stored.
   * @param input The input value.
   */
  static void Asin(Real *result, const Real &input);

  /**
   * Compute atan(input).
   * @param[out] result Where the result of the operation is stored.
   * @param input The input value.
   */
  static void Atan(Real *result, const Real &input);

  /**
   * Compute atan(a, b).
   * @param[out] result Where the result of the operation is stored.
   * @param input The input value.
   */
  static void Atan2(Real *result, const Real &a, const Real &b);

  /**
   * Compute the cbrt(input).
   * @param[out] result Where the result of the operation is stored.
   * @param input The input value.
   */
  static void Cbrt(Real *result, const Real &input);

  /**
   * Compute ceil(input).
   * @param[out] result Where the result of the operation is stored.
   * @param input The input value.
   */
  static void Ceil(Real *result, const Real &input);

  /**
   * Compute cos(input).
   * @param[out] result Where the result of the operation is stored.
   * @param input The input value.
   */
  static void Cos(Real *result, const Real &input);

  /**
   * Compute cosh(input).
   * @param[out] result Where the result of the operation is stored.
   * @param input The input value.
   */
  static void Cosh(Real *result, const Real &input);

  /**
   * Compute cot(input).
   * @param[out] result Where the result of the operation is stored.
   * @param input The input value.
   */
  static void Cot(Real *result, const Real &input);

  /**
   * Compute degrees(input).
   * @param[out] result Where the result of the operation is stored.
   * @param input The input value.
   */
  static void Degrees(Real *result, const Real &input);

  /**
   * Compute exp(input).
   * @param[out] result Where the result of the operation is stored.
   * @param input The input value.
   */
  static void Exp(Real *result, const Real &input);

  /**
   * Compute floor(input).
   * @param[out] result Where the result of the operation is stored.
   * @param input The input value.
   */
  static void Floor(Real *result, const Real &input);

  /**
   * Compute log(input).
   * @param[out] result Where the result of the operation is stored.
   * @param input The input value.
   */
  static void Log(Real *result, const Real &base, const Real &inputal);

  /**
   * Compute log2(input).
   * @param[out] result Where the result of the operation is stored.
   * @param input The input value.
   */
  static void Log2(Real *result, const Real &input);

  /**
   * Compute log10(input).
   * @param[out] result Where the result of the operation is stored.
   * @param input The input value.
   */
  static void Log10(Real *result, const Real &input);

  /**
   * Compute ln(input).
   * @param[out] result Where the result of the operation is stored.
   * @param input The input value.
   */
  static void Ln(Real *result, const Real &input);

  /**
   * Compute pow(input).
   * @param[out] result Where the result of the operation is stored.
   * @param input The input value.
   */
  static void Pow(Real *result, const Real &base, const Real &inputal);

  /**
   * Compute radians(input).
   * @param[out] result Where the result of the operation is stored.
   * @param input The input value.
   */
  static void Radians(Real *result, const Real &input);

  /**
   * Compute round(input).
   * @param[out] result Where the result of the operation is stored.
   * @param input The input value.
   */
  static void Round(Real *result, const Real &input);

  /**
   * Compute roundup(input, scale).
   * @param[out] result Where the result of the operation is stored.
   * @param input The input value.
   */
  static void RoundUpTo(Real *result, const Real &input, const Integer &scale);

  /**
   * Compute sign(input).
   * @param[out] result Where the result of the operation is stored.
   * @param input The input value.
   */
  static void Sign(Real *result, const Real &input);

  /**
   * Compute sin(input).
   * @param[out] result Where the result of the operation is stored.
   * @param input The input value.
   */
  static void Sin(Real *result, const Real &input);

  /**
   * Compute sinh(input).
   * @param[out] result Where the result of the operation is stored.
   * @param input The input value.
   */
  static void Sinh(Real *result, const Real &input);

  /**
   * Compute sqrt(input).
   * @param[out] result Where the result of the operation is stored.
   * @param input The input value.
   */
  static void Sqrt(Real *result, const Real &input);

  /**
   * Compute tan(input).
   * @param[out] result Where the result of the operation is stored.
   * @param input The input value.
   */
  static void Tan(Real *result, const Real &input);

  /**
   * Compute tanh(input).
   * @param[out] result Where the result of the operation is stored.
   * @param input The input value.
   */
  static void Tanh(Real *result, const Real &input);

  /**
   * Compute truncate(input).
   * @param[out] result Where the result of the operation is stored.
   * @param input The input value.
   */
  static void Truncate(Real *result, const Real &input);
};

}  // namespace tpl::sql
