#pragma once

#include <algorithm>

#include "sql/functions/helpers.h"
#include "sql/operators/boolean_operators.h"
#include "sql/operators/comparison_operators.h"
#include "sql/value.h"

namespace tpl::sql {

/**
 * Comparison functions for SQL values.
 */
class ComparisonFunctions : public AllStatic {
 public:
  /**
   * Compute the logical negation of the input boolean value, i.e., !input.
   * @param[out] result Where the result of the operation is stored.
   * @param input The input.
   */
  static void NotBoolVal(BoolVal *result, const BoolVal &input) {
    UnaryFunction::EvalFast(result, input, tpl::sql::Not{});
  }

  /**
   * Perform an equality comparison, i.e., result = a == b.
   * @tparam T The SQL value types of the input.
   * @param[out] result Where the result of the comparison is stored.
   * @param a The left input to the comparison.
   * @param b The right input to the comparison.
   */
  template <SQLValueType T>
  static void Equal(BoolVal *result, const T &a, const T &b) {
    BinaryFunction::EvalFast(result, a, b, tpl::sql::Equal<decltype(T::val)>{});
  }

  /**
   * Perform an equality comparison on two string inputs, i.e., result = a == b.
   * @param[out] result Where the result of the comparison is stored.
   * @param a The left input to the comparison.
   * @param b The right input to the comparison.
   */
  static void Equal(BoolVal *result, const StringVal &a, const StringVal &b) {
    BinaryFunction::EvalHideNull(result, a, b, tpl::sql::Equal<VarlenEntry>{});
  }

  /**
   * Perform an equality comparison, i.e., result = a >= b.
   * @tparam T The SQL value types of the input.
   * @param[out] result Where the result of the comparison is stored.
   * @param a The left input to the comparison.
   * @param b The right input to the comparison.
   */
  template <SQLValueType T>
  static void GreaterEqual(BoolVal *result, const T &a, const T &b) {
    BinaryFunction::EvalFast(result, a, b, tpl::sql::GreaterThanEqual<decltype(T::val)>{});
  }

  /**
   * Perform an equality comparison on two string inputs, i.e., result = a >= b.
   * @param[out] result Where the result of the comparison is stored.
   * @param a The left input to the comparison.
   * @param b The right input to the comparison.
   */
  static void GreaterEqual(BoolVal *result, const StringVal &a, const StringVal &b) {
    BinaryFunction::EvalFast(result, a, b, tpl::sql::GreaterThanEqual<VarlenEntry>{});
  }

  /**
   * Perform an equality comparison, i.e., result = a > b.
   * @tparam T The SQL value types of the input.
   * @param[out] result Where the result of the comparison is stored.
   * @param a The left input to the comparison.
   * @param b The right input to the comparison.
   */
  template <SQLValueType T>
  static void GreaterThan(BoolVal *result, const T &a, const T &b) {
    BinaryFunction::EvalFast(result, a, b, tpl::sql::GreaterThan<decltype(T::val)>{});
  }

  /**
   * Perform an equality comparison on two string inputs, i.e., result = a > b.
   * @param[out] result Where the result of the comparison is stored.
   * @param a The left input to the comparison.
   * @param b The right input to the comparison.
   */
  static void GreaterThan(BoolVal *result, const StringVal &a, const StringVal &b) {
    BinaryFunction::EvalFast(result, a, b, tpl::sql::GreaterThan<VarlenEntry>{});
  }

  /**
   * Perform an equality comparison, i.e., result = a <= b.
   * @tparam T The SQL value types of the input.
   * @param[out] result Where the result of the comparison is stored.
   * @param a The left input to the comparison.
   * @param b The right input to the comparison.
   */
  template <SQLValueType T>
  static void LessEqual(BoolVal *result, const T &a, const T &b) {
    BinaryFunction::EvalFast(result, a, b, tpl::sql::LessThanEqual<decltype(T::val)>{});
  }

  /**
   * Perform an equality comparison on two string inputs, i.e., result = a <= b.
   * @param[out] result Where the result of the comparison is stored.
   * @param a The left input to the comparison.
   * @param b The right input to the comparison.
   */
  static void LessEqual(BoolVal *result, const StringVal &a, const StringVal &b) {
    BinaryFunction::EvalFast(result, a, b, tpl::sql::LessThanEqual<VarlenEntry>{});
  }

  /**
   * Perform an equality comparison, i.e., result = a < b.
   * @tparam T The SQL value types of the input.
   * @param[out] result Where the result of the comparison is stored.
   * @param a The left input to the comparison.
   * @param b The right input to the comparison.
   */
  template <SQLValueType T>
  static void LessThan(BoolVal *result, const T &a, const T &b) {
    BinaryFunction::EvalFast(result, a, b, tpl::sql::LessThan<decltype(T::val)>{});
  }

  /**
   * Perform an equality comparison on two string inputs, i.e., result = a < b.
   * @param[out] result Where the result of the comparison is stored.
   * @param a The left input to the comparison.
   * @param b The right input to the comparison.
   */
  static void LessThan(BoolVal *result, const StringVal &a, const StringVal &b) {
    BinaryFunction::EvalFast(result, a, b, tpl::sql::LessThan<VarlenEntry>{});
  }

  /**
   * Perform an equality comparison, i.e., result = a != b.
   * @tparam T The SQL value types of the input.
   * @param[out] result Where the result of the comparison is stored.
   * @param a The left input to the comparison.
   * @param b The right input to the comparison.
   */
  template <SQLValueType T>
  static void NotEqual(BoolVal *result, const T &a, const T &b) {
    BinaryFunction::EvalFast(result, a, b, tpl::sql::NotEqual<decltype(T::val)>{});
  }

  /**
   * Perform an equality comparison on two string inputs, i.e., result = a != b.
   * @param[out] result Where the result of the comparison is stored.
   * @param a The left input to the comparison.
   * @param b The right input to the comparison.
   */
  static void NotEqual(BoolVal *result, const StringVal &a, const StringVal &b) {
    BinaryFunction::EvalFast(result, a, b, tpl::sql::NotEqual<VarlenEntry>{});
  }
};

}  // namespace tpl::sql
