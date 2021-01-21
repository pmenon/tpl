#pragma once

#include <algorithm>

#include "sql/operators/boolean_operators.h"
#include "sql/operators/comparison_operators.h"
#include "sql/value.h"
#include "sql/functions/helpers.h"

namespace tpl::sql {

/**
 * Comparison functions for SQL values.
 */
class ComparisonFunctions : public AllStatic {
 public:
  static void NotBoolVal(BoolVal *result, const BoolVal &input) {
    UnaryFunction::EvalFast(result, input, tpl::sql::Not{});
  }

  template <SQLValueType T>
  static void Equal(BoolVal *result, const T &a, const T &b) {
    BinaryFunction::EvalFast(result, a, b, tpl::sql::Equal<decltype(T::val)>{});
  }

  static void Equal(BoolVal *result, const StringVal &a, const StringVal &b) {
    BinaryFunction::EvalHideNull(result, a, b, tpl::sql::Equal<VarlenEntry>{});
  }

  template <SQLValueType T>
  static void GreaterEqual(BoolVal *result, const T &a, const T &b) {
    BinaryFunction::EvalFast(result, a, b, tpl::sql::GreaterThanEqual<decltype(T::val)>{});
  }

  static void GreaterEqual(BoolVal *result, const StringVal &a, const StringVal &b) {
    BinaryFunction::EvalFast(result, a, b, tpl::sql::GreaterThanEqual<VarlenEntry>{});
  }

  template <SQLValueType T>
  static void GreaterThan(BoolVal *result, const T &a, const T &b) {
    BinaryFunction::EvalFast(result, a, b, tpl::sql::GreaterThan<decltype(T::val)>{});
  }

  static void GreaterThan(BoolVal *result, const StringVal &a, const StringVal &b) {
    BinaryFunction::EvalFast(result, a, b, tpl::sql::GreaterThan<VarlenEntry>{});
  }

  template <SQLValueType T>
  static void LessEqual(BoolVal *result, const T &a, const T &b) {
    BinaryFunction::EvalFast(result, a, b, tpl::sql::LessThanEqual<decltype(T::val)>{});
  }

  static void LessEqual(BoolVal *result, const StringVal &a, const StringVal &b) {
    BinaryFunction::EvalFast(result, a, b, tpl::sql::LessThanEqual<VarlenEntry>{});
  }

  template <SQLValueType T>
  static void LessThan(BoolVal *result, const T &a, const T &b) {
    BinaryFunction::EvalFast(result, a, b, tpl::sql::LessThan<decltype(T::val)>{});
  }

  static void LessThan(BoolVal *result, const StringVal &a, const StringVal &b) {
    BinaryFunction::EvalFast(result, a, b, tpl::sql::LessThan<VarlenEntry>{});
  }

  template <SQLValueType T>
  static void NotEqual(BoolVal *result, const T &a, const T &b) {
    BinaryFunction::EvalFast(result, a, b, tpl::sql::NotEqual<decltype(T::val)>{});
  }

  static void NotEqual(BoolVal *result, const StringVal &a, const StringVal &b) {
    BinaryFunction::EvalFast(result, a, b, tpl::sql::NotEqual<VarlenEntry>{});
  }
};

}  // namespace tpl::sql
