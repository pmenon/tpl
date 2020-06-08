#pragma once

#include <algorithm>

#include "sql/operators/boolean_operators.h"
#include "sql/operators/comparison_operators.h"
#include "sql/value.h"

namespace tpl::sql {

/**
 * Comparison functions for SQL values.
 */
class ComparisonFunctions : public AllStatic {
 public:
  static void NotBoolVal(BoolVal *result, const BoolVal &input);

  static void EqBoolVal(BoolVal *result, const BoolVal &v1, const BoolVal &v2);
  static void GeBoolVal(BoolVal *result, const BoolVal &v1, const BoolVal &v2);
  static void GtBoolVal(BoolVal *result, const BoolVal &v1, const BoolVal &v2);
  static void LeBoolVal(BoolVal *result, const BoolVal &v1, const BoolVal &v2);
  static void LtBoolVal(BoolVal *result, const BoolVal &v1, const BoolVal &v2);
  static void NeBoolVal(BoolVal *result, const BoolVal &v1, const BoolVal &v2);

  static void EqInteger(BoolVal *result, const Integer &v1, const Integer &v2);
  static void GeInteger(BoolVal *result, const Integer &v1, const Integer &v2);
  static void GtInteger(BoolVal *result, const Integer &v1, const Integer &v2);
  static void LeInteger(BoolVal *result, const Integer &v1, const Integer &v2);
  static void LtInteger(BoolVal *result, const Integer &v1, const Integer &v2);
  static void NeInteger(BoolVal *result, const Integer &v1, const Integer &v2);

  static void EqReal(BoolVal *result, const Real &v1, const Real &v2);
  static void GeReal(BoolVal *result, const Real &v1, const Real &v2);
  static void GtReal(BoolVal *result, const Real &v1, const Real &v2);
  static void LeReal(BoolVal *result, const Real &v1, const Real &v2);
  static void LtReal(BoolVal *result, const Real &v1, const Real &v2);
  static void NeReal(BoolVal *result, const Real &v1, const Real &v2);

  static void EqDateVal(BoolVal *result, const DateVal &v1, const DateVal &v2);
  static void GeDateVal(BoolVal *result, const DateVal &v1, const DateVal &v2);
  static void GtDateVal(BoolVal *result, const DateVal &v1, const DateVal &v2);
  static void LeDateVal(BoolVal *result, const DateVal &v1, const DateVal &v2);
  static void LtDateVal(BoolVal *result, const DateVal &v1, const DateVal &v2);
  static void NeDateVal(BoolVal *result, const DateVal &v1, const DateVal &v2);

  static void EqTimestampVal(BoolVal *result, const TimestampVal &v1, const TimestampVal &v2);
  static void GeTimestampVal(BoolVal *result, const TimestampVal &v1, const TimestampVal &v2);
  static void GtTimestampVal(BoolVal *result, const TimestampVal &v1, const TimestampVal &v2);
  static void LeTimestampVal(BoolVal *result, const TimestampVal &v1, const TimestampVal &v2);
  static void LtTimestampVal(BoolVal *result, const TimestampVal &v1, const TimestampVal &v2);
  static void NeTimestampVal(BoolVal *result, const TimestampVal &v1, const TimestampVal &v2);

  static void EqStringVal(BoolVal *result, const StringVal &v1, const StringVal &v2);
  static void GeStringVal(BoolVal *result, const StringVal &v1, const StringVal &v2);
  static void GtStringVal(BoolVal *result, const StringVal &v1, const StringVal &v2);
  static void LeStringVal(BoolVal *result, const StringVal &v1, const StringVal &v2);
  static void LtStringVal(BoolVal *result, const StringVal &v1, const StringVal &v2);
  static void NeStringVal(BoolVal *result, const StringVal &v1, const StringVal &v2);
};

// ---------------------------------------------------------
// Implementation below
// ---------------------------------------------------------

// The functions below are inlined in the header for performance. Don't move it
// unless you know what you're doing.

inline void ComparisonFunctions::NotBoolVal(BoolVal *result, const BoolVal &input) {
  result->is_null = input.is_null;
  result->val = tpl::sql::Not{}(result->val);
}

#define BINARY_COMPARISON_NUMERIC_FN_HIDE_NULL(NAME, TYPE, OP)                                   \
  inline void ComparisonFunctions::NAME##TYPE(BoolVal *result, const TYPE &v1, const TYPE &v2) { \
    using CppType = decltype(v1.val);                                                            \
    result->is_null = (v1.is_null || v2.is_null);                                                \
    result->val = OP<CppType>{}(v1.val, v2.val);                                                 \
  }

#define BINARY_COMPARISON_STRING_FN_HIDE_NULL(NAME, TYPE, OP)                       \
  inline void ComparisonFunctions::NAME##TYPE(BoolVal *result, const StringVal &v1, \
                                              const StringVal &v2) {                \
    using CppType = decltype(v1.val);                                               \
    if (v1.is_null || v2.is_null) {                                                 \
      *result = BoolVal::Null();                                                    \
      return;                                                                       \
    }                                                                               \
    *result = BoolVal(OP<CppType>{}(v1.val, v2.val));                               \
  }

#define BINARY_COMPARISONS(NAME, OP)                             \
  BINARY_COMPARISON_NUMERIC_FN_HIDE_NULL(NAME, BoolVal, OP)      \
  BINARY_COMPARISON_NUMERIC_FN_HIDE_NULL(NAME, Integer, OP)      \
  BINARY_COMPARISON_NUMERIC_FN_HIDE_NULL(NAME, Real, OP)         \
  BINARY_COMPARISON_NUMERIC_FN_HIDE_NULL(NAME, DateVal, OP)      \
  BINARY_COMPARISON_NUMERIC_FN_HIDE_NULL(NAME, TimestampVal, OP) \
  BINARY_COMPARISON_STRING_FN_HIDE_NULL(NAME, StringVal, OP)

BINARY_COMPARISONS(Eq, tpl::sql::Equal);
BINARY_COMPARISONS(Ge, tpl::sql::GreaterThanEqual);
BINARY_COMPARISONS(Gt, tpl::sql::GreaterThan);
BINARY_COMPARISONS(Le, tpl::sql::LessThanEqual);
BINARY_COMPARISONS(Lt, tpl::sql::LessThan);
BINARY_COMPARISONS(Ne, tpl::sql::NotEqual);

#undef BINARY_COMPARISONS
#undef BINARY_COMPARISON_STRING_FN_HIDE_NULL
#undef BINARY_COMPARISON_NUMERIC_FN_HIDE_NULL

}  // namespace tpl::sql
