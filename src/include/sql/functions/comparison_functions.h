#pragma once

#include <algorithm>

#include "sql/operations/comparison_operators.h"
#include "sql/value.h"

namespace tpl::sql {

/**
 * Comparison functions for SQL values.
 */
class ComparisonFunctions {
 public:
  // Delete to force only static functions
  ComparisonFunctions() = delete;

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

  static void EqStringVal(BoolVal *result, const StringVal &v1,
                          const StringVal &v2);
  static void GeStringVal(BoolVal *result, const StringVal &v1,
                          const StringVal &v2);
  static void GtStringVal(BoolVal *result, const StringVal &v1,
                          const StringVal &v2);
  static void LeStringVal(BoolVal *result, const StringVal &v1,
                          const StringVal &v2);
  static void LtStringVal(BoolVal *result, const StringVal &v1,
                          const StringVal &v2);
  static void NeStringVal(BoolVal *result, const StringVal &v1,
                          const StringVal &v2);
};

// ---------------------------------------------------------
// Implementation below
// ---------------------------------------------------------

// The functions below are inlined in the header for performance. Don't move it
// unless you know what you're doing.

#define BINARY_COMPARISON_NUMERIC_FN_HIDE_NULL(NAME, TYPE, OP)                 \
  inline void ComparisonFunctions::NAME##TYPE(BoolVal *result, const TYPE &v1, \
                                              const TYPE &v2) {                \
    result->is_null = (v1.is_null || v2.is_null);                              \
    result->val = OP::Apply(v1.val, v2.val);                                   \
  }

#define BINARY_COMPARISON_STRING_FN_HIDE_NULL(NAME, TYPE, OP)      \
  inline void ComparisonFunctions::NAME##TYPE(                     \
      BoolVal *result, const StringVal &v1, const StringVal &v2) { \
    if (v1.is_null || v2.is_null) {                                \
      *result = BoolVal::Null();                                   \
      return;                                                      \
    }                                                              \
    *result = BoolVal(OP::Apply(v1.ptr, v1.len, v2.ptr, v2.len));  \
  }

#define BINARY_COMPARISON_NUMERIC_TYPES(NAME, OP)           \
  BINARY_COMPARISON_NUMERIC_FN_HIDE_NULL(NAME, BoolVal, OP) \
  BINARY_COMPARISON_NUMERIC_FN_HIDE_NULL(NAME, Integer, OP) \
  BINARY_COMPARISON_NUMERIC_FN_HIDE_NULL(NAME, Real, OP)    \
  BINARY_COMPARISON_STRING_FN_HIDE_NULL(NAME, StringVal, OP)

BINARY_COMPARISON_NUMERIC_TYPES(Eq, Equal);
BINARY_COMPARISON_NUMERIC_TYPES(Ge, GreaterThanEqual);
BINARY_COMPARISON_NUMERIC_TYPES(Gt, GreaterThan);
BINARY_COMPARISON_NUMERIC_TYPES(Le, LessThanEqual);
BINARY_COMPARISON_NUMERIC_TYPES(Lt, LessThan);
BINARY_COMPARISON_NUMERIC_TYPES(Ne, NotEqual);

#undef BINARY_COMPARISON_NUMERIC_TYPES
#undef BINARY_COMPARISON_STRING_FN_HIDE_NULL
#undef BINARY_COMPARISON_NUMERIC_FN_HIDE_NULL

}  // namespace tpl::sql
