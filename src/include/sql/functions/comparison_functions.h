#pragma once

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

#define BINARY_COMPARISON_NUMERIC_FN_HIDE_NULL_FAST(NAME, TYPE, OP)            \
  inline void ComparisonFunctions::NAME##TYPE(BoolVal *result, const TYPE &v1, \
                                              const TYPE &v2) {                \
    result->is_null = (v1.is_null || v2.is_null);                              \
    result->val = v1.val OP v2.val;                                            \
  }

#define BINARY_COMPARISON_NUMERIC_FN_HIDE_NULL_SLOW(NAME, TYPE, OP)            \
  inline void ComparisonFunctions::NAME##TYPE(BoolVal *result, const TYPE &v1, \
                                              const TYPE &v2) {                \
    if (v1.is_null || v2.is_null) {                                            \
      *result = BoolVal::Null();                                               \
      return;                                                                  \
    }                                                                          \
    *result = BoolVal(v1 OP v2);                                               \
  }

#define BINARY_COMPARISON_ALL_TYPES(NAME, OP)                    \
  BINARY_COMPARISON_NUMERIC_FN_HIDE_NULL_FAST(NAME, BoolVal, OP) \
  BINARY_COMPARISON_NUMERIC_FN_HIDE_NULL_FAST(NAME, Integer, OP) \
  BINARY_COMPARISON_NUMERIC_FN_HIDE_NULL_FAST(NAME, Real, OP)    \
  BINARY_COMPARISON_NUMERIC_FN_HIDE_NULL_SLOW(NAME, StringVal, OP)

BINARY_COMPARISON_ALL_TYPES(Eq, ==);
BINARY_COMPARISON_ALL_TYPES(Ge, >=);
BINARY_COMPARISON_ALL_TYPES(Gt, >);
BINARY_COMPARISON_ALL_TYPES(Le, <=);
BINARY_COMPARISON_ALL_TYPES(Lt, <);
BINARY_COMPARISON_ALL_TYPES(Ne, !=);

}  // namespace tpl::sql
