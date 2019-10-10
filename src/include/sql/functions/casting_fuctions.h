#pragma once

#include "sql/operations/cast_operators.h"
#include "sql/value.h"

namespace tpl::sql {

class ExecutionContext;

/**
 * Utility class to handle various SQL casting functions.
 */
class CastingFunctions : public AllStatic {
 public:
  static void CastToBoolVal(BoolVal *result, const Integer &v);
  static void CastToBoolVal(BoolVal *result, const Real &v);
  static void CastToBoolVal(BoolVal *result, const DateVal &v);
  static void CastToBoolVal(BoolVal *result, const TimestampVal &v);

  static void CastToInteger(Integer *result, const BoolVal &v);
  static void CastToInteger(Integer *result, const Real &v);
  static void CastToInteger(Integer *result, const StringVal &v);
  static void CastToInteger(Integer *result, const DateVal &v);
  static void CastToInteger(Integer *result, const TimestampVal &v);

  static void CastToReal(Real *result, const BoolVal &v);
  static void CastToReal(Real *result, const Integer &v);
  static void CastToReal(Real *result, const StringVal &v);
  static void CastToReal(Real *result, const DateVal &v);
  static void CastToReal(Real *result, const TimestampVal &v);

  static void CastToStringVal(ExecutionContext *ctx, StringVal *result, const BoolVal &v);
  static void CastToStringVal(ExecutionContext *ctx, StringVal *result, const Integer &v);
  static void CastToStringVal(ExecutionContext *ctx, StringVal *result, const Real &v);
  static void CastToStringVal(ExecutionContext *ctx, StringVal *result, const DateVal &v);
  static void CastToStringVal(ExecutionContext *ctx, StringVal *result, const TimestampVal &v);
};

// ---------------------------------------------------------
// Implementation below
// ---------------------------------------------------------

// The functions below are inlined in the header for performance. Don't move it
// unless you know what you're doing.

#define CAST_HIDE_NULL_FAST(FROM_TYPE, TO_TYPE)                                        \
  inline void CastingFunctions::CastTo##TO_TYPE(TO_TYPE *result, const FROM_TYPE &v) { \
    result->is_null = v.is_null;                                                       \
    result->val = v.val;                                                               \
  }

CAST_HIDE_NULL_FAST(Integer, BoolVal);
CAST_HIDE_NULL_FAST(Real, BoolVal);
CAST_HIDE_NULL_FAST(BoolVal, Integer);
CAST_HIDE_NULL_FAST(Real, Integer);
CAST_HIDE_NULL_FAST(BoolVal, Real);
CAST_HIDE_NULL_FAST(Integer, Real);

#undef CAST_HIDE_NULL_FAST

}  // namespace tpl::sql
