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
  static void CastToBoolVal(BoolVal *result, const StringVal &v);

  static void CastToInteger(Integer *result, const BoolVal &v);
  static void CastToInteger(Integer *result, const Real &v);
  static void CastToInteger(Integer *result, const StringVal &v);

  static void CastToReal(Real *result, const BoolVal &v);
  static void CastToReal(Real *result, const Integer &v);
  static void CastToReal(Real *result, const StringVal &v);

  static void CastToDateVal(DateVal *result, const TimestampVal &v);
  static void CastToDateVal(DateVal *result, const StringVal &v);

  static void CastToTimestampVal(TimestampVal *result, const DateVal &v);
  static void CastToTimestampVal(TimestampVal *result, const StringVal &v);

  static void CastToStringVal(StringVal *result, ExecutionContext *ctx, const BoolVal &v);
  static void CastToStringVal(StringVal *result, ExecutionContext *ctx, const Integer &v);
  static void CastToStringVal(StringVal *result, ExecutionContext *ctx, const Real &v);
  static void CastToStringVal(StringVal *result, ExecutionContext *ctx, const DateVal &v);
  static void CastToStringVal(StringVal *result, ExecutionContext *ctx, const TimestampVal &v);
};

// ---------------------------------------------------------
// Implementation below
// ---------------------------------------------------------

// The functions below are inlined in the header for performance. Don't move it unless you know what
// you're doing.

// TODO(pmenon): Catch cast exceptions!

#define CAST_HIDE_NULL_FAST(FROM_TYPE, TO_TYPE)                                        \
  inline void CastingFunctions::CastTo##TO_TYPE(TO_TYPE *result, const FROM_TYPE &v) { \
    using InputType = decltype(FROM_TYPE::val);                                        \
    using OutputType = decltype(TO_TYPE::val);                                         \
    result->is_null = v.is_null;                                                       \
    tpl::sql::TryCast<InputType, OutputType>{}(v.val, &result->val);                   \
  }

#define CAST_HIDE_NULL(FROM_TYPE, TO_TYPE)                                             \
  inline void CastingFunctions::CastTo##TO_TYPE(TO_TYPE *result, const FROM_TYPE &v) { \
    using InputType = decltype(FROM_TYPE::val);                                        \
    using OutputType = decltype(TO_TYPE::val);                                         \
    if (v.is_null) {                                                                   \
      *result = TO_TYPE::Null();                                                       \
      return;                                                                          \
    }                                                                                  \
    OutputType output;                                                                 \
    tpl::sql::TryCast<InputType, OutputType>{}(v.val, &output);                        \
    *result = TO_TYPE(output);                                                         \
  }

CAST_HIDE_NULL_FAST(Integer, BoolVal);
CAST_HIDE_NULL_FAST(Real, BoolVal);
CAST_HIDE_NULL(StringVal, BoolVal);
CAST_HIDE_NULL_FAST(BoolVal, Integer);
CAST_HIDE_NULL_FAST(Real, Integer);
CAST_HIDE_NULL(StringVal, Integer);
CAST_HIDE_NULL_FAST(BoolVal, Real);
CAST_HIDE_NULL_FAST(Integer, Real);
CAST_HIDE_NULL(StringVal, Real);
CAST_HIDE_NULL_FAST(TimestampVal, DateVal);
CAST_HIDE_NULL_FAST(DateVal, TimestampVal);

#undef CAST_HIDE_NULL
#undef CAST_HIDE_NULL_FAST

#define CAST_TO_STRING(FROM_TYPE)                                                               \
  inline void CastingFunctions::CastToStringVal(StringVal *result, ExecutionContext *const ctx, \
                                                const FROM_TYPE &v) {                           \
    /*                                                                                          \
     * TODO(pmenon): Perform an explicit if-check here because we expect string                 \
     *               parsing to be more costly than a branch mis-prediction.                    \
     *               Verify!                                                                    \
     */                                                                                         \
    if (v.is_null) {                                                                            \
      *result = StringVal::Null();                                                              \
      return;                                                                                   \
    }                                                                                           \
    const auto str = tpl::sql::Cast<decltype(FROM_TYPE::val), std::string>{}(v.val);            \
    *result = StringVal(ctx->GetStringHeap()->AddVarlen(str));                                  \
  }

CAST_TO_STRING(BoolVal);
CAST_TO_STRING(Integer);
CAST_TO_STRING(Real);
CAST_TO_STRING(DateVal);
CAST_TO_STRING(TimestampVal);

#undef CAST_TO_STRING

}  // namespace tpl::sql
