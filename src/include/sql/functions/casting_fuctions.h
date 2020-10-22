#pragma once

#include <string>

#include "sql/execution_context.h"
#include "sql/operators/cast_operators.h"
#include "sql/value.h"

namespace tpl::sql {

class ExecutionContext;

/**
 * Utility class to handle various SQL casting functions.
 */
class CastingFunctions : public AllStatic {
 public:
  static void CastToBoolVal(BoolVal *result, const Integer &input);
  static void CastToBoolVal(BoolVal *result, const Real &input);
  static void CastToBoolVal(BoolVal *result, const StringVal &input);

  static void CastToInteger(Integer *result, const BoolVal &input);
  static void CastToInteger(Integer *result, const Real &input);
  static void CastToInteger(Integer *result, const StringVal &input);

  static void CastToReal(Real *result, const BoolVal &input);
  static void CastToReal(Real *result, const Integer &input);
  static void CastToReal(Real *result, const StringVal &input);

  static void CastToDateVal(DateVal *result, const TimestampVal &input);
  static void CastToDateVal(DateVal *result, const StringVal &input);

  static void CastToTimestampVal(TimestampVal *result, const DateVal &input);
  static void CastToTimestampVal(TimestampVal *result, const StringVal &input);

  static void CastToStringVal(StringVal *result, ExecutionContext *ctx, const BoolVal &input);
  static void CastToStringVal(StringVal *result, ExecutionContext *ctx, const Integer &input);
  static void CastToStringVal(StringVal *result, ExecutionContext *ctx, const Real &input);
  static void CastToStringVal(StringVal *result, ExecutionContext *ctx, const DateVal &input);
  static void CastToStringVal(StringVal *result, ExecutionContext *ctx, const TimestampVal &input);
};

// ---------------------------------------------------------
// Implementation below
// ---------------------------------------------------------

// The functions below are inlined in the header for performance. Don't move it unless you know what
// you're doing.

// TODO(pmenon): Catch cast exceptions!

#define CAST_HIDE_NULL_FAST(FROM_TYPE, TO_TYPE)                                            \
  inline void CastingFunctions::CastTo##TO_TYPE(TO_TYPE *result, const FROM_TYPE &input) { \
    using InputType = decltype(FROM_TYPE::val);                                            \
    using OutputType = decltype(TO_TYPE::val);                                             \
    result->is_null = input.is_null;                                                       \
    tpl::sql::TryCast<InputType, OutputType>{}(input.val, &result->val);                   \
  }

#define CAST_HIDE_NULL(FROM_TYPE, TO_TYPE)                                                 \
  inline void CastingFunctions::CastTo##TO_TYPE(TO_TYPE *result, const FROM_TYPE &input) { \
    using InputType = decltype(FROM_TYPE::val);                                            \
    using OutputType = decltype(TO_TYPE::val);                                             \
    if (input.is_null) {                                                                   \
      *result = TO_TYPE::Null();                                                           \
      return;                                                                              \
    }                                                                                      \
    OutputType output{};                                                                   \
    tpl::sql::TryCast<InputType, OutputType>{}(input.val, &output);                        \
    *result = TO_TYPE(output);                                                             \
  }

// Something to boolean.
CAST_HIDE_NULL_FAST(Integer, BoolVal);
CAST_HIDE_NULL_FAST(Real, BoolVal);
CAST_HIDE_NULL(StringVal, BoolVal);
// Something to integer.
CAST_HIDE_NULL_FAST(BoolVal, Integer);
CAST_HIDE_NULL_FAST(Real, Integer);
CAST_HIDE_NULL(StringVal, Integer);
// Something to real.
CAST_HIDE_NULL_FAST(BoolVal, Real);
CAST_HIDE_NULL_FAST(Integer, Real);
CAST_HIDE_NULL(StringVal, Real);
// Something to date.
CAST_HIDE_NULL_FAST(TimestampVal, DateVal);
CAST_HIDE_NULL(StringVal, DateVal);
// Something to timestamp.
CAST_HIDE_NULL_FAST(DateVal, TimestampVal);
CAST_HIDE_NULL(StringVal, TimestampVal);

#undef CAST_HIDE_NULL
#undef CAST_HIDE_NULL_FAST

// Something to string.
#define CAST_TO_STRING(FROM_TYPE)                                                               \
  inline void CastingFunctions::CastToStringVal(StringVal *result, ExecutionContext *const ctx, \
                                                const FROM_TYPE &input) {                       \
    /*                                                                                          \
     * TODO(pmenon): Perform an explicit if-check here because we expect string                 \
     *               parsing to be costlier than a branch mis-prediction. Check!                \
     */                                                                                         \
    if (input.is_null) {                                                                        \
      *result = StringVal::Null();                                                              \
      return;                                                                                   \
    }                                                                                           \
    const auto str = tpl::sql::Cast<decltype(FROM_TYPE::val), std::string>{}(input.val);        \
    *result = StringVal(ctx->GetStringHeap()->AddVarlen(str));                                  \
  }

CAST_TO_STRING(BoolVal);
CAST_TO_STRING(Integer);
CAST_TO_STRING(Real);
CAST_TO_STRING(DateVal);
CAST_TO_STRING(TimestampVal);

#undef CAST_TO_STRING

}  // namespace tpl::sql
