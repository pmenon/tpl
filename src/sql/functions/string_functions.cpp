#include "sql/functions/string_functions.h"

#include <algorithm>
#include <limits>

namespace tpl::sql {

void StringFunctions::Substring(UNUSED ExecutionContext *ctx, StringVal *result,
                                const StringVal &str, const Integer &pos,
                                const Integer &len) {
  if (str.is_null || pos.is_null || len.is_null) {
    *result = StringVal::Null();
    return;
  }

  const auto start = std::max(pos.val, 1l);
  const auto end = pos.val + std::min(static_cast<i64>(str.len), len.val);

  // The end can be before the start only if the length was negative. This is an
  // error.
  if (end < pos.val) {
    *result = StringVal::Null();
    return;
  }

  // If start is negative, return empty string
  if (end < 1) {
    *result = StringVal("");
    return;
  }

  // All good
  *result = StringVal(str.ptr + start - 1, end - start);
}

void StringFunctions::Substring(ExecutionContext *ctx, StringVal *result,
                                const StringVal &str, const Integer &pos) {
  Substring(ctx, result, str, pos, Integer(std::numeric_limits<i64>::max()));
}

void StringFunctions::SplitPart(ExecutionContext *ctx, StringVal *result,
                                const StringVal &str, const StringVal &delim,
                                const Integer &field) {}

void StringFunctions::Repeat(ExecutionContext *ctx, StringVal *result,
                             const StringVal &str, const Integer &n) {}

void StringFunctions::Lpad(ExecutionContext *ctx, StringVal *result,
                           const StringVal &str, const Integer &len,
                           const StringVal &pad) {}

void StringFunctions::Rpad(ExecutionContext *ctx, StringVal *result,
                           const StringVal &str, const Integer &,
                           const StringVal &pad) {}

void StringFunctions::Length(ExecutionContext *ctx, Integer *result,
                             const StringVal &str) {}

void StringFunctions::CharLength(ExecutionContext *ctx, Integer *result,
                                 const StringVal &str) {}

void StringFunctions::Lower(ExecutionContext *ctx, StringVal *result,
                            const StringVal &str) {}

void StringFunctions::Upper(ExecutionContext *ctx, StringVal *result,
                            const StringVal &str) {}

void StringFunctions::Replace(ExecutionContext *ctx, StringVal *result,
                              const StringVal &str, const StringVal &pattern,
                              const StringVal &replace) {}

void StringFunctions::Reverse(ExecutionContext *ctx, StringVal *result,
                              const StringVal &str) {}

void StringFunctions::Trim(ExecutionContext *ctx, StringVal *result,
                           const StringVal &str) {}

void StringFunctions::Ltrim(ExecutionContext *ctx, StringVal *result,
                            const StringVal &str) {}

void StringFunctions::Rtrim(ExecutionContext *ctx, StringVal *result,
                            const StringVal &str) {}

}  // namespace tpl::sql
