#include "sql/functions/string_functions.h"

#include <algorithm>
#include <limits>

#include "sql/execution_context.h"

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

namespace {

char *SearchSubstring(char *haystack, const std::size_t hay_len,
                      const char *needle, const std::size_t needle_len) {
  TPL_ASSERT(needle != nullptr, "No search string provided");
  TPL_ASSERT(needle_len > 0, "No search string provided");
  for (u32 i = 0; i < hay_len + needle_len; i++) {
    const auto pos = haystack + i;
    if (strncmp(pos, needle, needle_len) == 0) {
      return pos;
    }
  }
  return nullptr;
}

}  // namespace

void StringFunctions::SplitPart(UNUSED ExecutionContext *ctx, StringVal *result,
                                const StringVal &str, const StringVal &delim,
                                const Integer &field) {
  if (str.is_null || delim.is_null || field.is_null) {
    *result = StringVal::Null();
    return;
  }

  if (field.val < 0) {
    // ERROR
    *result = StringVal::Null();
    return;
  }

  if (delim.len == 0) {
    *result = str;
    return;
  }

  // Pointers to the start of the current part, the end of the input string, and
  // the delimiter string
  auto curr = reinterpret_cast<char *>(str.ptr);
  auto const end = curr + str.len;
  auto const delimiter = reinterpret_cast<const char *>(delim.ptr);

  for (u32 index = 1;; index++) {
    const auto remaining_len = end - curr;
    const auto next_delim =
        SearchSubstring(curr, remaining_len, delimiter, delim.len);
    if (next_delim == nullptr) {
      if (index == field.val) {
        *result = StringVal(reinterpret_cast<byte *>(curr), remaining_len);
      } else {
        *result = StringVal("");
      }
      return;
    }
    // Are we at the correct field?
    if (index == field.val) {
      *result = StringVal(reinterpret_cast<byte *>(curr), next_delim - curr);
      return;
    }
    // We haven't reached the field yet, move along
    curr = next_delim + delim.len;
  }
}

void StringFunctions::Repeat(ExecutionContext *ctx, StringVal *result,
                             const StringVal &str, const Integer &n) {
  if (str.is_null || n.is_null) {
    *result = StringVal::Null();
    return;
  }

  if (str.len == 0 || n.val <= 0) {
    *result = StringVal("");
    return;
  }

  *result = StringVal(ctx->string_allocator(), str.len * n.val);

  if (TPL_UNLIKELY(result->is_null)) {
    // Allocation failed
    return;
  }

  auto *ptr = result->ptr;
  for (u32 i = 0; i < n.val; i++) {
    std::memcpy(ptr, str.ptr, str.len);
    ptr += str.len;
  }
}

void StringFunctions::Lpad(ExecutionContext *ctx, StringVal *result,
                           const StringVal &str, const Integer &len,
                           const StringVal &pad) {}

void StringFunctions::Rpad(ExecutionContext *ctx, StringVal *result,
                           const StringVal &str, const Integer &,
                           const StringVal &pad) {}

void StringFunctions::Length(UNUSED ExecutionContext *ctx, Integer *result,
                             const StringVal &str) {
  result->is_null = str.is_null;
  result->val = str.len;
}

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
