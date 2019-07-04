#include "sql/functions/string_functions.h"

#include <algorithm>

#include "sql/execution_context.h"
#include "util/bit_util.h"

namespace tpl::sql {

void StringFunctions::Substring(UNUSED ExecutionContext *ctx, StringVal *result,
                                const StringVal &str, const Integer &pos,
                                const Integer &len) {
  if (str.is_null || pos.is_null || len.is_null) {
    *result = StringVal::Null();
    return;
  }

  const auto start = std::max(pos.val, i64{1});
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
        *result = StringVal(curr, remaining_len);
      } else {
        *result = StringVal("");
      }
      return;
    }
    // Are we at the correct field?
    if (index == field.val) {
      *result = StringVal(curr, next_delim - curr);
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
                           const StringVal &pad) {
  if (str.is_null || len.is_null || pad.is_null || len.val < 0) {
    *result = StringVal::Null();
    return;
  }

  // If target length equals input length, nothing to do
  if (len.val == str.len) {
    *result = str;
    return;
  }

  // If target length is less than input length, truncate.
  if (len.val < str.len) {
    *result = StringVal(str.ptr, len.val);
    return;
  }

  *result = StringVal(ctx->string_allocator(), len.val);

  if (TPL_UNLIKELY(result->is_null)) {
    // Allocation failed
    return;
  }

  auto *ptr = result->ptr;
  for (u32 bytes_left = len.val - str.len; bytes_left > 0;) {
    auto copy_len = std::min(pad.len, bytes_left);
    std::memcpy(ptr, pad.ptr, copy_len);
    bytes_left -= copy_len;
    ptr += copy_len;
  }

  std::memcpy(ptr, str.ptr, str.len);
}

void StringFunctions::Rpad(ExecutionContext *ctx, StringVal *result,
                           const StringVal &str, const Integer &len,
                           const StringVal &pad) {
  if (str.is_null || len.is_null || pad.is_null || len.val < 0) {
    *result = StringVal::Null();
    return;
  }

  // If target length equals input length, nothing to do
  if (len.val == str.len) {
    *result = str;
    return;
  }

  // If target length is less than input length, truncate.
  if (len.val < str.len) {
    *result = StringVal(str.ptr, len.val);
    return;
  }

  *result = StringVal(ctx->string_allocator(), len.val);

  if (TPL_UNLIKELY(result->is_null)) {
    // Allocation failed
    return;
  }

  // Copy input string first
  auto *ptr = result->ptr;
  std::memcpy(ptr, str.ptr, str.len);
  ptr += str.len;

  // Then padding
  for (u32 bytes_left = len.val - str.len; bytes_left > 0;) {
    auto copy_len = std::min(pad.len, bytes_left);
    std::memcpy(ptr, pad.ptr, copy_len);
    bytes_left -= copy_len;
    ptr += copy_len;
  }
}

void StringFunctions::Length(UNUSED ExecutionContext *ctx, Integer *result,
                             const StringVal &str) {
  result->is_null = str.is_null;
  result->val = str.len;
}

void StringFunctions::Lower(ExecutionContext *ctx, StringVal *result,
                            const StringVal &str) {
  if (str.is_null) {
    *result = StringVal::Null();
    return;
  }

  *result = StringVal(ctx->string_allocator(), str.len);

  if (TPL_UNLIKELY(result->is_null)) {
    // Allocation failed
    return;
  }

  auto *ptr = reinterpret_cast<char *>(result->ptr);
  auto *src = reinterpret_cast<char *>(str.ptr);
  for (u32 i = 0; i < str.len; i++) {
    ptr[i] = std::tolower(src[i]);
  }
}

void StringFunctions::Upper(ExecutionContext *ctx, StringVal *result,
                            const StringVal &str) {
  if (str.is_null) {
    *result = StringVal::Null();
    return;
  }

  *result = StringVal(ctx->string_allocator(), str.len);

  if (TPL_UNLIKELY(result->is_null)) {
    // Allocation failed
    return;
  }

  auto *ptr = reinterpret_cast<char *>(result->ptr);
  auto *src = reinterpret_cast<char *>(str.ptr);
  for (u32 i = 0; i < str.len; i++) {
    ptr[i] = std::toupper(src[i]);
  }
}

void StringFunctions::Reverse(ExecutionContext *ctx, StringVal *result,
                              const StringVal &str) {
  if (str.is_null) {
    *result = StringVal::Null();
    return;
  }

  if (str.len == 0) {
    *result = str;
    return;
  }

  *result = StringVal(ctx->string_allocator(), str.len);

  if (TPL_UNLIKELY(result->is_null)) {
    // Allocation failed
    return;
  }

  std::reverse_copy(str.ptr, str.ptr + str.len, result->ptr);
}

namespace {

// TODO(pmenon): The bitset we use can be prepared once before all function
//               invocations if the characters list is a constant value, and not
//               a column value (i.e., SELECT ltrim(col, 'abc') FROM ...). This
//               should be populated in the execution context once apriori
//               rather that initializing it each invocation.
// TODO(pmenon): What about non-ASCII strings?
// Templatized from Postgres
template <bool TrimLeft, bool TrimRight>
void DoTrim(StringVal *result, const StringVal &str, const StringVal &chars) {
  if (str.is_null || chars.is_null) {
    *result = StringVal::Null();
    return;
  }

  if (str.len == 0) {
    *result = str;
    return;
  }

  util::InlinedBitVector<256> bitset;
  for (u32 i = 0; i < chars.len; i++) {
    bitset.Set(chars.ptr[i]);
  }

  // The valid range
  i32 begin = 0, end = str.len - 1;

  if constexpr (TrimLeft) {
    while (begin < static_cast<i32>(str.len) && bitset.Test(str.ptr[begin])) {
      begin++;
    }
  }

  if constexpr (TrimRight) {
    while (begin <= end && bitset.Test(str.ptr[end])) {
      end--;
    }
  }

  *result = StringVal(str.ptr + begin, end - begin + 1);
}

}  // namespace

void StringFunctions::Trim(UNUSED ExecutionContext *ctx, StringVal *result,
                           const StringVal &str) {
  DoTrim<true, true>(result, str, StringVal(" "));
}

void StringFunctions::Trim(UNUSED ExecutionContext *ctx, StringVal *result,
                           const StringVal &str, const StringVal &chars) {
  DoTrim<true, true>(result, str, chars);
}

void StringFunctions::Ltrim(UNUSED ExecutionContext *ctx, StringVal *result,
                            const StringVal &str) {
  DoTrim<true, false>(result, str, StringVal(" "));
}

void StringFunctions::Ltrim(UNUSED ExecutionContext *ctx, StringVal *result,
                            const StringVal &str, const StringVal &chars) {
  DoTrim<true, false>(result, str, chars);
}

void StringFunctions::Rtrim(UNUSED ExecutionContext *ctx, StringVal *result,
                            const StringVal &str) {
  DoTrim<false, true>(result, str, StringVal(" "));
}

void StringFunctions::Rtrim(UNUSED ExecutionContext *ctx, StringVal *result,
                            const StringVal &str, const StringVal &chars) {
  DoTrim<false, true>(result, str, chars);
}

void StringFunctions::Left(UNUSED ExecutionContext *ctx, StringVal *result,
                           const StringVal &str, const Integer &n) {
  if (str.is_null || n.is_null) {
    *result = StringVal::Null();
    return;
  }

  const auto len = n.val < 0 ? std::max(i64{0}, str.len + n.val)
                             : std::min(str.len, static_cast<u32>(n.val));
  *result = StringVal(str.ptr, len);
}

void StringFunctions::Right(UNUSED ExecutionContext *ctx, StringVal *result,
                            const StringVal &str, const Integer &n) {
  if (str.is_null || n.is_null) {
    *result = StringVal::Null();
    return;
  }

  const auto len = std::min(str.len, static_cast<u32>(std::abs(n.val)));
  if (n.val > 0) {
    *result = StringVal(str.ptr + (str.len - len), len);
  } else {
    *result = StringVal(str.ptr + len, str.len - len);
  }
}

}  // namespace tpl::sql
