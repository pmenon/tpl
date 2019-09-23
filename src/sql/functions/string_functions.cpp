#include "sql/functions/string_functions.h"

#include <algorithm>
#include <bitset>
#include <string>

#include "sql/execution_context.h"
#include "sql/operations/like_operators.h"

namespace tpl::sql {

void StringFunctions::Substring(UNUSED ExecutionContext *ctx, StringVal *result,
                                const StringVal &str, const Integer &pos, const Integer &len) {
  if (str.is_null || pos.is_null || len.is_null) {
    *result = StringVal::Null();
    return;
  }

  const auto start = std::max(pos.val, int64_t{1});
  const auto end = pos.val + std::min(static_cast<int64_t>(str.GetLength()), len.val);

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
  *result = StringVal(str.GetContent() + start - 1, end - start);
}

namespace {

const char *SearchSubstring(const char *haystack, const std::size_t hay_len, const char *needle,
                            const std::size_t needle_len) {
  TPL_ASSERT(needle != nullptr, "No search string provided");
  TPL_ASSERT(needle_len > 0, "No search string provided");
  for (uint32_t i = 0; i < hay_len + needle_len; i++) {
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

  if (delim.GetLength() == 0) {
    *result = str;
    return;
  }

  // Pointers to the start of the current part, the end of the input string, and
  // the delimiter string
  auto curr = str.GetContent();
  auto const end = curr + str.GetLength();
  auto const delimiter = delim.GetContent();

  for (uint32_t index = 1;; index++) {
    const auto remaining_len = end - curr;
    const auto next_delim = SearchSubstring(curr, remaining_len, delimiter, delim.GetLength());
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
    curr = next_delim + delim.GetLength();
  }
}

void StringFunctions::Repeat(ExecutionContext *ctx, StringVal *result, const StringVal &str,
                             const Integer &n) {
  if (str.is_null || n.is_null) {
    *result = StringVal::Null();
    return;
  }

  if (str.GetLength() == 0 || n.val <= 0) {
    *result = StringVal("");
    return;
  }

  // Allocate
  const std::size_t result_len = str.GetLength() * n.val;
  char *target = ctx->string_allocator()->PreAllocate(result_len);

  // Repeat
  char *ptr = target;
  for (uint32_t i = 0; i < n.val; i++) {
    std::memcpy(ptr, str.GetContent(), str.GetLength());
    ptr += str.GetLength();
  }

  // Set result
  *result = StringVal(target, result_len);
}

void StringFunctions::Lpad(ExecutionContext *ctx, StringVal *result, const StringVal &str,
                           const Integer &len, const StringVal &pad) {
  if (str.is_null || len.is_null || pad.is_null || len.val < 0) {
    *result = StringVal::Null();
    return;
  }

  // If target length equals input length, nothing to do
  if (static_cast<std::size_t>(len.val) == str.GetLength()) {
    *result = str;
    return;
  }

  // If target length is less than input length, truncate.
  if (static_cast<std::size_t>(len.val) < str.GetLength()) {
    *result = StringVal(str.GetContent(), len.val);
    return;
  }

  // Allocate some memory
  char *target = ctx->string_allocator()->PreAllocate(len.val);

  // Pad
  char *ptr = target;
  for (std::size_t bytes_left = len.val - str.GetLength(); bytes_left > 0;) {
    auto copy_len = std::min(pad.GetLength(), bytes_left);
    std::memcpy(ptr, pad.GetContent(), copy_len);
    bytes_left -= copy_len;
    ptr += copy_len;
  }

  // Copy main string into target
  std::memcpy(ptr, str.GetContent(), str.GetLength());

  // Set result
  *result = StringVal(target, len.val);
}

void StringFunctions::Rpad(ExecutionContext *ctx, StringVal *result, const StringVal &str,
                           const Integer &len, const StringVal &pad) {
  if (str.is_null || len.is_null || pad.is_null || len.val < 0) {
    *result = StringVal::Null();
    return;
  }

  // If target length equals input length, nothing to do
  if (static_cast<std::size_t>(len.val) == str.GetLength()) {
    *result = str;
    return;
  }

  // If target length is less than input length, truncate.
  if (static_cast<std::size_t>(len.val) < str.GetLength()) {
    *result = StringVal(str.GetContent(), len.val);
    return;
  }

  // Allocate output
  char *target = ctx->string_allocator()->PreAllocate(len.val);
  char *ptr = target;

  // Copy input string first
  std::memcpy(ptr, str.GetContent(), str.GetLength());
  ptr += str.GetLength();

  // Then padding
  for (std::size_t bytes_left = len.val - str.GetLength(); bytes_left > 0;) {
    auto copy_len = std::min(pad.GetLength(), bytes_left);
    std::memcpy(ptr, pad.GetContent(), copy_len);
    bytes_left -= copy_len;
    ptr += copy_len;
  }

  // Set result
  *result = StringVal(target, len.val);
}

void StringFunctions::Length(UNUSED ExecutionContext *ctx, Integer *result, const StringVal &str) {
  result->is_null = str.is_null;
  result->val = str.GetLength();
}

void StringFunctions::Lower(ExecutionContext *ctx, StringVal *result, const StringVal &str) {
  if (str.is_null) {
    *result = StringVal::Null();
    return;
  }

  char *target = ctx->string_allocator()->PreAllocate(str.GetLength());
  std::transform(str.GetContent(), str.GetContent() + str.GetLength(), target, ::tolower);
  *result = StringVal(target, str.GetLength());
}

void StringFunctions::Upper(ExecutionContext *ctx, StringVal *result, const StringVal &str) {
  if (str.is_null) {
    *result = StringVal::Null();
    return;
  }

  char *target = ctx->string_allocator()->PreAllocate(str.GetLength());
  std::transform(str.GetContent(), str.GetContent() + str.GetLength(), target, ::toupper);
  *result = StringVal(target, str.GetLength());
}

void StringFunctions::Reverse(ExecutionContext *ctx, StringVal *result, const StringVal &str) {
  if (str.is_null) {
    *result = StringVal::Null();
    return;
  }

  if (str.GetLength() == 0) {
    *result = str;
    return;
  }

  char *target = ctx->string_allocator()->PreAllocate(str.GetLength());
  std::reverse_copy(str.GetContent(), str.GetContent() + str.GetLength(), target);
  *result = StringVal(target, str.GetLength());
}

namespace {

// TODO(pmenon): The bitset we use can be prepared once before all function invocations if the
//               characters list is a constant value, and not a column value
//               (i.e., SELECT ltrim(col, 'abc') FROM ...). This should be populated in the
//               execution context once apriori rather that initializing it each invocation.
// TODO(pmenon): What about non-ASCII strings?
// Templatized from Postgres
template <bool TrimLeft, bool TrimRight>
void DoTrim(StringVal *result, const StringVal &str, const StringVal &chars) {
  if (str.is_null || chars.is_null) {
    *result = StringVal::Null();
    return;
  }

  if (str.GetLength() == 0) {
    *result = str;
    return;
  }

  std::bitset<256> bitset;
  for (uint32_t i = 0; i < chars.GetLength(); i++) {
    bitset.set(chars.GetContent()[i]);
  }

  // The valid range
  int32_t begin = 0, end = str.GetLength() - 1;

  if constexpr (TrimLeft) {
    while (begin < static_cast<int32_t>(str.GetLength()) && bitset.test(str.GetContent()[begin])) {
      begin++;
    }
  }

  if constexpr (TrimRight) {
    while (begin <= end && bitset.test(str.GetContent()[end])) {
      end--;
    }
  }

  *result = StringVal(str.GetContent() + begin, end - begin + 1);
}

}  // namespace

void StringFunctions::Trim(UNUSED ExecutionContext *ctx, StringVal *result, const StringVal &str) {
  DoTrim<true, true>(result, str, StringVal(" "));
}

void StringFunctions::Trim(UNUSED ExecutionContext *ctx, StringVal *result, const StringVal &str,
                           const StringVal &chars) {
  DoTrim<true, true>(result, str, chars);
}

void StringFunctions::Ltrim(UNUSED ExecutionContext *ctx, StringVal *result, const StringVal &str) {
  DoTrim<true, false>(result, str, StringVal(" "));
}

void StringFunctions::Ltrim(UNUSED ExecutionContext *ctx, StringVal *result, const StringVal &str,
                            const StringVal &chars) {
  DoTrim<true, false>(result, str, chars);
}

void StringFunctions::Rtrim(UNUSED ExecutionContext *ctx, StringVal *result, const StringVal &str) {
  DoTrim<false, true>(result, str, StringVal(" "));
}

void StringFunctions::Rtrim(UNUSED ExecutionContext *ctx, StringVal *result, const StringVal &str,
                            const StringVal &chars) {
  DoTrim<false, true>(result, str, chars);
}

void StringFunctions::Left(UNUSED ExecutionContext *ctx, StringVal *result, const StringVal &str,
                           const Integer &n) {
  if (str.is_null || n.is_null) {
    *result = StringVal::Null();
    return;
  }

  const auto len = n.val < 0 ? std::max(int64_t(0), static_cast<int64_t>(str.GetLength()) + n.val)
                             : std::min(str.GetLength(), static_cast<std::size_t>(n.val));
  *result = StringVal(str.GetContent(), len);
}

void StringFunctions::Right(UNUSED ExecutionContext *ctx, StringVal *result, const StringVal &str,
                            const Integer &n) {
  if (str.is_null || n.is_null) {
    *result = StringVal::Null();
    return;
  }

  const auto len = std::min(str.GetLength(), static_cast<std::size_t>(std::abs(n.val)));
  if (n.val > 0) {
    *result = StringVal(str.GetContent() + (str.GetLength() - len), len);
  } else {
    *result = StringVal(str.GetContent() + len, str.GetLength() - len);
  }
}

void StringFunctions::Like(UNUSED ExecutionContext *ctx, BoolVal *result, const StringVal &string,
                           const StringVal &pattern) {
  if (string.is_null || pattern.is_null) {
    *result = BoolVal::Null();
    return;
  }

  result->is_null = false;
  result->val = Like::Apply(string.GetContent(), string.GetLength(), pattern.GetContent(),
                            pattern.GetLength());
}

}  // namespace tpl::sql
