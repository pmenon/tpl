#pragma once

#include <algorithm>
#include <cstring>

#include "common/common.h"
#include "sql/runtime_types.h"

namespace tpl::sql {

struct Equal;
struct GreaterThan;
struct GreaterThanEqual;
struct LessThan;
struct LessThanEqual;
struct NotEqual;

/**
 * Compare two strings. Returns
 * < 0 if s1 < s2
 * 0 if s1 == s2
 * > 0 if s1 > s2
 *
 * @param v1 The first string.
 * @param v2 The second string.
 * @return The appropriate signed value indicating comparison order.
 */
inline int32_t CompareStrings(const void *str1, const std::size_t len1, const void *str2,
                              const std::size_t len2) {
  const auto min_len = std::min(len1, len2);
  const auto result = (min_len == 0) ? 0 : std::memcmp(str1, str2, min_len);
  if (result != 0) {
    return result;
  }
  return len1 - len2;
}

/**
 * Equality operator.
 */
struct Equal {
  using SymmetricOp = Equal;

  template <typename T>
  static bool Apply(T left, T right) {
    return left == right;
  }

  static bool Apply(const void *left_buf, const std::size_t len1, const char *right_buf,
                    const std::size_t len2) {
    return CompareStrings(left_buf, len1, right_buf, len2) == 0;
  }
};

/**
 * Greater-than operator.
 */
struct GreaterThan {
  using SymmetricOp = LessThan;

  template <typename T>
  static bool Apply(T left, T right) {
    return left > right;
  }

  static bool Apply(const char *str1, const std::size_t len1, const char *str2,
                    const std::size_t len2) {
    return CompareStrings(str1, len1, str2, len2) > 0;
  }
};

/**
 * Greater-than or equal operator.
 */
struct GreaterThanEqual {
  using SymmetricOp = LessThanEqual;

  template <typename T>
  static bool Apply(T left, T right) {
    return left >= right;
  }

  static bool Apply(const char *str1, const std::size_t len1, const char *str2,
                    const std::size_t len2) {
    return CompareStrings(str1, len1, str2, len2) >= 0;
  }
};

/**
 * Less-than operator.
 */
struct LessThan {
  using SymmetricOp = GreaterThan;

  template <typename T>
  static bool Apply(T left, T right) {
    return left < right;
  }

  static bool Apply(const char *str1, const std::size_t len1, const char *str2,
                    const std::size_t len2) {
    return CompareStrings(str1, len1, str2, len2) < 0;
  }
};

/**
 * Less-than or equal operator.
 */
struct LessThanEqual {
  using SymmetricOp = GreaterThanEqual;

  template <typename T>
  static bool Apply(T left, T right) {
    return left <= right;
  }

  static bool Apply(const char *str1, const std::size_t len1, const char *str2,
                    const std::size_t len2) {
    return CompareStrings(str1, len1, str2, len2) <= 0;
  }
};

/**
 * Inequality operator.
 */
struct NotEqual {
  using SymmetricOp = NotEqual;

  template <typename T>
  static bool Apply(T left, T right) {
    return left != right;
  }

  static bool Apply(const char *str1, const std::size_t len1, const char *str2,
                    const std::size_t len2) {
    return CompareStrings(str1, len1, str2, len2) != 0;
  }
};

}  // namespace tpl::sql
