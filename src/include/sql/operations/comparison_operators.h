#pragma once

#include <algorithm>
#include <cstring>

#include "util/common.h"

namespace tpl::sql {

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
static inline i32 CompareStrings(const char *str1, const std::size_t len1,
                                 const char *str2, const std::size_t len2) {
  const auto min_len = std::min(len1, len2);
  const auto result = (min_len == 0) ? 0 : std::memcmp(str1, str2, min_len);
  if (result != 0) {
    return result;
  }
  return len1 - len2;
}

/**
 * Scalar equality.
 */
struct Equal {
  template <typename T>
  static bool Apply(T left, T right) {
    return left == right;
  }

  static bool Apply(const char *str1, const std::size_t len1, const char *str2,
                    const std::size_t len2) {
    return CompareStrings(str1, len1, str2, len2) == 0;
  }
};

template <>
inline bool Equal::Apply(const char *left, const char *right) {
  return std::strcmp(left, right) == 0;
}

/**
 * Scalar greater-than.
 */
struct GreaterThan {
  template <typename T>
  static bool Apply(T left, T right) {
    return left > right;
  }

  static bool Apply(const char *str1, const std::size_t len1, const char *str2,
                    const std::size_t len2) {
    return CompareStrings(str1, len1, str2, len2) > 0;
  }
};

template <>
inline bool GreaterThan::Apply(const char *left, const char *right) {
  return std::strcmp(left, right) > 0;
}

/**
 * Scalar greater-than-equal.
 */
struct GreaterThanEqual {
  template <typename T>
  static bool Apply(T left, T right) {
    return left >= right;
  }

  static bool Apply(const char *str1, const std::size_t len1, const char *str2,
                    const std::size_t len2) {
    return CompareStrings(str1, len1, str2, len2) >= 0;
  }
};

template <>
inline bool GreaterThanEqual::Apply(const char *left, const char *right) {
  return std::strcmp(left, right) >= 0;
}

/**
 * Scalar less-than.
 */

struct LessThan {
  template <typename T>
  static bool Apply(T left, T right) {
    return left < right;
  }

  static bool Apply(const char *str1, const std::size_t len1, const char *str2,
                    const std::size_t len2) {
    return CompareStrings(str1, len1, str2, len2) < 0;
  }
};

template <>
inline bool LessThan::Apply(const char *left, const char *right) {
  return std::strcmp(left, right) < 0;
}

/**
 * Scalar less-than-equal.
 */
struct LessThanEqual {
  template <typename T>
  static bool Apply(T left, T right) {
    return left <= right;
  }

  static bool Apply(const char *str1, const std::size_t len1, const char *str2,
                    const std::size_t len2) {
    return CompareStrings(str1, len1, str2, len2) <= 0;
  }
};

template <>
inline bool LessThanEqual::Apply(const char *left, const char *right) {
  return std::strcmp(left, right) <= 0;
}

/**
 * Scalar inequality.
 */
struct NotEqual {
  template <typename T>
  static bool Apply(T left, T right) {
    return left != right;
  }

  static bool Apply(const char *str1, const std::size_t len1, const char *str2,
                    const std::size_t len2) {
    return CompareStrings(str1, len1, str2, len2) != 0;
  }
};

template <>
inline bool NotEqual::Apply(const char *left, const char *right) {
  return std::strcmp(left, right) != 0;
}

}  // namespace tpl::sql
