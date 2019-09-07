#pragma once

#include <algorithm>
#include <cstring>

#include "common/common.h"

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
static inline int32_t CompareStrings(const char *str1, const std::size_t len1, const char *str2,
                                     const std::size_t len2) {
  const auto min_len = std::min(len1, len2);
  const auto result = (min_len == 0) ? 0 : std::memcmp(str1, str2, min_len);
  if (result != 0) {
    return result;
  }
  return len1 - len2;
}

/**
 * Equality
 */
struct Equal {
  using SymmetricOp = Equal;

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
 * Greater-than
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

template <>
inline bool GreaterThan::Apply(const char *left, const char *right) {
  return std::strcmp(left, right) > 0;
}

/**
 * Greater-than or equal
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

template <>
inline bool GreaterThanEqual::Apply(const char *left, const char *right) {
  return std::strcmp(left, right) >= 0;
}

/**
 * Less-than
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

template <>
inline bool LessThan::Apply(const char *left, const char *right) {
  return std::strcmp(left, right) < 0;
}

/**
 * Less-than or equal
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

template <>
inline bool LessThanEqual::Apply(const char *left, const char *right) {
  return std::strcmp(left, right) <= 0;
}

/**
 * Inequality
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

template <>
inline bool NotEqual::Apply(const char *left, const char *right) {
  return std::strcmp(left, right) != 0;
}

}  // namespace tpl::sql
