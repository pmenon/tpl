#pragma once

#include <algorithm>
#include <cstring>

#include "common/common.h"
#include "sql/runtime_types.h"

namespace tpl::sql {

template <typename>
struct Equal;
template <typename>
struct GreaterThan;
template <typename>
struct GreaterThanEqual;
template <typename>
struct LessThan;
template <typename>
struct LessThanEqual;
template <typename>
struct NotEqual;

/**
 * Equality operator.
 */
template <typename T>
struct Equal {
  using SymmetricOp = Equal<T>;

  static bool Apply(T left, T right) { return left == right; }
};

/**
 * Greater-than operator.
 */
template <typename T>
struct GreaterThan {
  using SymmetricOp = LessThan<T>;

  static bool Apply(T left, T right) { return left > right; }
};

/**
 * Greater-than or equal operator.
 */
template <typename T>
struct GreaterThanEqual {
  using SymmetricOp = LessThanEqual<T>;

  static bool Apply(T left, T right) { return left >= right; }
};

/**
 * Less-than operator.
 */
template <typename T>
struct LessThan {
  using SymmetricOp = GreaterThan<T>;

  static bool Apply(T left, T right) { return left < right; }
};

/**
 * Less-than or equal operator.
 */
template <typename T>
struct LessThanEqual {
  using SymmetricOp = GreaterThanEqual<T>;

  static bool Apply(T left, T right) { return left <= right; }
};

/**
 * Inequality operator.
 */
template <typename T>
struct NotEqual {
  using SymmetricOp = NotEqual<T>;

  static bool Apply(T left, T right) { return left != right; }
};

}  // namespace tpl::sql
