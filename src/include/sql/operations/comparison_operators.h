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
 * Equality operator.
 */
struct Equal {
  using SymmetricOp = Equal;

  template <typename T>
  static bool Apply(T left, T right) {
    return left == right;
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
};

}  // namespace tpl::sql
