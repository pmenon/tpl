#pragma once

#include <algorithm>
#include <cstring>

#include "common/common.h"
#include "sql/runtime_types.h"

namespace tpl::sql {

// Forward-declare all comparisons since they're used before defined.
// clang-format off
template <typename> struct Equal;
template <typename> struct GreaterThan;
template <typename> struct GreaterThanEqual;
template <typename> struct LessThan;
template <typename> struct LessThanEqual;
template <typename> struct NotEqual;
// clang-format on

/**
 * Equality operator.
 */
template <typename T>
struct Equal {
  using SymmetricOp = Equal<T>;

  /**
   * @return True if left == right; false otherwise.
   */
  constexpr bool operator()(T left, T right) const { return left == right; }
};

/**
 * Greater-than operator.
 */
template <typename T>
struct GreaterThan {
  using SymmetricOp = LessThan<T>;

  /**
   * @return True if left > right; false otherwise.
   */
  constexpr bool operator()(T left, T right) const { return left > right; }
};

/**
 * Greater-than or equal operator.
 */
template <typename T>
struct GreaterThanEqual {
  using SymmetricOp = LessThanEqual<T>;

  /**
   * @return True if left >= right; false otherwise.
   */
  constexpr bool operator()(T left, T right) const { return left >= right; }
};

/**
 * Less-than operator.
 */
template <typename T>
struct LessThan {
  using SymmetricOp = GreaterThan<T>;

  /**
   * @return True if left < right; false otherwise.
   */
  constexpr bool operator()(T left, T right) const { return left < right; }
};

/**
 * Less-than or equal operator.
 */
template <typename T>
struct LessThanEqual {
  using SymmetricOp = GreaterThanEqual<T>;

  /**
   * @return True if left <= right; false otherwise.
   */
  constexpr bool operator()(T left, T right) const { return left <= right; }
};

/**
 * Inequality operator.
 */
template <typename T>
struct NotEqual {
  using SymmetricOp = NotEqual<T>;

  /**
   * @return True if left != right; false otherwise.
   */
  constexpr bool operator()(T left, T right) const { return left != right; }
};

/**
 * Inclusive between operator, i.e., lower <= input <= upper.
 * @tparam T The types to operate on.
 */
template <class T>
struct InclusiveBetweenOperator {
  constexpr bool operator()(T input, T lower, T upper) const noexcept {
    return GreaterThanEqual<T>{}(input, lower) && LessThanEqual<T>{}(input, upper);
  }
};

/**
 * Left-inclusive between operator, i.e., lower <= input < upper.
 * @tparam T The types to operate on.
 */
template <class T>
struct LowerInclusiveBetweenOperator {
  constexpr bool operator()(T input, T lower, T upper) const noexcept {
    return GreaterThanEqual<T>{}(input, lower) && LessThan<T>{}(input, upper);
  }
};

/**
 * Right-inclusive between operator, i.e., lower < input <= upper.
 * @tparam T The types to operate on.
 */
template <class T>
struct UpperInclusiveBetweenOperator {
  constexpr bool operator()(T input, T lower, T upper) const noexcept {
    return GreaterThan<T>{}(input, lower) && LessThanEqual<T>{}(input, upper);
  }
};

/**
 * Exclusive between, i.e., lower < input < upper.
 * @tparam T The types to operate on.
 */
template <class T>
struct ExclusiveBetweenOperator {
  constexpr bool operator()(T input, T lower, T upper) const noexcept {
    return GreaterThan<T>{}(input, lower) && LessThan<T>{}(input, upper);
  }
};

}  // namespace tpl::sql
