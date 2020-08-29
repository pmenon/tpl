#pragma once

#include <algorithm>
#include <cstring>

#include "common/common.h"
#include "sql/runtime_types.h"

namespace tpl::sql {

/**
 * Equality operator.
 */
template <typename T>
struct Equal {
  /**
   * @return True if left == right; false otherwise.
   */
  constexpr bool operator()(T left, T right) const { return left == right; }
};

/**
 * Greater-than operator.
 * @tparam T The types to operate on.
 */
template <typename T>
struct GreaterThan {
  /**
   * @return True if left > right; false otherwise.
   */
  constexpr bool operator()(T left, T right) const { return left > right; }
};

/**
 * Greater-than or equal operator.
 * @tparam T The types to operate on.
 */
template <typename T>
struct GreaterThanEqual {
  /**
   * @return True if left >= right; false otherwise.
   */
  constexpr bool operator()(T left, T right) const { return left >= right; }
};

/**
 * Less-than operator.
 * @tparam T The types to operate on.
 */
template <typename T>
struct LessThan {
  /**
   * @return True if left < right; false otherwise.
   */
  constexpr bool operator()(T left, T right) const { return left < right; }
};

/**
 * Less-than or equal operator.
 * @tparam T The types to operate on.
 */
template <typename T>
struct LessThanEqual {
  /**
   * @return True if left <= right; false otherwise.
   */
  constexpr bool operator()(T left, T right) const { return left <= right; }
};

/**
 * Inequality operator.
 * @tparam T The types to operate on.
 */
template <typename T>
struct NotEqual {
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
  /**
   * @return True if lower <= input <= upper; false otherwise.
   */
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
  /**
   * @return True if lower <= input < upper; false otherwise.
   */
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
  /**
   * @return True if lower < input <= upper; false otherwise.
   */
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
  /**
   * @return True if lower < input < upper; false otherwise.
   */
  constexpr bool operator()(T input, T lower, T upper) const noexcept {
    return GreaterThan<T>{}(input, lower) && LessThan<T>{}(input, upper);
  }
};

}  // namespace tpl::sql
