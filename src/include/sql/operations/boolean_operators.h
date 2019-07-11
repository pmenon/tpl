#pragma once

namespace tpl::sql {

/**
 * Boolean negation.
 */
struct Not {
  static bool Apply(const bool left) { return !left; }
};

/**
 * Boolean AND.
 */
struct And {
  static bool Apply(const bool left, bool right) { return left && right; }
};

/**
 * Determine if the result of a boolean AND is NULL. The truth table for AND is:
 *
 * true  AND true  = true
 * true  AND false = false
 * true  AND NULL  = NULL
 * false AND true  = false
 * false AND false = false
 * false AND NULL  = false
 * NULL  AND true  = NULL
 * NULL  AND false = false
 * NULL  AND NULL  = NULL
 *
 * So, the result of an AND is null if:
 * (1) Both inputs are NULL, or
 * (2) Either input is true and the other is NULL.
 */
struct AndNullMask {
  static bool Apply(const bool left, const bool right, const bool left_null,
                    const bool right_null) {
    return (left_null && (right_null || right)) || (left && right_null);
  }
};

/**
 * Boolean OR.
 */
struct Or {
  static bool Apply(const bool left, const bool right) { return left || right; }
};

/**
 * Determine if the result of a boolean OR is NULL. The truth table for OR is:
 *
 * true  OR true  = true
 * true  OR false = true
 * true  OR NULL  = true
 * false OR true  = true
 * false OR false = false
 * false OR NULL  = NULL
 * NULL  OR true  = true
 * NULL  OR false = NULL
 * NULL  OR NULL  = NULL
 *
 * So, the result of an OR is null if:
 * (1) Both inputs are NULL, or
 * (2) Either input is false and the other is NULL.
 */
struct OrNullMask {
  static bool Apply(const bool left, const bool right, const bool left_null,
                    const bool right_null) {
    return (left_null && (right_null || !right)) || (!left && right_null);
  }
};

}  // namespace tpl::sql
