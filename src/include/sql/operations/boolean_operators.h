#pragma once

namespace tpl::sql {

/**
 * Boolean negation.
 */
struct Not {
  static bool Apply(bool left) { return !left; }
};

/**
 * Boolean AND.
 */
struct And {
  static bool Apply(bool left, bool right) { return left && right; }
};

/**
 * Determine if the result of a boolean AND with the given values and NULL-ness
 * is also NULL.
 */
struct AndNullable {
  static bool Apply(bool left, bool right, bool left_null, bool right_null) {
    return (left_null && (right_null || right)) || (right_null && left);
  }
};

/**
 * Boolean OR.
 */
struct Or {
  static bool Apply(bool left, bool right) { return left || right; }
};

/**
 * Determine if the result of a boolean OR with the given values and NULL-ness
 * is also NULL.
 */
struct OrNullable {
  static bool Apply(bool left, bool right, bool left_null, bool right_null) {
    return (left_null && (right_null || !right)) || (right_null && !left);
  }
};

}  // namespace tpl::sql
