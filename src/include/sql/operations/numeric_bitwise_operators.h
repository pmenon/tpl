#pragma once

namespace tpl::sql {

/**
 * Bitwise AND two numeric elements.
 */
struct BitwiseAnd {
  template <typename T>
  static T Apply(const T &a, const T &b) {
    return a & b;
  }
};

/**
 * Bitwise OR two numeric elements.
 */
struct BitwiseOr {
  template <typename T>
  static T Apply(const T &a, const T &b) {
    return a | b;
  }
};

/**
 * Left-shift a numeric element.
 */
struct BitwiseShiftLeft {
  template <typename T>
  static T Apply(const T &a, const T &b) {
    return a << b;
  }
};

/**
 * Right-shift a numeric element.
 */
struct BitwiseShiftRight {
  template <typename T>
  static T Apply(const T &a, const T &b) {
    return a >> b;
  }
};

/**
 * Bitwise negate a numeric element.
 */
struct BitwiseNot {
  template <typename T>
  static T Apply(const T &input) {
    return ~input;
  }
};

/**
 * Bitwise XOR a numeric element.
 */
struct BitwiseXor {
  template <typename T>
  static T Apply(const T &a, const T &b) {
    return a ^ b;
  }
};

}  // namespace tpl::sql
