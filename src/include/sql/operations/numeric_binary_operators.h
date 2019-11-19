#pragma once

#include <cmath>

#include "common/macros.h"
#include "util/arithmetic_overflow.h"

namespace tpl::sql {

/**
 * Addition.
 */
struct Add {
  template <typename T>
  static T Apply(T a, T b) {
    return a + b;
  }

  template <typename T>
  static bool Apply(T a, T b, T *result) {
    return util::ArithmeticOverflow::Add(a, b, result);
  }
};

/**
 * Subtraction.
 */
struct Subtract {
  template <typename T>
  static T Apply(T a, T b) {
    return a - b;
  }

  template <typename T>
  static bool Apply(T a, T b, T *result) {
    return util::ArithmeticOverflow::Sub(a, b, result);
  }
};

/**
 * Multiplication.
 */
struct Multiply {
  template <typename T>
  static T Apply(T a, T b) {
    return a * b;
  }

  template <typename T>
  static bool Apply(T a, T b, T *result) {
    return util::ArithmeticOverflow::Mul(a, b, result);
  }
};

/**
 * Division.
 */
struct Divide {
  template <typename T>
  static T Apply(T a, T b) {
    // Ensure divisor isn't zero. This should have been checked before here!
    TPL_ASSERT(b != 0, "Divide by zero");
    return a / b;
  }
};

/**
 * Modulus.
 */
struct Modulo {
  template <typename T>
  static T Apply(T a, T b) {
    // Ensure divisor isn't zero. This should have been checked before here!
    TPL_ASSERT(b != 0, "Divide by zero");
    return a % b;
  }
};

template <>
inline float Modulo::Apply(float a, float b) {
  TPL_ASSERT(b != 0, "Divide by zero");
  return std::fmod(a, b);
}

template <>
inline double Modulo::Apply(double a, double b) {
  TPL_ASSERT(b != 0, "Divide by zero");
  return std::fmod(a, b);
}

}  // namespace tpl::sql
