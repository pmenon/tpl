#pragma once

#include <cmath>

#include "util/arithmetic_overflow.h"
#include "util/macros.h"

namespace tpl::sql {

// This file contains a bunch of templated functors that implement traditional
// mathematical operators.

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

struct Divide {
  template <typename T>
  static T Apply(T a, T b) {
    // Ensure divisor isn't zero. This should have been checked before here!
    TPL_ASSERT(b != 0, "Divide by zero");
    return a / b;
  }
};

struct Modulo {
  template <typename T>
  static T Apply(T a, T b) {
    // Ensure divisor isn't zero. This should have been checked before here!
    TPL_ASSERT(b != 0, "Divide by zero");
    return a % b;
  }
};

template <>
inline f32 Modulo::Apply(f32 a, f32 b) {
  TPL_ASSERT(b != 0, "Divide by zero");
  return std::fmod(a, b);
}

template <>
inline f64 Modulo::Apply(f64 a, f64 b) {
  TPL_ASSERT(b != 0, "Divide by zero");
  return std::fmod(a, b);
}

}  // namespace tpl::sql