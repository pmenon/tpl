#pragma once

#include <algorithm>
#include <cmath>
#include <stdexcept>

#include "util/common.h"

namespace tpl::sql {

// This file contains a bunch of templated functors that implement many
// Postgres mathematical operators and functions. These operate on raw data
// types, and hence, have no notion of "NULL"-ness. That behaviour is handled at
// a higher-level of the type system, most likely in the functions that call
// into these.
//
// The functors here are sorted alphabetically for convenience.

/**
 * Return the value of the mathematical constant PI.
 */
struct Pi {
  static double Apply() { return M_PI; }
};

/**
 * Return the value of the mathematical constant E.
 */
struct E {
  static double Apply() { return M_E; }
};

struct Abs {
  template <typename T>
  static T Apply(T input) {
    return input < 0 ? -input : input;
  }
};

struct Acos {
  template <typename T>
  static double Apply(T input) {
    if (input < -1 || input > 1) {
      throw std::runtime_error("ACos is undefined outside [-1,1]");
    }
    return std::acos(input);
  }
};

struct Asin {
  template <typename T>
  static double Apply(T input) {
    if (input < -1 || input > 1) {
      throw std::runtime_error("ASin is undefined outside [-1,1]");
    }
    return std::asin(input);
  }
};

struct Atan {
  template <typename T>
  static double Apply(T input) {
    return std::atan(input);
  }
};

struct Atan2 {
  template <typename T>
  static double Apply(T a, T b) {
    return std::atan2(a, b);
  }
};

struct Cbrt {
  template <typename T>
  static double Apply(T input) {
    return std::cbrt(input);
  }
};

struct Ceil {
  template <typename T>
  static T Apply(T input) {
    return std::ceil(input);
  }
};

struct Cos {
  template <typename T>
  static double Apply(T input) {
    return std::cos(input);
  }
};

struct Cosh {
  template <typename T>
  static double Apply(T input) {
    return std::cosh(input);
  }
};

struct Cot {
  static double cotan(const double arg) { return (1.0 / std::tan(arg)); }

  template <typename T>
  static double Apply(T input) {
    return cotan(input);
  }
};

struct Degrees {
  template <typename T>
  static double Apply(T input) {
    return input * 180.0 / M_PI;
  }
};

struct Exp {
  template <typename T>
  static double Apply(T input) {
    return std::exp(input);
  }
};

struct Floor {
  template <typename T>
  static T Apply(T input) {
    return std::floor(input);
  }
};

struct Ln {
  template <typename T>
  static T Apply(T input) {
    return std::log(input);
  }
};

struct Log {
  template <typename T>
  static T Apply(T input, T base) {
    return std::log(input) / std::log(base);
  }
};

struct Log2 {
  template <typename T>
  static T Apply(T input) {
    return std::log2(input);
  }
};

struct Log10 {
  template <typename T>
  static T Apply(T input) {
    return std::log10(input);
  }
};

struct Pow {
  template <typename T, typename U>
  static double Apply(T a, U b) {
    return std::pow(a, b);
  }
};

struct Radians {
  template <typename T>
  static double Apply(T input) {
    return input * M_PI / 180.0;
  }
};

struct Round {
  template <typename T>
  static T Apply(T input) {
    return input + ((input < 0) ? -0.5 : 0.5);
  }
};

struct RoundUpTo {
  template <typename T, typename U>
  static T Apply(T input, U scale) {
    if (scale < 0) {
      scale = 0;
    }
    T modifier = std::pow(10, scale);
    return (static_cast<i64>(input * modifier)) / modifier;
  }
};

struct Sign {
  template <typename T>
  static T Apply(T input) {
    return (input > 0) ? 1 : ((input < 0) ? -1.0 : 0);
  }
};

struct Sin {
  template <typename T>
  static double Apply(T input) {
    return std::sin(input);
  }
};

struct Sinh {
  template <typename T>
  static double Apply(T input) {
    return std::sinh(input);
  }
};

struct Sqrt {
  template <typename T>
  static T Apply(T input) {
    return std::sqrt(input);
  }
};

struct Tan {
  template <typename T>
  static double Apply(T input) {
    return std::tan(input);
  }
};

struct Tanh {
  template <typename T>
  static double Apply(T input) {
    return std::tanh(input);
  }
};

struct Truncate {
  template <typename T>
  static T Apply(T input) {
    return std::trunc(input);
  }
};

}  // namespace tpl::sql::ops
