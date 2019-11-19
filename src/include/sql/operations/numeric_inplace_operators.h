#pragma once

#include "common/macros.h"

namespace tpl::sql {

/**
 * In-place addition.
 */
struct AddInPlace {
  template <typename T>
  static void Apply(T *a, T b) {
    *a += b;
  }
};

/**
 * In-place modulus.
 */
struct ModuloInPlace {
  template <typename T>
  static void Apply(T *a, T b) {
    // Ensure divisor isn't zero. This should have been checked before here!
    TPL_ASSERT(b != 0, "Divide by zero");
    *a %= b;
  }
};

template <>
inline void ModuloInPlace::Apply(float *a, float b) {
  TPL_ASSERT(b != 0, "Divide by zero");
  *a = std::fmod(*a, b);
}

template <>
inline void ModuloInPlace::Apply(double *a, double b) {
  TPL_ASSERT(b != 0, "Divide by zero");
  *a = std::fmod(*a, b);
}

}  // namespace tpl::sql
