#pragma once

#include "util/common.h"

namespace tpl::sql {

/**
 * Simple unchecked cast.
 */
struct Cast {
  template <typename Src, typename Dest>
  static Dest Apply(Src source) {
    return static_cast<Dest>(source);
  }
};

/**
 * Simple checked cast.
 */
struct TryCast {
  template <typename Src, typename Dest>
  static bool Apply(Src source, Dest *dest) {
    *dest = Cast::Apply(source);
    return true;
  }
};

// ---------------------------------------------------------
// Downcasting Numeric -> TinyInt (int8_t)
// ---------------------------------------------------------

template <>
int8_t Cast::Apply(int16_t);
template <>
int8_t Cast::Apply(int32_t);
template <>
int8_t Cast::Apply(int64_t);
template <>
int8_t Cast::Apply(float);
template <>
int8_t Cast::Apply(double);
template <>
bool TryCast::Apply(int16_t, int8_t *);
template <>
bool TryCast::Apply(int32_t, int8_t *);
template <>
bool TryCast::Apply(int64_t, int8_t *);
template <>
bool TryCast::Apply(float, int8_t *);
template <>
bool TryCast::Apply(double, int8_t *);

// ---------------------------------------------------------
// Downcasting Numeric -> SmallInt (int16_t)
// ---------------------------------------------------------

template <>
int16_t Cast::Apply(int32_t);
template <>
int16_t Cast::Apply(int64_t);
template <>
int16_t Cast::Apply(float);
template <>
int16_t Cast::Apply(double);
template <>
bool TryCast::Apply(int32_t, int16_t *);
template <>
bool TryCast::Apply(int64_t, int16_t *);
template <>
bool TryCast::Apply(float, int16_t *);
template <>
bool TryCast::Apply(double, int16_t *);

// ---------------------------------------------------------
// Downcasting Numeric -> Int (int32_t)
// ---------------------------------------------------------

template <>
int32_t Cast::Apply(int64_t);
template <>
int32_t Cast::Apply(float);
template <>
int32_t Cast::Apply(double);
template <>
bool TryCast::Apply(int64_t, int32_t *);
template <>
bool TryCast::Apply(float, int32_t *);
template <>
bool TryCast::Apply(double, int32_t *);

// ---------------------------------------------------------
// Downcasting Numeric -> BigInt (int64_t)
// ---------------------------------------------------------

template <>
int64_t Cast::Apply(float);
template <>
int64_t Cast::Apply(double);
template <>
bool TryCast::Apply(float, int64_t *);
template <>
bool TryCast::Apply(double, int64_t *);

}  // namespace tpl::sql
