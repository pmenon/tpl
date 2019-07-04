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
// Downcasting Numeric -> TinyInt (i8)
// ---------------------------------------------------------

template <>
i8 Cast::Apply(i16);
template <>
i8 Cast::Apply(i32);
template <>
i8 Cast::Apply(i64);
template <>
i8 Cast::Apply(f32);
template <>
i8 Cast::Apply(f64);
template <>
bool TryCast::Apply(i16, i8 *);
template <>
bool TryCast::Apply(i32, i8 *);
template <>
bool TryCast::Apply(i64, i8 *);
template <>
bool TryCast::Apply(f32, i8 *);
template <>
bool TryCast::Apply(f64, i8 *);

// ---------------------------------------------------------
// Downcasting Numeric -> SmallInt (i16)
// ---------------------------------------------------------

template <>
i16 Cast::Apply(i32);
template <>
i16 Cast::Apply(i64);
template <>
i16 Cast::Apply(f32);
template <>
i16 Cast::Apply(f64);
template <>
bool TryCast::Apply(i32, i16 *);
template <>
bool TryCast::Apply(i64, i16 *);
template <>
bool TryCast::Apply(f32, i16 *);
template <>
bool TryCast::Apply(f64, i16 *);

// ---------------------------------------------------------
// Downcasting Numeric -> Int (i32)
// ---------------------------------------------------------

template <>
i32 Cast::Apply(i64);
template <>
i32 Cast::Apply(f32);
template <>
i32 Cast::Apply(f64);
template <>
bool TryCast::Apply(i64, i32 *);
template <>
bool TryCast::Apply(f32, i32 *);
template <>
bool TryCast::Apply(f64, i32 *);

// ---------------------------------------------------------
// Downcasting Numeric -> BigInt (i64)
// ---------------------------------------------------------

template <>
i64 Cast::Apply(f32);
template <>
i64 Cast::Apply(f64);
template <>
bool TryCast::Apply(f32, i64 *);
template <>
bool TryCast::Apply(f64, i64 *);

}  // namespace tpl::sql
