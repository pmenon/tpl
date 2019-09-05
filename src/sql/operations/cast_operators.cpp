#include "sql/operations/cast_operators.h"

#include <limits>
#include <stdexcept>

namespace tpl::sql {

namespace {

template <typename Src, typename Dest>
static bool DoSafeCheckedCast(Src source, Dest *dest) {
  if (source < std::numeric_limits<Dest>::min() || source > std::numeric_limits<Dest>::max()) {
    return false;
  }
  *dest = static_cast<Dest>(source);
  return true;
}

template <typename Src, typename Dest>
static Dest DoCheckedCast(Src source) {
  Dest dest;
  if (!DoSafeCheckedCast(source, &dest)) {
    throw std::runtime_error("Write me");
  }
  return dest;
}

}  // namespace

// ---------------------------------------------------------
// Downcasting Numeric -> TinyInt (i8)
// ---------------------------------------------------------

template <>
i8 Cast::Apply(i16 source) {
  return DoCheckedCast<i16, i8>(source);
}

template <>
i8 Cast::Apply(i32 source) {
  return DoCheckedCast<i32, i8>(source);
}

template <>
i8 Cast::Apply(i64 source) {
  return DoCheckedCast<i64, i8>(source);
}

template <>
i8 Cast::Apply(f32 source) {
  return DoCheckedCast<f32, i8>(source);
}

template <>
i8 Cast::Apply(f64 source) {
  return DoCheckedCast<f64, i8>(source);
}

template <>
bool TryCast::Apply(i16 source, i8 *dest) {
  return DoSafeCheckedCast<i16, i8>(source, dest);
}

template <>
bool TryCast::Apply(i32 source, i8 *dest) {
  return DoSafeCheckedCast<i32, i8>(source, dest);
}

template <>
bool TryCast::Apply(i64 source, i8 *dest) {
  return DoSafeCheckedCast<i64, i8>(source, dest);
}

template <>
bool TryCast::Apply(f32 source, i8 *dest) {
  return DoSafeCheckedCast<f32, i8>(source, dest);
}

template <>
bool TryCast::Apply(f64 source, i8 *dest) {
  return DoSafeCheckedCast<f64, i8>(source, dest);
}

// ---------------------------------------------------------
// Downcasting Numeric -> SmallInt (i16)
// ---------------------------------------------------------

template <>
i16 Cast::Apply(i32 source) {
  return DoCheckedCast<i32, i16>(source);
}
template <>
i16 Cast::Apply(i64 source) {
  return DoCheckedCast<i64, i16>(source);
}
template <>
i16 Cast::Apply(f32 source) {
  return DoCheckedCast<f32, i16>(source);
}
template <>
i16 Cast::Apply(f64 source) {
  return DoCheckedCast<f64, i16>(source);
}

template <>
bool TryCast::Apply(i32 source, i16 *dest) {
  return DoSafeCheckedCast<i32, i16>(source, dest);
}
template <>
bool TryCast::Apply(i64 source, i16 *dest) {
  return DoSafeCheckedCast<i64, i16>(source, dest);
}
template <>
bool TryCast::Apply(f32 source, i16 *dest) {
  return DoSafeCheckedCast<f32, i16>(source, dest);
}
template <>
bool TryCast::Apply(f64 source, i16 *dest) {
  return DoSafeCheckedCast<f64, i16>(source, dest);
}

// ---------------------------------------------------------
// Downcasting Numeric -> Int (i32)
// ---------------------------------------------------------

template <>
i32 Cast::Apply(i64 source) {
  return DoCheckedCast<i64, i32>(source);
}

template <>
i32 Cast::Apply(f32 source) {
  return DoCheckedCast<f32, i32>(source);
}

template <>
i32 Cast::Apply(f64 source) {
  return DoCheckedCast<f64, i32>(source);
}

template <>
bool TryCast::Apply(i64 source, i32 *dest) {
  return DoSafeCheckedCast<i64, i32>(source, dest);
}

template <>
bool TryCast::Apply(f32 source, i32 *dest) {
  return DoSafeCheckedCast<f32, i32>(source, dest);
}

template <>
bool TryCast::Apply(f64 source, i32 *dest) {
  return DoSafeCheckedCast<f64, i32>(source, dest);
}

// ---------------------------------------------------------
// Downcasting Numeric -> BigInt (i64)
// ---------------------------------------------------------

template <>
i64 Cast::Apply(f32 source) {
  return DoCheckedCast<f32, i64>(source);
}

template <>
i64 Cast::Apply(f64 source) {
  return DoCheckedCast<f64, i64>(source);
}

template <>
bool TryCast::Apply(f32 source, i64 *dest) {
  return DoSafeCheckedCast<f32, i64>(source, dest);
}

template <>
bool TryCast::Apply(f64 source, i64 *dest) {
  return DoSafeCheckedCast<f32, i64>(source, dest);
}

}  // namespace tpl::sql
