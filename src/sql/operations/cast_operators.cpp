#include "sql/operations/cast_operators.h"

#include <limits>
#include <string>

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
    throw ValueOutOfRangeException(source, GetTypeId<Src>(), GetTypeId<Dest>());
  }
  return dest;
}

}  // namespace

// ---------------------------------------------------------
// Down-casting Numeric -> TinyInt (int8_t)
// ---------------------------------------------------------

template <>
int8_t Cast::Apply(int16_t source) {
  return DoCheckedCast<int16_t, int8_t>(source);
}

template <>
int8_t Cast::Apply(int32_t source) {
  return DoCheckedCast<int32_t, int8_t>(source);
}

template <>
int8_t Cast::Apply(int64_t source) {
  return DoCheckedCast<int64_t, int8_t>(source);
}

template <>
int8_t Cast::Apply(float source) {
  return DoCheckedCast<float, int8_t>(source);
}

template <>
int8_t Cast::Apply(double source) {
  return DoCheckedCast<double, int8_t>(source);
}

template <>
bool TryCast::Apply(int16_t source, int8_t *dest) {
  return DoSafeCheckedCast<int16_t, int8_t>(source, dest);
}

template <>
bool TryCast::Apply(int32_t source, int8_t *dest) {
  return DoSafeCheckedCast<int32_t, int8_t>(source, dest);
}

template <>
bool TryCast::Apply(int64_t source, int8_t *dest) {
  return DoSafeCheckedCast<int64_t, int8_t>(source, dest);
}

template <>
bool TryCast::Apply(float source, int8_t *dest) {
  return DoSafeCheckedCast<float, int8_t>(source, dest);
}

template <>
bool TryCast::Apply(double source, int8_t *dest) {
  return DoSafeCheckedCast<double, int8_t>(source, dest);
}

// ---------------------------------------------------------
// Down-casting Numeric -> SmallInt (int16_t)
// ---------------------------------------------------------

template <>
int16_t Cast::Apply(int32_t source) {
  return DoCheckedCast<int32_t, int16_t>(source);
}
template <>
int16_t Cast::Apply(int64_t source) {
  return DoCheckedCast<int64_t, int16_t>(source);
}
template <>
int16_t Cast::Apply(float source) {
  return DoCheckedCast<float, int16_t>(source);
}
template <>
int16_t Cast::Apply(double source) {
  return DoCheckedCast<double, int16_t>(source);
}

template <>
bool TryCast::Apply(int32_t source, int16_t *dest) {
  return DoSafeCheckedCast<int32_t, int16_t>(source, dest);
}
template <>
bool TryCast::Apply(int64_t source, int16_t *dest) {
  return DoSafeCheckedCast<int64_t, int16_t>(source, dest);
}
template <>
bool TryCast::Apply(float source, int16_t *dest) {
  return DoSafeCheckedCast<float, int16_t>(source, dest);
}
template <>
bool TryCast::Apply(double source, int16_t *dest) {
  return DoSafeCheckedCast<double, int16_t>(source, dest);
}

// ---------------------------------------------------------
// Down-casting Numeric -> Int (int32_t)
// ---------------------------------------------------------

template <>
int32_t Cast::Apply(int64_t source) {
  return DoCheckedCast<int64_t, int32_t>(source);
}

template <>
int32_t Cast::Apply(float source) {
  return DoCheckedCast<float, int32_t>(source);
}

template <>
int32_t Cast::Apply(double source) {
  return DoCheckedCast<double, int32_t>(source);
}

template <>
bool TryCast::Apply(int64_t source, int32_t *dest) {
  return DoSafeCheckedCast<int64_t, int32_t>(source, dest);
}

template <>
bool TryCast::Apply(float source, int32_t *dest) {
  return DoSafeCheckedCast<float, int32_t>(source, dest);
}

template <>
bool TryCast::Apply(double source, int32_t *dest) {
  return DoSafeCheckedCast<double, int32_t>(source, dest);
}

// ---------------------------------------------------------
// Down-casting Numeric -> BigInt (int64_t)
// ---------------------------------------------------------

template <>
int64_t Cast::Apply(float source) {
  return DoCheckedCast<float, int64_t>(source);
}

template <>
int64_t Cast::Apply(double source) {
  return DoCheckedCast<double, int64_t>(source);
}

template <>
bool TryCast::Apply(float source, int64_t *dest) {
  return DoSafeCheckedCast<float, int64_t>(source, dest);
}

template <>
bool TryCast::Apply(double source, int64_t *dest) {
  return DoSafeCheckedCast<float, int64_t>(source, dest);
}

// ---------------------------------------------------------
// Date casting
// ---------------------------------------------------------

template <>
std::string CastFromDate::Apply(Date source) {
  return source.ToString();
}

template <>
Date CastToDate::Apply(const char *source) {
  return Date::FromString(source, strlen(source));
}

// ---------------------------------------------------------
// Numeric -> String
// ---------------------------------------------------------

template <>
std::string Cast::Apply(bool source) {
  return source ? "true" : "false";
}

template <>
std::string Cast::Apply(int8_t source) {
  return std::to_string(source);
}

template <>
std::string Cast::Apply(int16_t source) {
  return std::to_string(source);
}

template <>
std::string Cast::Apply(int32_t source) {
  return std::to_string(source);
}

template <>
std::string Cast::Apply(int64_t source) {
  return std::to_string(source);
}

template <>
std::string Cast::Apply(float source) {
  return std::to_string(source);
}

template <>
std::string Cast::Apply(double source) {
  return std::to_string(source);
}

}  // namespace tpl::sql
