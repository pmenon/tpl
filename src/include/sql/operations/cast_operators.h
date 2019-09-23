#pragma once

#include <string>

#include "common/common.h"
#include "common/exception.h"
#include "sql/runtime_types.h"

namespace tpl::sql {

/**
 * Cast is a checking cast. Cast::Apply() will attempt to cast its input of type @em Src into type
 * @em Dest. If the cast operation is valid, the casted input is returned. Otherwise, an exception
 * is thrown.
 */
struct Cast {
  template <typename Src, typename Dest>
  static Dest Apply(Src source) {
    return static_cast<Dest>(source);
  }
};

/**
 * TryCast is a "safe" cast. TryCast::Apply() will attempt to cast its input operand of type @em Src
 * into type @em Dest. The function returns a boolean indicating if a cast was applied. If true, the
 * output operand contains the result of the cast.
 */
struct TryCast {
  template <typename Src, typename Dest>
  static bool Apply(Src source, Dest *dest) {
    *dest = Cast::Apply(source);
    return true;
  }
};

// ---------------------------------------------------------
// Down-casting Numeric -> TinyInt (int8_t)
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
// Down-casting Numeric -> SmallInt (int16_t)
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
// Down-casting Numeric -> Int (int32_t)
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
// Down-casting Numeric -> BigInt (int64_t)
// ---------------------------------------------------------

template <>
int64_t Cast::Apply(float);
template <>
int64_t Cast::Apply(double);
template <>
bool TryCast::Apply(float, int64_t *);
template <>
bool TryCast::Apply(double, int64_t *);

// ---------------------------------------------------------
// Date casting
// ---------------------------------------------------------

struct CastFromDate {
  template <class Src, class Dest>
  static inline Dest Apply(Src source) {
    throw NotImplementedException("Cast from Date is not supported!");
  }
};

struct CastToDate {
  template <class Src, class Dest>
  static inline Dest Apply(Src source) {
    throw NotImplementedException("Cast to Date is not supported!");
  }
};

template <>
std::string CastFromDate::Apply(Date);

template <>
Date CastToDate::Apply(const char *);

// ---------------------------------------------------------
// Numeric -> String
// ---------------------------------------------------------

template <>
std::string Cast::Apply(bool);
template <>
std::string Cast::Apply(int8_t);
template <>
std::string Cast::Apply(int16_t);
template <>
std::string Cast::Apply(int32_t);
template <>
std::string Cast::Apply(int64_t);
template <>
std::string Cast::Apply(float);
template <>
std::string Cast::Apply(double);

}  // namespace tpl::sql
