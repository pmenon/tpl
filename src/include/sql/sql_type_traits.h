#pragma once

#include "sql/sql.h"

namespace tpl::sql {

template <TypeId type>
struct TypeTraits;

template <>
struct TypeTraits<TypeId::Boolean> {
  using CppType = bool;
  static constexpr SqlTypeId kSqlTypeId = SqlTypeId::Boolean;
  static constexpr bool kNullValue = false;
};

template <>
struct TypeTraits<TypeId::TinyInt> {
  using CppType = i8;
  static constexpr SqlTypeId kSqlTypeId = SqlTypeId::TinyInt;
  static constexpr CppType kNullValue = 0;
};

template <>
struct TypeTraits<TypeId::SmallInt> {
  using CppType = i16;
  static constexpr SqlTypeId kSqlTypeId = SqlTypeId::SmallInt;
  static constexpr CppType kNullValue = 0;
};

template <>
struct TypeTraits<TypeId::Integer> {
  using CppType = i32;
  static constexpr SqlTypeId kSqlTypeId = SqlTypeId::Integer;
  static constexpr CppType kNullValue = 0;
};

template <>
struct TypeTraits<TypeId::BigInt> {
  using CppType = i64;
  static constexpr SqlTypeId kSqlTypeId = SqlTypeId::BigInt;
  static constexpr CppType kNullValue = 0;
};

template <>
struct TypeTraits<TypeId::Float> {
  using CppType = f32;
  static constexpr SqlTypeId kSqlTypeId = SqlTypeId::Real;
  static constexpr CppType kNullValue = 0.0;
};

template <>
struct TypeTraits<TypeId::Double> {
  using CppType = f64;
  static constexpr SqlTypeId kSqlTypeId = SqlTypeId::Double;
  static constexpr CppType kNullValue = 0.0;
};

template <>
struct TypeTraits<TypeId::Hash> {
  using CppType = hash_t;
};

template <>
struct TypeTraits<TypeId::Pointer> {
  using CppType = uintptr_t;
};

template <>
struct TypeTraits<TypeId::Varchar> {
  using CppType = char *;
  static constexpr SqlTypeId kSqlTypeId = SqlTypeId::Varchar;
  static constexpr CppType kNullValue = nullptr;
};

template <>
struct TypeTraits<TypeId::Varbinary> {
  using CppType = Blob;
};

}  // namespace tpl::sql