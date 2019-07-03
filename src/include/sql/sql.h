#pragma once

#include "util/common.h"

namespace tpl::sql {

/**
 * The primitive types underlying the SQL types.
 */
enum class TypeId : u8 {
  Boolean,   // bool
  TinyInt,   // i8
  SmallInt,  // i16
  Integer,   // i32
  BigInt,    // i64
  Hash,      // hash_t
  Pointer,   // uintptr_t
  Float,     // f32
  Double,    // f64
  VarChar,   // char*, representing a null-terminated UTF-8 string
  VarBinary  // blobs representing arbitrary bytes
};

/**
 * Supported SQL data types.
 */
enum class SqlTypeId : u8 {
  Boolean,
  TinyInt,   // 1-byte integer
  SmallInt,  // 2-byte integer
  Integer,   // 4-byte integer
  BigInt,    // 8-byte integer
  Real,      // 4-byte float
  Double,    // 8-byte float
  Decimal,   // Arbitrary-precision numeric
  Date,
  Char,    // Fixed-length string
  Varchar  // Variable-length string
};

/**
 * The possible column compression/encodings.
 */
enum class ColumnEncoding : u8 {
  None,
  Rle,
  Delta,
  IntegerDict,
  StringDict,
};

/**
 * All possible JOIN types.
 */
enum class JoinType : u8 { Inner, Outer, Left, Right, Anti, Semi };

/**
 * Given a templated type, return the associated internal type ID.
 */
template <class T>
constexpr inline TypeId GetTypeId() {
  if constexpr (std::is_same<T, bool>()) {
    return TypeId::Boolean;
  } else if constexpr (std::is_same<std::remove_const_t<T>, i8>()) {
    return TypeId::TinyInt;
  } else if constexpr (std::is_same<std::remove_const_t<T>, i16>()) {
    return TypeId::SmallInt;
  } else if constexpr (std::is_same<std::remove_const_t<T>, i32>()) {
    return TypeId::Integer;
  } else if constexpr (std::is_same<std::remove_const_t<T>, i64>()) {
    return TypeId::BigInt;
  } else if constexpr (std::is_same<std::remove_const_t<T>, hash_t>()) {
    return TypeId::Hash;
  } else if constexpr (std::is_same<std::remove_const_t<T>, uintptr_t>()) {
    return TypeId::Pointer;
  } else if constexpr (std::is_same<std::remove_const_t<T>, f32>()) {
    return TypeId::Float;
  } else if constexpr (std::is_same<std::remove_const_t<T>, f64>()) {
    return TypeId::Double;
  } else if constexpr (std::is_same<std::remove_const_t<T>, char *>() ||
                       std::is_same<std::remove_const_t<T>, std::string>()) {
    return TypeId::VarChar;
  } else {
    return TypeId::VarBinary;
  }
}

}  // namespace tpl::sql
