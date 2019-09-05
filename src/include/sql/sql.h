#pragma once

#include <string>

#include "util/common.h"
#include "util/macros.h"

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
  Varchar,   // char*, representing a null-terminated UTF-8 string
  Varbinary  // blobs representing arbitrary bytes
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
 * Simple structure representing a blob.
 */
struct Blob {
  u8 *data;
  u64 size;
};

/**
 * Given an internal type, return the simplest SQL type. Note that this
 * conversion is a lossy since some internal types are used as the underlying
 * storage for multiple SQL types. For example, i32 is used for SQL Integers and
 * SQL Dates).
 */
SqlTypeId GetSqlTypeFromInternalType(TypeId type);

/**
 * Given a templated type, return the associated internal type ID.
 */
template <class T>
constexpr static inline TypeId GetTypeId() {
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
                       std::is_same<std::remove_const_t<T>, const char *>() ||
                       std::is_same<std::remove_const_t<T>, std::string>() ||
                       std::is_same<std::remove_const_t<T>, std::string_view>()) {
    return TypeId::Varchar;
  } else if constexpr (std::is_same<std::remove_const_t<T>, Blob>()) {
    return TypeId::Varbinary;
  }
  static_assert("Not a valid primitive type");
}

/**
 * Return the size in bytes of a value with the primitive type @em type.
 */
std::size_t GetTypeIdSize(TypeId type);

/**
 * Is the given type a fixed-size type?
 */
bool IsTypeFixedSize(TypeId type);

/**
 * Is the given type a numeric?
 */
bool IsTypeNumeric(TypeId type);

/**
 * Convert a TypeId to a string value.
 */
const char *TypeIdToString(TypeId type);

}  // namespace tpl::sql
