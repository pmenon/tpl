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
static inline SqlTypeId GetSqlTypeFromInternalType(TypeId type) {
  switch (type) {
    case TypeId::Boolean:
      return SqlTypeId::Boolean;
    case TypeId::TinyInt:
      return SqlTypeId::TinyInt;
    case TypeId::SmallInt:
      return SqlTypeId::SmallInt;
    case TypeId::Integer:
      return SqlTypeId::Integer;
    case TypeId::BigInt:
      return SqlTypeId::BigInt;
    case TypeId::Float:
      return SqlTypeId::Real;
    case TypeId::Double:
      return SqlTypeId::Double;
    case TypeId::Varchar:
      return SqlTypeId::Varchar;
    case TypeId::Varbinary:
      return SqlTypeId::Varchar;
    default:
      UNREACHABLE("Impossible internal type");
  }
}

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
                       std::is_same<std::remove_const_t<T>,
                                    std::string_view>()) {
    return TypeId::Varchar;
  } else if constexpr (std::is_same<std::remove_const_t<T>, Blob>()) {
    return TypeId::Varbinary;
  }
  static_assert("Not a valid primitive type");
}

/**
 * Return the size in bytes of a value with the primitive type @em type
 */
static inline std::size_t GetTypeIdSize(TypeId type) {
  switch (type) {
    case TypeId::Boolean:
      return sizeof(bool);
    case TypeId::TinyInt:
      return sizeof(i8);
    case TypeId::SmallInt:
      return sizeof(i16);
    case TypeId::Integer:
      return sizeof(i32);
    case TypeId::BigInt:
      return sizeof(i64);
    case TypeId::Hash:
      return sizeof(hash_t);
    case TypeId::Pointer:
      return sizeof(uintptr_t);
    case TypeId::Float:
      return sizeof(f32);
    case TypeId::Double:
      return sizeof(f64);
    case TypeId::Varchar:
      return sizeof(char *);
    case TypeId::Varbinary:
      return sizeof(Blob);
    default:
      UNREACHABLE("Impossible type");
  }
}

/**
 * Is the given type a fixed-size type?
 */
static inline bool IsTypeFixedSize(TypeId type) {
  switch (type) {
    case TypeId::Boolean:
    case TypeId::TinyInt:
    case TypeId::SmallInt:
    case TypeId::Integer:
    case TypeId::BigInt:
    case TypeId::Hash:
    case TypeId::Pointer:
    case TypeId::Float:
    case TypeId::Double:
      return true;
    case TypeId::Varchar:
    case TypeId::Varbinary:
      return false;
    default:
      UNREACHABLE("Impossible type");
  }
}

/**
 * Is the given type a numeric?
 */
static inline bool IsTypeNumeric(TypeId type) {
  switch (type) {
    case TypeId::Boolean:
    case TypeId::TinyInt:
    case TypeId::SmallInt:
    case TypeId::Integer:
    case TypeId::BigInt:
    case TypeId::Hash:
    case TypeId::Pointer:
    case TypeId::Float:
    case TypeId::Double:
      return true;
    case TypeId::Varchar:
    case TypeId::Varbinary:
      return false;
    default:
      UNREACHABLE("Impossible type");
  }
}

/**
 * Convert a TypeId to a string value.
 * @param type_id The type ID to stringify.
 * @return The string representation of the type ID.
 */
static inline std::string TypeIdToString(TypeId type) {
  switch (type) {
    case TypeId::Boolean:
      return "Boolean";
    case TypeId::TinyInt:
      return "TinyInt";
    case TypeId::SmallInt:
      return "SmallInt";
    case TypeId::Integer:
      return "Integer";
    case TypeId::BigInt:
      return "BigInt";
    case TypeId::Hash:
      return "Hash";
    case TypeId::Pointer:
      return "Pointer";
    case TypeId::Float:
      return "Float";
    case TypeId::Double:
      return "Double";
    case TypeId::Varchar:
      return "VarChar";
    case TypeId::Varbinary:
      return "VarBinary";
    default:
      UNREACHABLE("Impossible type");
  }
}

}  // namespace tpl::sql
