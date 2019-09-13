#pragma once

#include <cstring>
#include <string>

#include "common/common.h"
#include "common/macros.h"

namespace tpl::sql {

/**
 * The primitive types underlying the SQL types.
 */
enum class TypeId : uint8_t {
  Boolean,   // bool
  TinyInt,   // int8_t
  SmallInt,  // int16_t
  Integer,   // int32_t
  BigInt,    // int64_t
  Hash,      // hash_t
  Pointer,   // uintptr_t
  Float,     // float
  Double,    // double
  Varchar,   // char*, representing a null-terminated UTF-8 string
  Varbinary  // blobs representing arbitrary bytes
};

/**
 * Supported SQL data types.
 */
enum class SqlTypeId : uint8_t {
  Boolean,
  TinyInt,   // 1-byte integer
  SmallInt,  // 2-byte integer
  Integer,   // 4-byte integer
  BigInt,    // 8-byte integer
  Real,      // 4-byte float
  Double,    // 8-byte float
  Decimal,   // Arbitrary-precision numeric
  Date,      // Dates
  Char,      // Fixed-length string
  Varchar    // Variable-length string
};

/**
 * The possible column compression/encodings.
 */
enum class ColumnEncoding : uint8_t {
  None,
  Rle,
  Delta,
  IntegerDict,
  StringDict,
};

/**
 * All possible JOIN types.
 */
enum class JoinType : uint8_t { Inner, Outer, Left, Right, Anti, Semi };

/**
 * Simple structure representing a blob.
 */
struct Blob {
  uint8_t *data;
  uint64_t size;

  bool operator==(const Blob &that) const noexcept {
    return size == that.size && std::memcmp(data, that.data, size) == 0;
  }

  bool operator!=(const Blob &that) const noexcept { return !(*this == that); }
};

/**
 * Given an internal type, return the simplest SQL type. Note that this
 * conversion is a lossy since some internal types are used as the underlying
 * storage for multiple SQL types. For example, int32_t is used for SQL Integers and
 * SQL Dates).
 */
SqlTypeId GetSqlTypeFromInternalType(TypeId type);

/**
 * Given a templated type, return the associated internal type ID.
 */
template <class T>
constexpr inline TypeId GetTypeId() {
  if constexpr (std::is_same<T, bool>()) {
    return TypeId::Boolean;
  } else if constexpr (std::is_same<std::remove_const_t<T>, int8_t>()) {
    return TypeId::TinyInt;
  } else if constexpr (std::is_same<std::remove_const_t<T>, int16_t>()) {
    return TypeId::SmallInt;
  } else if constexpr (std::is_same<std::remove_const_t<T>, int32_t>()) {
    return TypeId::Integer;
  } else if constexpr (std::is_same<std::remove_const_t<T>, int64_t>()) {
    return TypeId::BigInt;
  } else if constexpr (std::is_same<std::remove_const_t<T>, hash_t>()) {
    return TypeId::Hash;
  } else if constexpr (std::is_same<std::remove_const_t<T>, uintptr_t>()) {
    return TypeId::Pointer;
  } else if constexpr (std::is_same<std::remove_const_t<T>, float>()) {
    return TypeId::Float;
  } else if constexpr (std::is_same<std::remove_const_t<T>, double>()) {
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
std::string TypeIdToString(TypeId type);

}  // namespace tpl::sql
