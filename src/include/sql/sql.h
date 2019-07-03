#pragma once

#include "util/common.h"

namespace tpl::sql {

/**
 * The primitive types underlying the SQL types.
 */
enum class TypeId : u8 {
  BOOLEAN,   // bool
  TINYINT,   // i8
  SMALLINT,  // i16
  INTEGER,   // i32
  BIGINT,    // i64
  HASH,      // hash_t
  POINTER,   // uintptr_t
  FLOAT,     // f32
  DOUBLE,    // f64
  VARCHAR,   // char*, representing a null-terminated UTF-8 string
  VARBINARY  // blob_t, representing arbitrary bytes
};

/**
 * Supported SQL data types.
 */
enum class SqlTypeId : u8 {
  Boolean,
  TinyInt,
  SmallInt,
  Integer,
  BigInt,
  Real,
  Decimal,
  Date,
  Char,
  Varchar
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

}  // namespace tpl::sql
