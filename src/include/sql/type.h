#pragma once

#include "util/common.h"

namespace tpl::sql {

/**
 * Supported SQL data types
 */
enum class TypeId : u8 { SmallInt, Integer, BigInt, Decimal, Varchar };

/**
 * A type supported in SQL. Every SQL type has an underlying native type (e.g.,
 * a 16-bit integer for SMALLINT), a boolean flag indicating whether the value
 * is NULL-able, and an opaque union containing auxiliary information depending
 * on the SQL type.
 */
struct Type {
  TypeId type_id;
  bool nullable;

  union AuxInfo {
    // For numerics, we store the precision and scale
    struct {
      uint32_t precision;
      uint32_t scale;
    } numeric_info;

    // For variable-length types, we store the length here
    uint32_t varlen;
  } aux;

  bool IsIntegral() const {
    switch (type_id) {
      case TypeId::SmallInt:
      case TypeId::Integer:
      case TypeId::BigInt: {
        return true;
      }
      default: { return false; }
    }
  }

  bool IsDecimal() const {
    switch (type_id) {
      case TypeId::Decimal: {
        return true;
      }
      default: { return false; }
    }
  }

  bool IsVarchar() const {
    switch (type_id) {
      case TypeId::Varchar: {
        return true;
      }
      default: { return false; }
    }
  }
};

}  // namespace tpl::sql