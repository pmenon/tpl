#pragma once

#include "util/common.h"
#include "util/macros.h"

namespace tpl::sql {

/**
 * Supported SQL data types
 */
enum class TypeId : u8 { Boolean, SmallInt, Integer, BigInt, Decimal, Varchar };

/**
 * A type supported in SQL. Every SQL type has an underlying native type (e.g.,
 * a 16-bit integer for SMALLINT), a boolean flag indicating whether the value
 * is NULL-able, and an opaque union containing auxiliary information depending
 * on the SQL type.
 */
class Type {
 public:
  Type(TypeId type_id, bool nullable)
      : type_id_(type_id), nullable_(nullable), aux_() {}

  Type(TypeId type_id, bool nullable, u32 precision, u32 scale)
      : type_id_(type_id), nullable_(nullable), aux_() {
    aux_.numeric_info.precision = precision;
    aux_.numeric_info.scale = scale;
  }

  TypeId type_id() const { return type_id_; }

  bool nullable() const { return nullable_; }

  u32 precision() const {
    TPL_ASSERT(IsDecimal(), "Non decimal values don't have a precision");
    return aux_.numeric_info.precision;
  }

  u32 scale() const {
    TPL_ASSERT(IsDecimal(), "Non decimal values don't have a scale");
    return aux_.numeric_info.scale;
  }

  u32 varlen() const {
    TPL_ASSERT(IsString(), "Non strings don't have a variable length");
    return aux_.varlen;
  }

  Type AsNullable() const {
    Type ret = *this;
    ret.nullable_ = true;
    return ret;
  }

  bool IsBoolean() const { return type_id() == TypeId::Boolean; }

  bool IsIntegral() const {
    switch (type_id()) {
      case TypeId::SmallInt:
      case TypeId::Integer:
      case TypeId::BigInt: {
        return true;
      }
      default: { return false; }
    }
  }

  bool IsDecimal() const {
    switch (type_id()) {
      case TypeId::Decimal: {
        return true;
      }
      default: { return false; }
    }
  }

  bool IsArithmetic() const { return IsIntegral() || IsDecimal(); }

  bool IsString() const {
    switch (type_id()) {
      case TypeId::Varchar: {
        return true;
      }
      default: { return false; }
    }
  }

  bool operator==(const Type &other) const {
    if (type_id() != other.type_id()) {
      return false;
    }
    if (nullable() != other.nullable()) {
      return false;
    }
    if (IsDecimal() &&
        (precision() != other.precision() || scale() != other.scale())) {
      return false;
    } else if (IsString() && (varlen() != other.varlen())) {
      return false;
    }

    return true;
  }

 private:
  TypeId type_id_;
  bool nullable_;

  union AuxInfo {
    /*
     * For numeric and decimal, we store the precision and scale
     */
    struct {
      u32 precision;
      u32 scale;
    } numeric_info;

    /*
     * For variable-length types, we store the length here
     */
    u32 varlen;
  } aux_;
};

}  // namespace tpl::sql