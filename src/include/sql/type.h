#pragma once

#include <string>

#include "sql/sql.h"

namespace tpl::sql {

namespace planner {
class ConstantValueExpression;
}  // namespace planner

/**
 * This class captures schema information about a SQL type. SQL types have a dedicated type ID, a
 * boolean NULL indication flag, and auxiliary type-based information. These types are meant to be
 * lightweight and easily copyable.
 *
 * All SQL types are backed (i.e., implemented) using one or more primitive types. The ID of the
 * backing primitive type can be queried through Type::GetPrimitiveTypeId(). Types are be compared
 * for equality or ordering through the overloaded comparison operators.
 */
class Type {
 public:
  /**
   * @return The SQL type ID.
   */
  SqlTypeId GetTypeId() const { return type_id_; }

  /**
   * @return The underlying primitive type ID used to represent the SQL type.
   */
  TypeId GetPrimitiveTypeId() const;

  /**
   * @return True if this type is NULL-able; false otherwise.
   */
  bool IsNullable() const { return nullable_; }

  /**
   * @return The maximum length string this type represents. Zero if the type is not string-like.
   */
  uint32_t GetMaxStringLength() const;

  /**
   * @return A string representation of this type.
   */
  std::string ToString() const;

  /**
   * @return A string representation of this type, but without any NULL-ability information.
   */
  std::string ToStringWithoutNullability() const;

  /**
   * @return True if this type is equivalent to the provided type; false otherwise.
   */
  bool operator==(const Type &that) const;

  /**
   * @return True if this type is not equivalent to the provided type; false otherwise.
   */
  bool operator!=(const Type &that) const { return !(*this == that); }

  /**
   * @return True if this type is less than the provided type; false otherwise.
   */
  bool operator<(const Type &that) const;

  /**
   * @return True if this type is less than or equal to the provided type; false otherwise.
   */
  bool operator<=(const Type &that) const { return (*this == that || *this < that); }

  /**
   * @return True if this type is greater than the provided type; false otherwise.
   */
  bool operator>(const Type &that) const { return !(*this <= that); }

  /**
   * @return True if this type is greater than or equal to the provided type; false otherwise.
   */
  bool operator>=(const Type &that) const { return !(that > *this); }

#define GEN_SIMPLE_TYPE_FACTORY(Name)                            \
  /** @return A new SQL type with the requested NULL-ability. */ \
  static Type Name##Type(bool nullable) { return Type(SqlTypeId::Name, nullable); }

  GEN_SIMPLE_TYPE_FACTORY(Boolean)
  GEN_SIMPLE_TYPE_FACTORY(TinyInt)
  GEN_SIMPLE_TYPE_FACTORY(SmallInt)
  GEN_SIMPLE_TYPE_FACTORY(Integer)
  GEN_SIMPLE_TYPE_FACTORY(BigInt)
  GEN_SIMPLE_TYPE_FACTORY(Real)
  GEN_SIMPLE_TYPE_FACTORY(Double)
#undef GEN_SIMPLE_TYPE_FACTORY

  /**
   * @return A fixed-point DECIMAL SQL type with the requested NULL-ability, precision, and scale.
   */
  static Type DecimalType(bool nullable, uint32_t precision, uint32_t scale) {
    return Type(SqlTypeId::Decimal, nullable, NumericInfo{precision, scale});
  }

  /**
   * @return A new DATE SQL type with the requested NULL-ability.
   */
  static Type DateType(bool nullable) { return Type(SqlTypeId::Date, nullable); }

  /**
   * @return A new TIMESTAMP SQL type with the requested NULL-ability.
   */
  static Type TimestampType(bool nullable) { return Type(SqlTypeId::Timestamp, nullable); }

  /**
   * @return A new CHAR SQL type with the requested NULL-ability and maximum length.
   */
  static Type CharType(bool nullable, uint32_t max_len) {
    return Type(SqlTypeId::Varchar, nullable, VarcharInfo{max_len});
  }

  /**
   * @return A new VARCHAR SQL type with the requested NULL-ability and maximum length.
   */
  static Type VarcharType(bool nullable, uint32_t max_len) {
    return Type(SqlTypeId::Varchar, nullable, VarcharInfo{max_len});
  }

 private:
  // Information about varchars.
  struct VarcharInfo {
    uint32_t max_len;
    bool operator==(const VarcharInfo &that) const noexcept = default;
  };

  // Information about chars.
  struct CharInfo {
    uint32_t len;
    bool operator==(const CharInfo &that) const noexcept = default;
  };

  // Information about numerics.
  struct NumericInfo {
    uint32_t precision;
    uint32_t scale;
    bool operator==(const NumericInfo &that) const noexcept = default;
  };

 private:
  friend class planner::ConstantValueExpression;

  // Private constructors to force factory functions.
  Type(SqlTypeId type, bool nullable);
  Type(SqlTypeId type, bool nullable, VarcharInfo varchar_info);
  Type(SqlTypeId type, bool nullable, CharInfo char_info);
  Type(SqlTypeId type, bool nullable, NumericInfo numeric_info);

 private:
  union {
    VarcharInfo varchar_info_;
    CharInfo char_info_;
    NumericInfo numeric_info_;
  };
  // The actual SQL type.
  SqlTypeId type_id_;
  // NULL-ability indicator.
  bool nullable_;
};

}  // namespace tpl::sql
