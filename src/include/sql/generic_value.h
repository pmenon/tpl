#pragma once

#include <iosfwd>
#include <string>
#include <string_view>

#include "sql/data_types.h"
#include "util/common.h"

namespace tpl::sql {

/**
 * A generic value is a glorified typed-union representing some primitive value.
 * This is purely a container class and isn't used for actual expression
 * evaluation or performance-critical execution.
 */
class GenericValue {
  friend class Vector;
  friend class VectorOps;
  friend class GenericValueTests;

 public:
  /**
   * Return the SQL type of this value.
   */
  TypeId type_id() const { return type_id_; }

  /**
   * Is this value NULL?
   */
  bool is_null() const { return is_null_; }

  /**
   * Is this value equal to the provided value?
   * @param other The value to compare with.
   * @return True if equal; false otherwise.
   */
  bool Equals(const GenericValue &other) const;

  /**
   * Cast this value to the given type.
   * @param type The type to cast to.
   */
  GenericValue CastTo(TypeId type);

  /**
   * Copy this value.
   */
  GenericValue Copy() const { return GenericValue(*this); }

  /**
   * Convert this generic value into a string.
   * @return The string representation of this value.
   */
  std::string ToString() const;

  bool operator==(const GenericValue &that) const { return this->Equals(that); }
  bool operator!=(const GenericValue &that) const { return !(*this == that); }

  // -------------------------------------------------------
  // Static factory methods
  // -------------------------------------------------------

  static GenericValue CreateNull(TypeId type_id);

  // Create a tinyint value from a specified value.
  static GenericValue CreateBoolean(bool value);

  // Create a tinyint value from a specified value.
  static GenericValue CreateTinyInt(i8 value);

  // Create a smallint value from a specified value.
  static GenericValue CreateSmallInt(i16 value);

  // Create an integer value from a specified value.
  static GenericValue CreateInteger(i32 value);

  // Create a bigint value from a specified value.
  static GenericValue CreateBigInt(i64 value);

  // Create a value representing the specified hash value.
  static GenericValue CreateHash(hash_t value);

  // Create a value representing the specified opaque pointer
  static GenericValue CreatePointer(uintptr_t value);

  // Create a value representing a pointer value.
  template <typename T>
  static GenericValue CreatePointer(T *pointer) {
    return CreatePointer(reinterpret_cast<uintptr_t>(pointer));
  }

  // Create a real value from a specified value.
  static GenericValue CreateReal(f32 value);

  // Create a float value from a specified value.
  static GenericValue CreateFloat(f32 value) { return CreateReal(value); }

  // Create a double value from a specified value.
  static GenericValue CreateDouble(f64 value);

  // Create a date value from a specified date.
  static GenericValue CreateDate(i32 year, i32 month, i32 day);

  // Create a timestamp value from a specified timestamp in separate
  // values.
  static GenericValue CreateTimestamp(i32 year, i32 month, i32 day, i32 hour,
                                      i32 min, i32 sec, i32 msec);

  // Create a string value from a NULL-terminated C-style string.
  static GenericValue CreateString(const char *str);

  // Create a string value from a C++ string.
  static GenericValue CreateString(std::string_view str);

  // Output
  friend std::ostream &operator<<(std::ostream &out, const GenericValue &val);

 private:
  explicit GenericValue(TypeId type_id) : type_id_(type_id), is_null_(true) {}

 private:
  // The primitive type
  TypeId type_id_;
  // Is this value null?
  bool is_null_;
  // The value of the object if it's a fixed-length type
  union Val {
    bool boolean;
    i8 tinyint;
    i16 smallint;
    i32 integer;
    i64 bigint;
    hash_t hash;
    uintptr_t pointer;
    f32 float_;
    f64 double_;
  } value_;
  // The value of the object if it's a variable size type.
  std::string str_value_;
};

}  // namespace tpl::sql
