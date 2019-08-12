#pragma once

#include <iosfwd>
#include <string>
#include <string_view>

#include "sql/data_types.h"
#include "util/common.h"

namespace tpl::sql {

struct Val;

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
   * Is this value equal to the provided value? This is NOT SQL equivalence!
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

  /**
   * Generic value equality. This is NOT SQL equality!
   */
  bool operator==(const GenericValue &that) const { return this->Equals(that); }

  /**
   * Generic value inequality. This is NOT SQL inequality!
   */
  bool operator!=(const GenericValue &that) const { return !(*this == that); }

  //===--------------------------------------------------------------------===//
  //
  // Static factory methods
  //
  //===--------------------------------------------------------------------===//

  /**
   * Create a NULL value.
   * @param type_id The type of the value.
   * @return A NULL value.
   */
  static GenericValue CreateNull(TypeId type_id);

  /**
   * Create a non-NULL boolean value.
   * @param value The value.
   * @return A Boolean value.
   */
  static GenericValue CreateBoolean(bool value);

  /**
   * Create a non-NULL tinyint value.
   * @param value The value.
   * @return A TinyInt value.
   */
  static GenericValue CreateTinyInt(i8 value);

  /**
   * Create a non-NULL smallint value.
   * @param value The value.
   * @return A SmallInt value.
   */
  static GenericValue CreateSmallInt(i16 value);

  /**
   * Create a non-NULL integer value.
   * @param value The value.
   * @return An Integer value.
   */
  static GenericValue CreateInteger(i32 value);

  /**
   * Create a non-NULL bigint value.
   * @param value The value.
   * @return A BigInt value.
   */
  static GenericValue CreateBigInt(i64 value);

  /**
   * Create a non-NULL hash value.
   * @param value The value.
   * @return A hash value.
   */
  static GenericValue CreateHash(hash_t value);

  /**
   * Create a non-NULL pointer value.
   * @param value The value.
   * @return A pointer value.
   */
  static GenericValue CreatePointer(uintptr_t value);

  /**
   * Create a non-NULL pointer value.
   * @param value The value.
   * @return A pointer value.
   */
  template <typename T>
  static GenericValue CreatePointer(T *pointer) {
    return CreatePointer(reinterpret_cast<uintptr_t>(pointer));
  }

  /**
   * Create a non-NULL real value.
   * @param value The value.
   * @return A Real value.
   */
  static GenericValue CreateReal(f32 value);

  /**
   * Create a non-NULL float value.
   * @param value The value.
   * @return A float value.
   */
  static GenericValue CreateFloat(f32 value) { return CreateReal(value); }

  /**
   * Create a non-NULL double value.
   * @param value The value.
   * @return A Double value.
   */
  static GenericValue CreateDouble(f64 value);

  /**
   * Create a non-NULL date value.
   * @param value The value.
   * @return A Date value.
   */
  static GenericValue CreateDate(i32 year, i32 month, i32 day);

  /**
   * Create a non-NULL timestamp value.
   * @param value The value.
   * @return A Timestamp value.
   */
  static GenericValue CreateTimestamp(i32 year, i32 month, i32 day, i32 hour,
                                      i32 min, i32 sec, i32 msec);

  /**
   * Create a non-NULL varchar value.
   * @param value The value.
   * @return A Varchar value.
   */
  static GenericValue CreateVarchar(std::string_view str);

  /**
   * Create a generic value from a runtime value.
   * @param type_id
   * @param val
   * @return
   */
  static GenericValue CreateFromRuntimeValue(TypeId type_id, const Val &val);

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
  union {
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
