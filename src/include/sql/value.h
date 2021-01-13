#pragma once

#include <cstring>

#include "common/common.h"
#include "common/macros.h"
#include "sql/runtime_types.h"
#include "util/string_heap.h"

namespace tpl::sql {

/**
 * A generic base catch-all SQL value. Used to represent a NULL-able SQL value.
 */
struct Val {
  // NULL indication flag
  bool is_null;

  /**
   * Construct a value with the given NULL indication.
   * @param is_null Whether the SQL value is NULL.
   */
  explicit Val(bool is_null) noexcept : is_null(is_null) {}
};

/**
 * A NULL-able SQL boolean value.
 */
struct BoolVal : public Val {
  // The value
  bool val;

  /**
   * Construct a potentially NULL boolean with the given initial vale.
   * @param val The value of the boolean.
   * @param is_null The NULL indication.
   */
  explicit BoolVal(bool val, bool is_null) noexcept : Val(is_null), val(val) {}

  /**
   * Construct a non-NULL boolean with the given value.
   * @param val The value of the boolean.
   */
  explicit BoolVal(bool val) noexcept : BoolVal(val, false) {}

  /**
   * Convert this SQL boolean into a primitive boolean. Thanks to SQL's
   * three-valued logic, we implement the following truth table:
   *
   *   Value | NULL? | Output
   * +-------+-------+--------+
   * | false | false | false  |
   * | false | true  | false  |
   * | true  | false | true   |
   * | true  | true  | false  |
   * +-------+-------+--------+
   */
  bool ForceTruth() const noexcept { return !is_null && val; }

  /**
   * @return The logical compliment of this value.
   */
  BoolVal LogicalNot() const noexcept { return BoolVal(!val, is_null); }

  /**
   * @return The logical conjunction of @em this and @em o.
   */
  BoolVal LogicalAnd(const BoolVal &o) const { return BoolVal(val && o.val, is_null || o.is_null); }

  /**
   * @return The logical disjunction of @em this and @em o.
   */
  BoolVal LogicalOr(const BoolVal &o) const { return BoolVal(val || o.val, is_null || o.is_null); }

  /**
   * @return A NULL boolean value.
   */
  static BoolVal Null() { return BoolVal(false, true); }
};

/**
 * A NULL-able integral SQL value. Captures tinyint, smallint, integer and bigint.
 */
struct Integer : public Val {
  // The value
  int64_t val;

  /**
   * Construct a potentially NULL integer with the given initial vale.
   * @param val The value of the boolean.
   * @param is_null The NULL indication.
   */
  explicit Integer(int64_t val, bool is_null) noexcept : Val(is_null), val(val) {}

  /**
   * Construct a non-NULL integer with the given value.
   * @param val The value to set.
   */
  explicit Integer(int64_t val) noexcept : Integer(val, false) {}

  /**
   * @return A NULL integer.
   */
  static Integer Null() { return Integer(0, true); }
};

/**
 * A NULL-able single- and double-precision floating point SQL value.
 */
struct Real : public Val {
  // The value
  double val;

  /**
   * Construct a potentially NULL real value from a 64-bit floating point value
   * @param val The initial value.
   * @param is_null The NULL indication.
   */
  explicit Real(double val, bool is_null) noexcept : Val(is_null), val(val) {}

  /**
   * Construct a non-NULL real value from a 32-bit floating point value.
   * @param val The initial value.
   */
  explicit Real(float val) noexcept : Real(val, false) {}

  /**
   * Construct a non-NULL real value from a 64-bit floating point value
   * @param val The initial value.
   */
  explicit Real(double val) noexcept : Real(val, false) {}

  /**
   * @return A NULL Real value.
   */
  static Real Null() { return Real(0.0, true); }
};

/**
 * A NULL-able fixed-point decimal SQL value.
 */
struct DecimalVal : public Val {
  // The value
  Decimal64 val;

  /**
   * Construct a potentially NULL decimal value from the given 64-bit decimal value.
   * @param val The raw decimal value.
   * @param is_null The NULL indication.
   */
  explicit DecimalVal(Decimal64 val, bool is_null) noexcept : Val(is_null), val(val) {}

  /**
   * Construct a non-NULL decimal value from the given 64-bit decimal value.
   * @param val The decimal value.
   */
  explicit DecimalVal(Decimal64 val) noexcept : DecimalVal(val, false) {}

  /**
   * Construct a non-NULL decimal value from the given 64-bit decimal value.
   * @param val The raw decimal value.
   */
  explicit DecimalVal(Decimal64::NativeType val) noexcept : DecimalVal(Decimal64{val}) {}

  /**
   * @return A NULL decimal value.
   */
  static DecimalVal Null() { return DecimalVal(Decimal64{0}, true); }
};

/**
 * A NULL-able SQL string. These strings are always <b>views</b> onto externally managed memory.
 * They never own the memory they point to! They're a very thin wrapper around tpl::sql::VarlenEntry
 * used for string processing.
 */
struct StringVal : public Val {
  // The value
  VarlenEntry val;

  /**
   * Construct a potentially NULL string from the given string value.
   * @param val The string.
   * @param is_null The NULL indication.
   */
  explicit StringVal(VarlenEntry val, bool is_null) noexcept : Val(is_null), val(val) {}

  /**
   * Construct a non-NULL string from the given string value.
   * @param val The string.
   */
  explicit StringVal(VarlenEntry val) noexcept : StringVal(val, false) {}

  /**
   * Create a non-NULL string value (i.e., a view) over the given (potentially non-null terminated)
   * string.
   * @param str The character sequence.
   * @param len The length of the sequence.
   */
  StringVal(const char *str, uint32_t len) noexcept
      : StringVal(VarlenEntry::Create(reinterpret_cast<const byte *>(str), len), false) {
    TPL_ASSERT(str != nullptr, "String input cannot be NULL");
  }

  /**
   * Create a non-NULL string value (i.e., view) over the C-style null-terminated string.
   * @param str The C-string.
   */
  explicit StringVal(const char *str) noexcept : StringVal(const_cast<char *>(str), strlen(str)) {}

  /**
   * Get the length of the string value.
   * @return The length of the string in bytes.
   */
  std::size_t GetLength() const noexcept { return val.GetSize(); }

  /**
   * Return a pointer to the bytes underlying the string.
   * @return A pointer to the underlying content.
   */
  const char *GetContent() const noexcept {
    return reinterpret_cast<const char *>(val.GetContent());
  }

  /**
   * Compare if this (potentially nullable) string value is equivalent to
   * another string value, taking NULLness into account.
   * @param that The string value to compare with.
   * @return True if equivalent; false otherwise.
   */
  bool operator==(const StringVal &that) const {
    if (is_null != that.is_null) {
      return false;
    }
    if (is_null) {
      return true;
    }
    return val == that.val;
  }

  /**
   * Is this string not equivalent to another?
   * @param that The string value to compare with.
   * @return True if not equivalent; false otherwise.
   */
  bool operator!=(const StringVal &that) const { return !(*this == that); }

  /**
   * @return A NULL varchar/string.
   */
  static StringVal Null() {
    StringVal result("");
    result.is_null = true;
    return result;
  }
};

/**
 * A NULL-able SQL date value.
 */
struct DateVal : public Val {
  // The value
  Date val;

  /**
   * Construct a non-NULL date with the given date value.
   * @param val The value.
   * @param is_null The NULL indication.
   */
  explicit DateVal(Date val, bool is_null) noexcept : Val(is_null), val(val) {}

  /**
   * Construct a non-NULL date with the given date value.
   * @param val The value.
   */
  explicit DateVal(Date val) noexcept : DateVal(val, false) {}

  /**
   * Construct a non-NULL date with the given date value.
   * @param val The value.
   */
  explicit DateVal(Date::NativeType val) noexcept : DateVal(Date{val}) {}

  /**
   * @return A NULL date.
   */
  static DateVal Null() { return DateVal(Date{}, true); }
};

/**
 * A NULL-able SQL timestamp value.
 */
struct TimestampVal : public Val {
  // The value
  Timestamp val;

  /**
   * Construct a non-NULL timestamp with the given value.
   * @param val The timestamp.
   * @param is_null The NULL indication.
   */
  explicit TimestampVal(Timestamp val, bool is_null) noexcept : Val(is_null), val(val) {}

  /**
   * Construct a non-NULL timestamp with the given value.
   * @param val The timestamp.
   */
  explicit TimestampVal(Timestamp val) noexcept : TimestampVal(val, false) {}

  /**
   * Construct a non-NULL timestamp with the given raw timestamp value
   * @param val The raw timestamp value.
   */
  explicit TimestampVal(Timestamp::NativeType val) noexcept : TimestampVal(Timestamp{val}) {}

  /**
   * @return A NULL timestamp.
   */
  static TimestampVal Null() { return TimestampVal(Timestamp{0}, true); }
};

}  // namespace tpl::sql
