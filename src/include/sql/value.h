#pragma once

#include <cstring>

#include "common/common.h"
#include "common/macros.h"
#include "sql/runtime_types.h"
#include "util/string_heap.h"

namespace tpl::sql {

//===----------------------------------------------------------------------===//
//
// Base SQL Value
//
//===----------------------------------------------------------------------===//

/**
 * A generic base catch-all SQL value
 */
struct Val {
  bool is_null;

  explicit Val(bool is_null = false) noexcept : is_null(is_null) {}
};

//===----------------------------------------------------------------------===//
//
// Boolean
//
//===----------------------------------------------------------------------===//

/**
 * A SQL boolean value
 */
struct BoolVal : public Val {
  bool val;

  explicit BoolVal(bool val) noexcept : Val(false), val(val) {}

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
   * Return a NULL boolean value
   */
  static BoolVal Null() {
    BoolVal val(false);
    val.is_null = true;
    return val;
  }
};

//===----------------------------------------------------------------------===//
//
// Integer
//
//===----------------------------------------------------------------------===//

/**
 * An integral SQL value
 */
struct Integer : public Val {
  int64_t val;

  explicit Integer(int64_t val) noexcept : Integer(false, val) {}
  explicit Integer(bool null, int64_t val) noexcept : Val(null), val(val) {}

  /**
   * Create a NULL integer
   */
  static Integer Null() {
    Integer val(0);
    val.is_null = true;
    return val;
  }
};

//===----------------------------------------------------------------------===//
//
// Real
//
//===----------------------------------------------------------------------===//

/**
 * An real and double SQL value
 */
struct Real : public Val {
  double val;

  /**
   * Construct a non-null real value from a 32-bit floating point value
   */
  explicit Real(float val) noexcept : Val(false), val(val) {}

  /**
   * Construct a non-null real value from a 64-bit floating point value
   */
  explicit Real(double val) noexcept : Val(false), val(val) {}

  /**
   * Return a NULL real value
   */
  static Real Null() {
    Real real(0.0);
    real.is_null = true;
    return real;
  }
};

//===----------------------------------------------------------------------===//
//
// Decimal
//
//===----------------------------------------------------------------------===//

/**
 * A fixed-point decimal SQL value
 */
struct Decimal : public Val {
  uint64_t val;
  uint32_t precision;
  uint32_t scale;

  Decimal(uint64_t val, uint32_t precision, uint32_t scale) noexcept
      : Val(false), val(val), precision(precision), scale(scale) {}

  /**
   * Return a NULL decimal value
   */
  static Decimal Null() {
    Decimal val(0, 0, 0);
    val.is_null = true;
    return val;
  }
};

//===----------------------------------------------------------------------===//
//
// String
//
//===----------------------------------------------------------------------===//

/**
 * A SQL string. SQL strings only and always <b>VIEWS</b> onto externally managed memory. They never
 * own the memory they point to! They're a very thin wrapper around VarlenEntrys used for String
 * processing.
 */
struct StringVal : public Val {
  VarlenEntry val;

  explicit StringVal(VarlenEntry v) noexcept : Val(false), val(v) {}

  /**
   * Create a string value (i.e., a view) over the given (potentially non-null terminated) string.
   * @param str The character sequence.
   * @param len The length of the sequence.
   */
  StringVal(const char *str, uint32_t len) noexcept
      : Val(false), val(VarlenEntry::Create(reinterpret_cast<const byte *>(str), len)) {
    TPL_ASSERT(str != nullptr, "String input cannot be NULL");
  }

  /**
   * Create a string value (i.e., view) over the C-style null-terminated string.
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
   * Create a NULL varchar/string
   */
  static StringVal Null() {
    StringVal result("");
    result.is_null = true;
    return result;
  }
};

//===----------------------------------------------------------------------===//
//
// Date
//
//===----------------------------------------------------------------------===//

/**
 * A SQL date value
 */
struct DateVal : public Val {
  Date val;

  DateVal() noexcept : Val(), val() {}

  explicit DateVal(Date v) noexcept : Val(false), val(v) {}

  /**
   * Create a NULL date
   */
  static DateVal Null() {
    DateVal date;
    date.is_null = true;
    return date;
  }
};

//===----------------------------------------------------------------------===//
//
// Timestamp
//
//===----------------------------------------------------------------------===//

/**
 * A SQL timestamp value
 */
struct TimestampVal : public Val {
  timespec time;

  explicit TimestampVal(timespec time) noexcept : Val(false), time(time) {}

  /**
   * Create a NULL timestamp
   */
  static TimestampVal Null() {
    TimestampVal timestamp({0, 0});
    timestamp.is_null = true;
    return timestamp;
  }
};

}  // namespace tpl::sql
