#pragma once

#include <cstring>

#include "common/common.h"
#include "common/macros.h"
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
 * A SQL string
 */
struct StringVal : public Val {
  static constexpr std::size_t kMaxStingLen = 1 * GB;

  char *ptr;
  uint32_t len;

  /**
   * Create a string value (i.e., a view) over the given potentially non-null
   * terminated byte sequence.
   * @param str The byte sequence.
   * @param len The length of the sequence.
   */
  StringVal(char *str, uint32_t len) noexcept : Val(str == nullptr), ptr(str), len(len) {}

  /**
   * Create a string value (i.e., view) over the C-style null-terminated string.
   * Note that no copy is made.
   * @param str The C-string.
   */
  explicit StringVal(const char *str) noexcept : StringVal(const_cast<char *>(str), strlen(str)) {}

  /**
   * Create a new string using the given memory pool and length.
   * @param memory The memory pool to allocate this string's contents from
   * @param len The size of the string
   */
  StringVal(util::StringHeap *memory, std::size_t len) : ptr(nullptr), len(len) {
    if (TPL_UNLIKELY(len > kMaxStingLen)) {
      len = 0;
      is_null = true;
    } else {
      ptr = reinterpret_cast<char *>(memory->Allocate(len));
    }
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
    if (len != that.len) {
      return false;
    }
    return ptr == that.ptr || memcmp(ptr, that.ptr, len) == 0;
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
  static StringVal Null() { return StringVal(static_cast<char *>(nullptr), 0); }
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
  int32_t date_val;

  explicit DateVal(int32_t date) noexcept : Val(false), date_val(date) {}

  /**
   * Create a NULL date
   */
  static DateVal Null() {
    DateVal date(0);
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
