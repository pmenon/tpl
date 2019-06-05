#pragma once

#include <cstring>

#include "sql/execution_context.h"
#include "util/common.h"
#include "util/macros.h"

namespace tpl::sql {

#define AVG_PRECISION 3
#define AVG_SCALE 6

/**
 * A generic base catch-all SQL value
 */
struct Val {
  bool is_null;

  explicit Val(bool is_null = false) noexcept : is_null(is_null) {}
};

// ---------------------------------------------------------
// Boolean
// ---------------------------------------------------------

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

// ---------------------------------------------------------
// Integer
// ---------------------------------------------------------

/**
 * An integral SQL value
 */
struct Integer : public Val {
  i64 val;

  explicit Integer(i64 val) noexcept : Integer(false, val) {}
  explicit Integer(bool null, i64 val) noexcept : Val(null), val(val) {}

  /**
   * Create a NULL integer
   */
  static Integer Null() {
    Integer val(0);
    val.is_null = true;
    return val;
  }

  Integer Add(const Integer &that, bool *overflow) const {
    i64 result;
    *overflow = __builtin_add_overflow(val, that.val, &result);
    return Integer(is_null || that.is_null, result);
  }

  Integer Sub(const Integer &that, bool *overflow) const {
    i64 result;
    *overflow = __builtin_sub_overflow(val, that.val, &result);
    return Integer(is_null || that.is_null, result);
  }

  Integer Multiply(const Integer &that, bool *overflow) const {
    i64 result;
    *overflow = __builtin_mul_overflow(val, that.val, &result);
    return Integer(is_null || that.is_null, result);
  }

  Integer Divide(const Integer &that) const {
    Integer result(0);
    if (that.val == 0) {
      result.is_null = true;
    } else {
      result.val = (val / that.val);
      result.is_null = false;
    }
    return result;
  }

  Integer Modulo(const Integer &that) const {
    Integer result(0);
    if (that.val == 0) {
      result.is_null = true;
    } else {
      result.val = (val % that.val);
      result.is_null = false;
    }
    return result;
  }
};

// ---------------------------------------------------------
// Real
// ---------------------------------------------------------

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

// ---------------------------------------------------------
// Decimal
// ---------------------------------------------------------

/**
 * A fixed-point decimal SQL value
 */
struct Decimal : public Val {
  u64 val;
  u32 precision;
  u32 scale;

  Decimal(u64 val, u32 precision, u32 scale) noexcept
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

// ---------------------------------------------------------
// String
// ---------------------------------------------------------

/**
 * A SQL string
 */
struct StringVal : public Val {
  static constexpr std::size_t kMaxStingLen = 1 * GB;

  char *ptr;
  u32 len;

  /**
   * Create a string value (i.e., a view) over the given potentially non-null
   * terminated byte sequence.
   * @param str The byte sequence.
   * @param len The length of the sequence.
   */
  StringVal(char *str, u32 len) noexcept
      : Val(str == nullptr), ptr(str), len(len) {}

  /**
   * Create a string value (i.e., view) over the C-style null-terminated string.
   * Note that no copy is made.
   * @param str The C-string.
   */
  explicit StringVal(const char *str) noexcept
      : StringVal(const_cast<char *>(str), strlen(str)) {}

  /**
   * Create a new string using the given memory pool and length.
   * @param memory The memory pool to allocate this string's contents from
   * @param len The size of the string
   */
  StringVal(ExecutionContext::StringAllocator *memory, std::size_t len);

  i32 Compare(const StringVal &that) const;

  bool Eq(const StringVal &that) const { return Compare(that) == 0; }
  bool Ge(const StringVal &that) const { return Compare(that) >= 0; }
  bool Gt(const StringVal &that) const { return Compare(that) > 0; }
  bool Le(const StringVal &that) const { return Compare(that) <= 0; }
  bool Lt(const StringVal &that) const { return Compare(that) < 0; }
  bool Ne(const StringVal &that) const { return Compare(that) != 0; }

  bool operator==(const StringVal &that) const { return Eq(that); }
  bool operator>=(const StringVal &that) const { return Ge(that); }
  bool operator>(const StringVal &that) const { return Gt(that); }
  bool operator<=(const StringVal &that) const { return Le(that); }
  bool operator<(const StringVal &that) const { return Lt(that); }
  bool operator!=(const StringVal &that) const { return Ne(that); }

  /**
   * Create a NULL varchar/string
   */
  static StringVal Null() { return StringVal(static_cast<char *>(nullptr), 0); }

  /**
   * Compare two strings. Returns:
   * < 0 if s1 < s2
   * 0 if s1 == s2
   * > 0 if s1 > s2
   *
   * @param s1 The first string
   * @param len1 The length of the first string
   * @param s2 The second string
   * @param len2 The length of the second string
   * @param min_len The minimum length between the two input strings
   * @return The appropriate signed value idicated comparison order
   */
  static i32 StringCompare(const char *s1, std::size_t len1, const char *s2,
                           std::size_t len2, std::size_t min_len);
};

inline StringVal::StringVal(ExecutionContext::StringAllocator *memory,
                            std::size_t len)
    : ptr(nullptr), len(len) {
  if (TPL_UNLIKELY(len > kMaxStingLen)) {
    len = 0;
    is_null = true;
  } else {
    ptr = reinterpret_cast<char *>(memory->Allocate(len));
  }
}

inline i32 StringVal::StringCompare(const char *s1, std::size_t len1,
                                    const char *s2, std::size_t len2,
                                    std::size_t min_len) {
  const auto result = (min_len == 0) ? 0 : memcmp(s1, s2, min_len);
  if (result != 0) {
    return result;
  }
  return len1 - len2;
}

inline i32 StringVal::Compare(const StringVal &that) const {
  const auto min_len = std::min(len, that.len);
  if (min_len == 0) {
    return len == that.len ? 0 : (len == 0 ? -1 : 1);
  }
  return StringCompare(ptr, len, that.ptr, that.len, min_len);
}

// ---------------------------------------------------------
// Date
// ---------------------------------------------------------

/**
 * A SQL date value
 */
struct Date : public Val {
  i32 date_val;

  explicit Date(i32 date) noexcept : Val(false), date_val(date) {}

  /**
   * Create a NULL date
   */
  static Date Null() {
    Date date(0);
    date.is_null = true;
    return date;
  }
};

// ---------------------------------------------------------
// Timestamp
// ---------------------------------------------------------

/**
 * A SQL timestamp value
 */
struct Timestamp : public Val {
  timespec time;

  explicit Timestamp(timespec time) noexcept : Val(false), time(time) {}

  /**
   * Create a NULL timestamp
   */
  static Timestamp Null() {
    Timestamp timestamp({0, 0});
    timestamp.is_null = true;
    return timestamp;
  }
};

}  // namespace tpl::sql
