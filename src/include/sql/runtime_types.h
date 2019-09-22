#pragma once

#include <string>

#include "util/hash_util.h"

namespace tpl::sql {

// ---------------------------------------------------------
//
// Dates
//
// ---------------------------------------------------------

/**
 * A SQL date.
 */
class Date {
 public:
  /**
   * Empty constructor.
   */
  Date() = default;

  /**
   * Is this a valid date?
   * @return True if this is a valid date instance; false otherwise.
   */
  bool IsValid() const noexcept;

  /**
   * Convert this date object into a string of the form "YYYY-MM-DD"
   * @return The stringification of this date object.
   */
  std::string ToString() const;

  /**
   * Return the year corresponding to this date.
   * @return The year of the date.
   */
  uint32_t ExtractYear() const noexcept;

  /**
   * Return the month corresponding to this date.
   * @return The month of the date.
   */
  uint32_t ExtractMonth() const noexcept;

  /**
   * Return the day corresponding to this date.
   * @return The day of the date.
   */
  uint32_t ExtractDay() const noexcept;

  /**
   * Convert this date object into its year, month, and day parts.
   * @param[out] year The year corresponding to this date.
   * @param[out] month The month corresponding to this date.
   * @param[out] day The day corresponding to this date.
   */
  void Convert(uint32_t *year, uint32_t *month, uint32_t *day) {
    *year = ExtractYear();
    *month = ExtractMonth();
    *day = ExtractDay();
  }

  /**
   * Equality comparison.
   * @return True if this date equals @em that date.
   */
  bool operator==(const Date &that) const noexcept { return value_ == that.value_; }

  /**
   * Inequality comparison.
   * @return True if this date is not equal to @em that date.
   */
  bool operator!=(const Date &that) const noexcept { return value_ != that.value_; }

  /**
   * Less-than comparison.
   * @return True if this data occurs before @em that date.
   */
  bool operator<(const Date &that) const noexcept { return value_ < that.value_; }

  /**
   * Less-than-or-equal-to comparison.
   * @return True if this data occurs before or is the same as @em that date.
   */
  bool operator<=(const Date &that) const noexcept { return value_ <= that.value_; }

  /**
   * Greater-than comparison.
   * @return True if this date occurs after @em that date.
   */
  bool operator>(const Date &that) const noexcept { return value_ > that.value_; }

  /**
   * Greater-than-or-equal-to comparison.
   * @return True if this date occurs after or is equal to @em that date.
   */
  bool operator>=(const Date &that) const noexcept { return value_ >= that.value_; }

  /**
   * Convert a C-style string of the form "YYYY-MM-DD" into a date instance. Will attempt to convert
   * the first date-like object it sees, skipping any leading whitespace.
   * @param str The string to convert.
   * @param len The length of the string.
   * @return The constructed date. May be invalid.
   */
  static Date FromString(const char *str, std::size_t len);

  /**
   * Convert a string of the form "YYYY-MM-DD" into a date instance. Will attempt to convert the
   * first date-like object it sees, skipping any leading whitespace.
   * @param str The string to convert.
   * @return The constructed Date. May be invalid.
   */
  static Date FromString(const std::string &str) { return FromString(str.c_str(), str.size()); }

  /**
   * Create a Date instance from a specified year, month, and day.
   * @param year The year of the date.
   * @param month The month of the date.
   * @param day The day of the date.
   * @return The constructed date. May be invalid.
   */
  static Date FromYMD(uint32_t year, uint32_t month, uint32_t day);

  /**
   * Is the date corresponding to the given year, month, and day a valid date?
   * @param year The year of the date.
   * @param month The month of the date.
   * @param day The day of the date.
   * @return True if valid date.
   */
  static bool IsValidDate(uint32_t year, uint32_t month, uint32_t day) {
    return FromYMD(year, month, day).IsValid();
  }

  /**
   * Hash the date.
   * @return The hash value for this date instance.
   */
  hash_t Hash() const noexcept { return util::HashUtil::Hash(value_); }

 private:
  friend struct DateVal;

  // Private constructor to force static factories.
  explicit Date(uint32_t value) : value_(value) {}

 private:
  // Date value
  uint32_t value_;
};

}  // namespace tpl::sql
