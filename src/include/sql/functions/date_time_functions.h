#pragma once

#include "sql/value.h"

namespace tpl::sql {

/**
 * Date/timestamp functions.
 */
class DateTimeFunctions : public AllStatic {
 public:
  /**
   * @return The century the SQL date falls into.
   */
  static Integer Century(const TimestampVal &time) noexcept;

  /**
   * @return The decade the NULL-able SQL date falls into. This is the year field divided by 10.
   */
  static Integer Decade(const TimestampVal &time) noexcept;

  /**
   * @return The year component of the NULL-able SQL timestamp.
   */
  static Integer Year(const TimestampVal &time) noexcept;

  /**
   * @return The quarter (1-4) the NULL-able SQL timestamp falls into.
   */
  static Integer Quarter(const TimestampVal &time) noexcept;

  /**
   * @return The month component (1-12) of the NULL-able SQL timestamp. Note the returned month is
   *         1-based, NOT 0-based.
   */
  static Integer Month(const TimestampVal &time) noexcept;

  /**
   * @return The date component (1-31) of the NULL-able SQL timestamp. Note that the returned day is
   *         1-based, NOT 0-based.
   */
  static Integer Day(const TimestampVal &time) noexcept;

  /**
   * @return The day-of-the-week (0-Sun, 1-Mon, 2-Tue, 3-Wed, 4-Thu, 5-Fri, 6-Sat) of the NULL-able
   *         SQL timestamp.
   */
  static Integer DayOfWeek(const TimestampVal &time) noexcept;

  /**
   * @return The day-of-the-year (1-366) of the NULL-able SQL timestamp.
   */
  static Integer DayOfYear(const TimestampVal &time) noexcept;

  /**
   * @return The hour component (0-23) of the NULL-able SQL timestamp.
   */
  static Integer Hour(const TimestampVal &time) noexcept;

  /**
   * @return The minute component (0-59) of the NULL-able SQL timestamp.
   */
  static Integer Minute(const TimestampVal &time) noexcept;

  /**
   * @return The second component (0-59) of the NULL-able SQL timestamp.
   */
  static Integer Second(const TimestampVal &time) noexcept;

  /**
   * @return The milliseconds component of the NULL-able SQL timestamp. This is the seconds field
   *          plus fractional seconds, multiplied by 1000.
   */
  static Integer Millisecond(const TimestampVal &time) noexcept;

  /**
   * @return The microseconds component of the NULL-able SQL timestamp. This is the seconds field
   *         plus the fractional seconds, multiplied by 1000000.
   */
  static Integer Microseconds(const TimestampVal &time) noexcept;
};

}  // namespace tpl::sql
