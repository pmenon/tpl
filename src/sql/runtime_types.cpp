#include "sql/runtime_types.h"

#include <array>
#include <charconv>
#include <string>

#include "spdlog/fmt/fmt.h"

#include "common/exception.h"
#include "sql/sql.h"

namespace tpl::sql {

namespace {

// TODO(pmenon): Should be configurable.
constexpr DateOrderFormat kDateOrder = DateOrderFormat::MDY;

constexpr std::size_t kMaxDateTimeLen = 128;

constexpr int64_t kMonthsPerYear = 12;
constexpr int32_t kDaysPerMonth[2][12] = {{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
                                          {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}};
constexpr const char *const kMonthNames[] = {"January",   "February", "March",    "April",
                                             "May",       "June",     "July",     "August",
                                             "September", "October",  "November", "December"};
constexpr const char *const kDayNames[] = {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};

constexpr int64_t kHoursPerDay = 24;
constexpr int64_t kMinutesPerHour = 60;
constexpr int64_t kSecondsPerMinute = 60;

constexpr std::size_t kMaxTimestampPrecision = 6;

// Like Postgres, TPL stores dates as Julian Date Numbers. Julian dates are
// commonly used in astronomical applications and in software since it's
// numerically accurate and computationally simple. BuildJulianDate() and
// SplitJulianDate() correctly convert between Julian day and Gregorian
// calendar for all non-negative Julian days (i.e., from 4714-11-24 BC to
// 5874898-06-03 AD). Though the JDN number is unsigned, it's physically
// stored as a signed 32-bit integer, and comparison functions also use
// signed integer logic.
//
// Many of the conversion functions are adapted from implementations in
// Postgres. Specifically, we use the algorithms date2j() and j2date()
// in src/backend/utils/adt/datetime.c.

constexpr int64_t kJulianMinYear = -4713;
constexpr int64_t kJulianMinMonth = 11;
// constexpr int64_t kJulianMinDay = 24;
constexpr int64_t kJulianMaxYear = 5874898;
constexpr int64_t kJulianMaxMonth = 6;
// constexpr int64_t kJulianMaxDay = 3;

// Is the provided year a leap year?
bool IsLeapYear(int32_t year) { return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0); }

// Does the provided date fall into the Julian date range?
bool IsValidJulianDate(int32_t y, int32_t m, int32_t d) {
  return (y > kJulianMinYear || (y == kJulianMinYear && m >= kJulianMinMonth)) &&
         (y < kJulianMaxYear || (y == kJulianMaxYear && m < kJulianMaxMonth));
}

// Is the provided date a valid calendar date?
bool IsValidCalendarDate(int32_t year, int32_t month, int32_t day) {
  // There isn't a year 0. We represent 1 BC as year zero, 2 BC as -1, etc.
  if (year == 0) return false;

  // Month.
  if (month < 1 || month > kMonthsPerYear) return false;

  // Day.
  if (day < 1 || day > kDaysPerMonth[IsLeapYear(year)][month - 1]) return false;

  // Looks good.
  return true;
}

// Based on date2j().
uint32_t BuildJulianDate(uint32_t year, uint32_t month, uint32_t day) {
  if (month > 2) {
    month += 1;
    year += 4800;
  } else {
    month += 13;
    year += 4799;
  }

  int32_t century = year / 100;
  int32_t julian = year * 365 - 32167;
  julian += year / 4 - century + century / 4;
  julian += 7834 * month / 256 + day;

  return julian;
}

// Based on j2date().
void SplitJulianDate(int32_t jd, int32_t *year, int32_t *month, int32_t *day) {
  uint32_t julian = jd;
  julian += 32044;
  uint32_t quad = julian / 146097;
  uint32_t extra = (julian - quad * 146097) * 4 + 3;
  julian += 60 + quad * 3 + extra / 146097;
  quad = julian / 1461;
  julian -= quad * 1461;
  int32_t y = julian * 4 / 1461;
  julian = ((y != 0) ? ((julian + 305) % 365) : ((julian + 306) % 366)) + 123;
  y += quad * 4;
  *year = y - 4800;
  quad = julian * 2141 / 65536;
  *day = julian - 7834 * quad / 256;
  *month = (quad + 10) % kMonthsPerYear + 1;
}

// Based on j2day()
int32_t JulianDateToDay(int32_t date) {
  date += 1;
  date %= 7;

  // Cope if division truncates towards zero, as it probably does.
  if (date < 0) date += 7;

  return date;
}

// Split a Julian time (i.e., Julian date in microseconds) into a time and date
// component.
void StripTime(int64_t jd, int64_t *date, int64_t *time) {
  *date = jd / kMicroSecondsPerDay;
  *time = jd - (*date * kMicroSecondsPerDay);
}

// Given hour, minute, and second components, build a time in microseconds.
int64_t BuildTime(int32_t hour, int32_t min, int32_t sec) {
  return (((hour * kMinutesPerHour + min) * kSecondsPerMinute) * kMicroSecondsPerSecond) +
         sec * kMicroSecondsPerSecond;
}

// Given a time in microseconds, split it into hour, minute, second, and
// fractional second components.
void SplitTime(int64_t jd, int32_t *hour, int32_t *min, int32_t *sec, int32_t *fsec) {
  int64_t time = jd;

  *hour = time / kMicroSecondsPerHour;
  time -= (*hour) * kMicroSecondsPerHour;
  *min = time / kMicroSecondsPerMinute;
  time -= (*min) * kMicroSecondsPerMinute;
  *sec = time / kMicroSecondsPerSecond;
  *fsec = time - (*sec * kMicroSecondsPerSecond);
}

char *NumberToStringWithZeroPad(char *str, char *end, uint32_t value, uint32_t min_width) {
  // A table of all two-digit numbers. Used to speed up decimal digit generation.
  // clang-format off
  constexpr const char kDigitTable[] =
      "00" "01" "02" "03" "04" "05" "06" "07" "08" "09"
      "10" "11" "12" "13" "14" "15" "16" "17" "18" "19"
      "20" "21" "22" "23" "24" "25" "26" "27" "28" "29"
      "30" "31" "32" "33" "34" "35" "36" "37" "38" "39"
      "40" "41" "42" "43" "44" "45" "46" "47" "48" "49"
      "50" "51" "52" "53" "54" "55" "56" "57" "58" "59"
      "60" "61" "62" "63" "64" "65" "66" "67" "68" "69"
      "70" "71" "72" "73" "74" "75" "76" "77" "78" "79"
      "80" "81" "82" "83" "84" "85" "86" "87" "88" "89"
      "90" "91" "92" "93" "94" "95" "96" "97" "98" "99";
  // clang-format on

  // Fast path since we mostly expect 2-digit numbers for days/months.
  if (value < 100 && min_width == 2) {
    std::memcpy(str, kDigitTable + value * 2, 2);
    return str + 2;
  }

  const auto [ptr, error] = std::to_chars(str, end, value);
  TPL_ASSERT(error == std::errc(), "We expect the user to provide a sufficiently larger buffer!");

  const auto len = ptr - str;
  if (len >= min_width) {
    return str + len;
  }

  std::memmove(str + min_width - len, str, len);  // Data.
  std::memset(str, '0', min_width - len);         // Padding.
  return str + min_width;
}

// Encode date as local time.
void EncodeDateOnly(int32_t year, int32_t month, int32_t day, DateTimeFormat style, char *str,
                    char *end) {
  TPL_ASSERT(month >= 1 && month <= kMonthsPerYear, "Invalid month");

  switch (style) {
    case DateTimeFormat::ISO:
      str = NumberToStringWithZeroPad(str, end, year > 0 ? year : -(year - 1), 4);
      *str++ = '-';
      str = NumberToStringWithZeroPad(str, end, month, 2);
      *str++ = '-';
      str = NumberToStringWithZeroPad(str, end, day, 2);
      break;
    case DateTimeFormat::SQL:
    case DateTimeFormat::Postgres: {
      const auto sep = style == DateTimeFormat::SQL ? '/' : '-';
      if (kDateOrder == DateOrderFormat::DMY) {
        str = NumberToStringWithZeroPad(str, end, day, 2);
        *str++ = sep;
        str = NumberToStringWithZeroPad(str, end, month, 2);
      } else {
        str = NumberToStringWithZeroPad(str, end, month, 2);
        *str++ = sep;
        str = NumberToStringWithZeroPad(str, end, day, 2);
      }
      *str++ = sep;
      str = NumberToStringWithZeroPad(str, end, year > 0 ? year : -(year - 1), 4);
      break;
    }
  }

  if (year <= 0) {
    std::memcpy(str, " BC", 3);
    str += 3;
  }

  *str = '\0';
}

char *AppendTimestampSeconds(char *str, char *end, int32_t sec, int32_t fsec, int32_t precision) {
  // Seconds.
  str = NumberToStringWithZeroPad(str, end, std::abs(sec), 2);

  // Fractional seconds, if exists.
  if (fsec != 0) {
    *str++ = '.';

    uint32_t value = std::abs(fsec);
    auto seen_non_zero = false;
    auto last = &str[precision];

    // Append the fractional seconds part, but skip any tailing zeros.
    // Note: were building up this part in reverse.
    // e.g., 1001 -> .1001
    // e.g., 1900 -> .19
    for (; precision > 0; precision--) {
      const auto r = value % 10;
      value /= 10;

      if (r != 0) seen_non_zero = true;

      if (seen_non_zero) {
        str[precision] = '0' + r;
      } else {
        last = &str[precision];
      }
    }

    // If value remains, precision was too short. Print the whole thing.
    if (value != 0) {
      auto [p, ec] = std::to_chars(str, end, std::abs(fsec));
      TPL_ASSERT(ec == std::errc(), "Expected no error.");
      return p;
    }

    return last;
  }

  return str;
}

char *AppendTimestampSeconds(char *str, char *end, int32_t sec, int32_t fsec) {
  return AppendTimestampSeconds(str, end, sec, fsec, kMaxTimestampPrecision);
}

// Encode date and time interpreted as local time.
// Supported date styles:
//  Postgres - day mon hh:mm:ss yyyy tz
//  SQL - mm/dd/yyyy hh:mm:ss.ss tz
//  ISO - yyyy-mm-dd hh:mm:ss+/-tz
void EncodeDateTime(int32_t year, int32_t month, int32_t day, int32_t hour, int32_t min,
                    int32_t sec, int32_t fsec, DateTimeFormat style, char *str, char *end) {
  TPL_ASSERT(month >= 1 && month <= kMonthsPerYear, "Invalid month.");

  switch (style) {
    case DateTimeFormat::ISO:
      str = NumberToStringWithZeroPad(str, end, year > 0 ? year : -(year - 1), 4);
      *str++ = '-';
      str = NumberToStringWithZeroPad(str, end, month, 2);
      *str++ = '-';
      str = NumberToStringWithZeroPad(str, end, day, 2);
      *str++ = ' ';
      str = NumberToStringWithZeroPad(str, end, hour, 2);
      *str++ = ':';
      str = NumberToStringWithZeroPad(str, end, min, 2);
      *str++ = ':';
      str = AppendTimestampSeconds(str, end, sec, fsec);
      // TODO(pmenon): Time-zone.
      break;

    case DateTimeFormat::SQL:
      if (kDateOrder == DateOrderFormat::DMY) {
        str = NumberToStringWithZeroPad(str, end, day, 2);
        *str++ = '/';
        str = NumberToStringWithZeroPad(str, end, month, 2);
      } else {
        str = NumberToStringWithZeroPad(str, end, month, 2);
        *str++ = '/';
        str = NumberToStringWithZeroPad(str, end, day, 2);
      }
      *str++ = '/';
      str = NumberToStringWithZeroPad(str, end, year > 0 ? year : -(year - 1), 4);
      *str++ = ' ';
      str = NumberToStringWithZeroPad(str, end, hour, 2);
      *str++ = ':';
      str = NumberToStringWithZeroPad(str, end, min, 2);
      *str++ = ':';
      str = AppendTimestampSeconds(str, end, sec, fsec);
      // TODO(pmenon): Time-zone.
      break;

    case DateTimeFormat::Postgres: {
      const int32_t julian_date = BuildJulianDate(year, month, day);
      const int32_t day_of_week = JulianDateToDay(julian_date);
      std::memcpy(str, kDayNames[day_of_week], 3);
      str += 3;
      *str++ = ' ';
      if (kDateOrder == DateOrderFormat::DMY) {
        str = NumberToStringWithZeroPad(str, end, day, 2);
        *str++ = ' ';
        std::memcpy(str, kMonthNames[month - 1], 3);
        str += 3;
      } else {
        std::memcpy(str, kMonthNames[month - 1], 3);
        str += 3;
        *str++ = ' ';
        str = NumberToStringWithZeroPad(str, end, day, 2);
      }
      *str++ = ' ';
      str = NumberToStringWithZeroPad(str, end, hour, 2);
      *str++ = ':';
      str = NumberToStringWithZeroPad(str, end, min, 2);
      *str++ = ':';
      str = AppendTimestampSeconds(str, end, sec, fsec);
      *str++ = ' ';
      str = NumberToStringWithZeroPad(str, end, year > 0 ? year : -(year - 1), 4);
      // TODO(pmenon): Time-zone.
      break;
    }
  }

  if (year <= 0) {
    std::memcpy(str, " BC", 3);
    str += 3;
  }

  *str = '\0';
}

}  // namespace

//===----------------------------------------------------------------------===//
//
// Date
//
//===----------------------------------------------------------------------===//

std::string Date::ToString() const {
  int32_t year, month, day;
  SplitJulianDate(value_, &year, &month, &day);

  std::array<char, kMaxDateTimeLen + 1> buf;
  EncodeDateOnly(year, month, day, DateTimeFormat::ISO, buf.begin(), buf.end());
  return std::string(buf.data());
}

int32_t Date::ExtractYear() const {
  int32_t year, month, day;
  SplitJulianDate(value_, &year, &month, &day);
  return year;
}

int32_t Date::ExtractMonth() const {
  int32_t year, month, day;
  SplitJulianDate(value_, &year, &month, &day);
  return month;
}

int32_t Date::ExtractDay() const {
  int32_t year, month, day;
  SplitJulianDate(value_, &year, &month, &day);
  return day;
}

void Date::ExtractComponents(int32_t *year, int32_t *month, int32_t *day) const {
  SplitJulianDate(value_, year, month, day);
}

Date Date::FromString(const char *str, std::size_t len) {
  const char *ptr = str, *limit = ptr + len;

  // Trim leading and trailing whitespace
  while (ptr != limit && std::isspace(*ptr)) ptr++;
  while (ptr != limit && std::isspace(*(limit - 1))) limit--;

  uint32_t year = 0, month = 0, day = 0;

#define ERROR \
  throw ConversionException(fmt::format("{} is not a valid date", std::string(str, len)));

  // Year
  while (true) {
    if (ptr == limit) ERROR;
    char c = *ptr++;
    if (std::isdigit(c)) {
      year = year * 10 + (c - '0');
    } else if (c == '-') {
      break;
    } else {
      ERROR;
    }
  }

  // Month
  while (true) {
    if (ptr == limit) ERROR;
    char c = *ptr++;
    if (std::isdigit(c)) {
      month = month * 10 + (c - '0');
    } else if (c == '-') {
      break;
    } else {
      ERROR;
    }
  }

  // Day
  while (true) {
    if (ptr == limit) break;
    char c = *ptr++;
    if (std::isdigit(c)) {
      day = day * 10 + (c - '0');
    } else {
      ERROR;
    }
  }

  return Date::FromYMD(year, month, day);
}

Date Date::FromYMD(int32_t year, int32_t month, int32_t day) {
  // Check calendar date.
  if (!IsValidCalendarDate(year, month, day)) {
    throw ConversionException(fmt::format("{}-{}-{} is not a valid date", year, month, day));
  }

  // Check if date would overflow Julian calendar.
  if (!IsValidJulianDate(year, month, day)) {
    throw ConversionException(fmt::format("{}-{}-{} is not a valid date", year, month, day));
  }

  return Date(BuildJulianDate(year, month, day));
}

//===----------------------------------------------------------------------===//
//
// Timestamp
//
//===----------------------------------------------------------------------===//

int32_t Timestamp::ExtractYear() const {
  // Extract date component.
  int64_t date, time;
  StripTime(value_, &date, &time);

  // Extract year from date.
  int32_t year, month, day;
  SplitJulianDate(date, &year, &month, &day);
  return year;
}

int32_t Timestamp::ExtractMonth() const {
  // Extract date component.
  int64_t date, time;
  StripTime(value_, &date, &time);

  // Extract month from date.
  int32_t year, month, day;
  SplitJulianDate(date, &year, &month, &day);
  return month;
}

int32_t Timestamp::ExtractDay() const {
  // Extract date component.
  int64_t date, time;
  StripTime(value_, &date, &time);

  // Extract day from date.
  int32_t year, month, day;
  SplitJulianDate(date, &year, &month, &day);
  return day;
}

int32_t Timestamp::ExtractHour() const {
  // Extract date component.
  int64_t date, time;
  StripTime(value_, &date, &time);

  // Extract month from date.
  int32_t hour, min, sec, fsec;
  SplitTime(time, &hour, &min, &sec, &fsec);
  return hour;
}

int32_t Timestamp::ExtractMinute() const {
  // Extract date component.
  int64_t date, time;
  StripTime(value_, &date, &time);

  // Extract month from date.
  int32_t hour, min, sec, fsec;
  SplitTime(time, &hour, &min, &sec, &fsec);
  return min;
}

int32_t Timestamp::ExtractSecond() const {
  // Extract date component.
  int64_t date, time;
  StripTime(value_, &date, &time);

  // Extract month from date.
  int32_t hour, min, sec, fsec;
  SplitTime(time, &hour, &min, &sec, &fsec);
  return sec;
}

int32_t Timestamp::ExtractMillis() const {
  // Extract date component.
  int64_t date, time;
  StripTime(value_, &date, &time);

  // Extract month from date.
  int32_t hour, min, sec, fsec;
  SplitTime(time, &hour, &min, &sec, &fsec);
  return sec * 1000.0 + fsec / 1000.0;
}

int32_t Timestamp::ExtractMicros() const {
  // Extract date component.
  int64_t date, time;
  StripTime(value_, &date, &time);

  // Extract month from date.
  int32_t hour, min, sec, fsec;
  SplitTime(time, &hour, &min, &sec, &fsec);
  return sec + fsec / 1000000.0;
}

int32_t Timestamp::ExtractDayOfWeek() const {
  int64_t date, time;
  StripTime(value_, &date, &time);

  date += 1;
  date %= 7;
  if (date < 0) date += 7;
  return date;
}

int32_t Timestamp::ExtractDayOfYear() const {
  int64_t date, time;
  StripTime(value_, &date, &time);

  // Split date components.
  int32_t year, month, day;
  SplitJulianDate(date, &year, &month, &day);

  // Compute date of year.
  return BuildJulianDate(year, month, day) - BuildJulianDate(year, 1, 1) + 1;
}

Timestamp Timestamp::FromYMDHMS(int32_t year, int32_t month, int32_t day, int32_t hour, int32_t min,
                                int32_t sec) {
  // Check date component.
  if (!IsValidCalendarDate(year, month, day) || !IsValidJulianDate(year, month, day)) {
    throw ConversionException(fmt::format("date field {}-{}-{} out of range", year, month, day));
  }

  // Check time component.
  if (hour < 0 || min < 0 || min > kMinutesPerHour - 1 || sec < 0 || sec > kSecondsPerMinute ||
      hour > kHoursPerDay ||
      // Check for > 24:00:00.
      (hour == kHoursPerDay && (min > 0 || sec > 0))) {
    throw ConversionException(fmt::format("time field {}:{}:{} out of range", hour, min, sec));
  }

  const int64_t date = BuildJulianDate(year, month, day);
  const int64_t time = BuildTime(hour, min, sec);
  const int64_t result = date * kMicroSecondsPerDay + time;

  // Check for major overflow.
  if ((result - time) / kMicroSecondsPerDay != date) {
    throw ConversionException(fmt::format("timestamp out of range {}-{}-{} {}:{}:{} out of range",
                                          year, month, day, hour, min, sec));
  }

  // Loos good.
  return Timestamp(result);
}

std::string Timestamp::ToString() const {
  int64_t date, time;
  StripTime(value_, &date, &time);

  int32_t year, month, day;
  SplitJulianDate(date, &year, &month, &day);

  int32_t hour, min, sec, fsec;
  SplitTime(time, &hour, &min, &sec, &fsec);

  std::array<char, kMaxDateTimeLen + 1> buf;
  EncodeDateTime(year, month, day, hour, min, sec, std::lroundl(fsec), DateTimeFormat::ISO,
                 buf.begin(), buf.end());
  return std::string(buf.data());
}

Timestamp Timestamp::FromString(const char *str, std::size_t len) {
  throw NotImplementedException("Converting strings to timestamps not implemented.");
}

//===----------------------------------------------------------------------===//
//
// Varlen Entry
//
//===----------------------------------------------------------------------===//

hash_t VarlenEntry::Hash(hash_t seed) const {
  return util::HashUtil::HashXXH3(reinterpret_cast<const uint8_t *>(GetContent()), GetSize(), seed);
}

//===----------------------------------------------------------------------===//
//
// Blob
//
//===----------------------------------------------------------------------===//

hash_t Blob::Hash(hash_t seed) const {
  return util::HashUtil::HashXXH3(reinterpret_cast<const uint8_t *>(data_), size_, seed);
}

}  // namespace tpl::sql
