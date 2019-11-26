#include "sql/runtime_types.h"

#include <string>

#include "spdlog/fmt/fmt.h"

#include "common/exception.h"

namespace tpl::sql {

namespace {

// The below Julian date conversions are taken from Postgres.

bool IsValidJulianDate(uint32_t year, uint32_t month, uint32_t day) {
  return year <= 9999 && month >= 1 && month <= 12 && day >= 1 && day <= 31;
}

uint32_t BuildJulianDate(uint32_t year, uint32_t month, uint32_t day) {
  uint32_t a = (14 - month) / 12;
  uint32_t y = year + 4800 - a;
  uint32_t m = month + (12 * a) - 3;

  return day + ((153 * m + 2) / 5) + (365 * y) + (y / 4) - (y / 100) + (y / 400) - 32045;
}

void SplitJulianDate(uint32_t julian_date, uint32_t *year, uint32_t *month, uint32_t *day) {
  uint32_t a = julian_date + 32044;
  uint32_t b = (4 * a + 3) / 146097;
  uint32_t c = a - ((146097 * b) / 4);
  uint32_t d = (4 * c + 3) / 1461;
  uint32_t e = c - ((1461 * d) / 4);
  uint32_t m = (5 * e + 2) / 153;

  *day = e - ((153 * m + 2) / 5) + 1;
  *month = m + 3 - (12 * (m / 10));
  *year = (100 * b) + d - 4800 + (m / 10);
}

}  // namespace

//===----------------------------------------------------------------------===//
//
// Date
//
//===----------------------------------------------------------------------===//

bool Date::IsValid() const noexcept {
  uint32_t year, month, day;
  SplitJulianDate(value_, &year, &month, &day);
  return IsValidJulianDate(year, month, day);
}

std::string Date::ToString() const {
  uint32_t year, month, day;
  SplitJulianDate(value_, &year, &month, &day);
  return fmt::format("{}-{:02}-{:02}", year, month, day);
}

uint32_t Date::ExtractYear() const noexcept {
  uint32_t year, month, day;
  SplitJulianDate(value_, &year, &month, &day);
  return year;
}

uint32_t Date::ExtractMonth() const noexcept {
  uint32_t year, month, day;
  SplitJulianDate(value_, &year, &month, &day);
  return month;
}

uint32_t Date::ExtractDay() const noexcept {
  uint32_t year, month, day;
  SplitJulianDate(value_, &year, &month, &day);
  return day;
}

void Date::ExtractComponents(uint32_t *year, uint32_t *month, uint32_t *day) {
  SplitJulianDate(value_, year, month, day);
}

Date Date::FromString(const char *str, std::size_t len) {
  const char *ptr = str, *limit = ptr + len;

  // Trim leading and trailing whitespace
  while (ptr != limit && std::isspace(*ptr)) ptr++;
  while (ptr != limit && std::isspace(*(limit - 1))) limit--;

  uint32_t year = 0, month = 0, day = 0;

#define ERROR throw ConversionException("{} is not a valid date", std::string(str, len));

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

  if (!IsValidJulianDate(year, month, day)) ERROR;

#undef ERROR

  return Date(BuildJulianDate(year, month, day));
}

Date Date::FromYMD(uint32_t year, uint32_t month, uint32_t day) {
  if (!IsValidJulianDate(year, month, day)) {
    throw ConversionException("{}-{}-{} is not a valid date", year, month, day);
  }

  return Date(BuildJulianDate(year, month, day));
}

bool Date::IsValidDate(uint32_t year, uint32_t month, uint32_t day) {
  return IsValidJulianDate(year, month, day);
}

//===----------------------------------------------------------------------===//
//
// Varlen
//
//===----------------------------------------------------------------------===//

hash_t VarlenEntry::Hash(const hash_t seed) const noexcept {
  if (GetSize() < GetInlineThreshold()) {
    return util::HashUtil::HashCrc(reinterpret_cast<const uint8_t *>(GetContent()), GetSize(),
                                   seed);
  } else {
    return util::HashUtil::HashXX3(reinterpret_cast<const uint8_t *>(GetContent()), GetSize(),
                                   seed);
  }
}

//===----------------------------------------------------------------------===//
//
// Blob
//
//===----------------------------------------------------------------------===//

hash_t Blob::Hash(hash_t seed) const noexcept {
  return util::HashUtil::HashXX3(reinterpret_cast<const uint8_t *>(data_), size_, seed);
}

}  // namespace tpl::sql
