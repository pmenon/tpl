#include "sql/functions/date_time_functions.h"

namespace tpl::sql {

TimestampVal DateTimeFunctions::ConvertDateToTimestamp(DateVal *date) noexcept {
  return date->is_null ? TimestampVal::Null() : TimestampVal(date->val.ConvertToTimestamp());
}

Integer DateTimeFunctions::Century(const TimestampVal &time) noexcept {
  if (time.is_null) {
    return Integer::Null();
  }
  const auto year = time.val.ExtractYear();
  if (year > 0) {
    return Integer((year + 99) / 100);
  } else {
    return Integer(-((99 - (year - 1)) / 100));
  }
}

Integer DateTimeFunctions::Decade(const TimestampVal &time) noexcept {
  if (time.is_null) {
    return Integer::Null();
  }
  const auto year = time.val.ExtractYear();
  if (year >= 0) {
    return Integer(year / 10);
  } else {
    return Integer(-((8 - (year - 1)) / 10));
  }
}

Integer DateTimeFunctions::Year(const TimestampVal &time) noexcept {
  if (time.is_null) {
    return Integer::Null();
  }
  return Integer(time.val.ExtractYear());
}

Integer DateTimeFunctions::Quarter(const TimestampVal &time) noexcept {
  if (time.is_null) {
    return Integer::Null();
  }
  const auto month = time.val.ExtractMonth();
  return Integer((month - 1) / 3 + 1);
}

Integer DateTimeFunctions::Month(const TimestampVal &time) noexcept {
  if (time.is_null) {
    return Integer::Null();
  }
  return Integer(time.val.ExtractMonth());
}

Integer DateTimeFunctions::Day(const TimestampVal &time) noexcept {
  if (time.is_null) {
    return Integer::Null();
  }
  return Integer(time.val.ExtractDay());
}

Integer DateTimeFunctions::DayOfWeek(const TimestampVal &time) noexcept {
  if (time.is_null) {
    return Integer::Null();
  }
  return Integer(time.val.ExtractDayOfWeek());
}

Integer DateTimeFunctions::DayOfYear(const TimestampVal &time) noexcept {
  if (time.is_null) {
    return Integer::Null();
  }
  return Integer(time.val.ExtractDayOfYear());
}

Integer DateTimeFunctions::Hour(const TimestampVal &time) noexcept {
  if (time.is_null) {
    return Integer::Null();
  }
  return Integer(time.val.ExtractHour());
}

Integer DateTimeFunctions::Minute(const TimestampVal &time) noexcept {
  if (time.is_null) {
    return Integer::Null();
  }
  return Integer(time.val.ExtractMinute());
}

Integer DateTimeFunctions::Second(const TimestampVal &time) noexcept {
  if (time.is_null) {
    return Integer::Null();
  }
  return Integer(time.val.ExtractSecond());
}

Integer DateTimeFunctions::Millisecond(const TimestampVal &time) noexcept {
  if (time.is_null) {
    return Integer::Null();
  }
  return Integer(time.val.ExtractMillis());
}

Integer DateTimeFunctions::Microseconds(const TimestampVal &time) noexcept {
  if (time.is_null) {
    return Integer::Null();
  }
  return Integer(time.val.ExtractMicros());
}

}  // namespace tpl::sql
