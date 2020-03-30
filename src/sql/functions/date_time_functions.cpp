#include "sql/functions/date_time_functions.h"

namespace tpl::sql {

void DateTimeFunctions::Century(Integer *result, const TimestampVal &time) noexcept {
  if (time.is_null) {
    *result = Integer::Null();
    return;
  }

  const auto year = time.val.ExtractYear();
  if (year > 0) {
    *result = Integer((year + 99) / 100);
  } else {
    *result = Integer(-((99 - (year - 1)) / 100));
  }
}

void DateTimeFunctions::Decade(Integer *result, const TimestampVal &time) noexcept {
  if (time.is_null) {
    *result = Integer::Null();
    return;
  }

  const auto year = time.val.ExtractYear();
  if (year >= 0) {
    *result = Integer(year / 10);
  } else {
    *result = Integer(-((8 - (year - 1)) / 10));
  }
}

void DateTimeFunctions::Year(Integer *result, const TimestampVal &time) noexcept {
  if (time.is_null) {
    *result = Integer::Null();
    return;
  }
  *result = Integer(time.val.ExtractYear());
}

void DateTimeFunctions::Quarter(Integer *result, const TimestampVal &time) noexcept {
  if (time.is_null) {
    *result = Integer::Null();
    return;
  }
  const auto month = time.val.ExtractMonth();
  *result = Integer((month - 1) / 3 + 1);
}

void DateTimeFunctions::Month(Integer *result, const TimestampVal &time) noexcept {
  if (time.is_null) {
    *result = Integer::Null();
    return;
  }
  *result = Integer(time.val.ExtractMonth());
}

void DateTimeFunctions::Day(Integer *result, const TimestampVal &time) noexcept {
  if (time.is_null) {
    *result = Integer::Null();
    return;
  }
  *result = Integer(time.val.ExtractDay());
}

void DateTimeFunctions::DayOfWeek(Integer *result, const TimestampVal &time) noexcept {
  if (time.is_null) {
    *result = Integer::Null();
    return;
  }
  *result = Integer(time.val.ExtractDayOfWeek());
}

void DateTimeFunctions::DayOfYear(Integer *result, const TimestampVal &time) noexcept {
  if (time.is_null) {
    *result = Integer::Null();
    return;
  }
  *result = Integer(time.val.ExtractDayOfYear());
}

void DateTimeFunctions::Hour(Integer *result, const TimestampVal &time) noexcept {
  if (time.is_null) {
    *result = Integer::Null();
    return;
  }
  *result = Integer(time.val.ExtractHour());
}

void DateTimeFunctions::Minute(Integer *result, const TimestampVal &time) noexcept {
  if (time.is_null) {
    *result = Integer::Null();
    return;
  }
  *result = Integer(time.val.ExtractMinute());
}

void DateTimeFunctions::Second(Integer *result, const TimestampVal &time) noexcept {
  if (time.is_null) {
    *result = Integer::Null();
    return;
  }
  *result = Integer(time.val.ExtractSecond());
}

void DateTimeFunctions::Millisecond(Integer *result, const TimestampVal &time) noexcept {
  if (time.is_null) {
    *result = Integer::Null();
    return;
  }
  *result = Integer(time.val.ExtractMillis());
}

void DateTimeFunctions::Microseconds(Integer *result, const TimestampVal &time) noexcept {
  if (time.is_null) {
    *result = Integer::Null();
    return;
  }
  *result = Integer(time.val.ExtractMicros());
}

}  // namespace tpl::sql
