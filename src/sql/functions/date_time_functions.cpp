#include "sql/functions/date_time_functions.h"

#include "sql/functions/helpers.h"
#include "sql/operators/datetime_operators.h"

namespace tpl::sql {

void DateTimeFunctions::Century(Integer *result, const TimestampVal &time) noexcept {
  UnaryFunction::EvalHideNull(result, time, tpl::sql::Century{});
}

void DateTimeFunctions::Decade(Integer *result, const TimestampVal &time) noexcept {
  UnaryFunction::EvalHideNull(result, time, tpl::sql::Decade{});
}

void DateTimeFunctions::Quarter(Integer *result, const TimestampVal &time) noexcept {
  UnaryFunction::EvalHideNull(result, time, tpl::sql::Quarter{});
}

void DateTimeFunctions::Day(Integer *result, const TimestampVal &time) noexcept {
  UnaryFunction::EvalHideNull(result, time, [](auto t) { return t.ExtractDay(); });
}

void DateTimeFunctions::DayOfWeek(Integer *result, const TimestampVal &time) noexcept {
  UnaryFunction::EvalHideNull(result, time, [](auto t) { return t.ExtractDayOfWeek(); });
}

void DateTimeFunctions::DayOfYear(Integer *result, const TimestampVal &time) noexcept {
  UnaryFunction::EvalHideNull(result, time, [](auto t) { return t.ExtractDayOfYear(); });
}

void DateTimeFunctions::Hour(Integer *result, const TimestampVal &time) noexcept {
  UnaryFunction::EvalHideNull(result, time, [](auto t) { return t.ExtractHour(); });
}

void DateTimeFunctions::Minute(Integer *result, const TimestampVal &time) noexcept {
  UnaryFunction::EvalHideNull(result, time, [](auto t) { return t.ExtractMinute(); });
}

void DateTimeFunctions::Second(Integer *result, const TimestampVal &time) noexcept {
  UnaryFunction::EvalHideNull(result, time, [](auto t) { return t.ExtractSecond(); });
}

void DateTimeFunctions::Millisecond(Integer *result, const TimestampVal &time) noexcept {
  UnaryFunction::EvalHideNull(result, time, [](auto t) { return t.ExtractMillis(); });
}

void DateTimeFunctions::Microseconds(Integer *result, const TimestampVal &time) noexcept {
  UnaryFunction::EvalHideNull(result, time, [](auto t) { return t.ExtractMicros(); });
}

}  // namespace tpl::sql
