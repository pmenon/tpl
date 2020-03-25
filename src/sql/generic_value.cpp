#include "sql/generic_value.h"

#include <sql/generic_value.h>
#include <string>

#include "spdlog/fmt/fmt.h"

#include "common/exception.h"
#include "common/macros.h"
#include "sql/constant_vector.h"
#include "sql/value.h"
#include "sql/vector_operations/vector_operators.h"
#include "util/math_util.h"

namespace tpl::sql {

bool GenericValue::Equals(const GenericValue &other) const {
  if (type_id_ != other.type_id_) {
    return false;
  }
  if (is_null_ != other.is_null_) {
    return false;
  }
  if (is_null_ && other.is_null_) {
    return true;
  }
  switch (type_id_) {
    case TypeId::Boolean:
      return value_.boolean == other.value_.boolean;
    case TypeId::TinyInt:
      return value_.tinyint == other.value_.tinyint;
    case TypeId::SmallInt:
      return value_.smallint == other.value_.smallint;
    case TypeId::Integer:
      return value_.integer == other.value_.integer;
    case TypeId::BigInt:
      return value_.bigint == other.value_.bigint;
    case TypeId::Hash:
      return value_.hash == other.value_.hash;
    case TypeId::Pointer:
      return value_.pointer == other.value_.pointer;
    case TypeId::Float:
      return util::MathUtil::ApproxEqual(value_.float_, other.value_.float_);
    case TypeId::Double:
      return util::MathUtil::ApproxEqual(value_.double_, other.value_.double_);
    case TypeId::Date:
      return value_.date_ == other.value_.date_;
    case TypeId::Varchar:
      return str_value_ == other.str_value_;
    default:
      throw NotImplementedException(
          fmt::format("Equality of '{}' generic value is unsupported", TypeIdToString(type_id_)));
  }
  return false;
}

GenericValue GenericValue::CastTo(TypeId type) {
  // Copy if same type
  if (type_id_ == type) {
    return GenericValue(*this);
  }
  // Use vector to cast
  ConstantVector result(*this);
  result.Cast(type);
  return result.GetValue(0);
}

std::string GenericValue::ToString() const {
  if (is_null_) {
    return "NULL";
  }
  switch (type_id_) {
    case TypeId::Boolean:
      return value_.boolean ? "True" : "False";
    case TypeId::TinyInt:
      return std::to_string(value_.tinyint);
    case TypeId::SmallInt:
      return std::to_string(value_.smallint);
    case TypeId::Integer:
      return std::to_string(value_.integer);
    case TypeId::BigInt:
      return std::to_string(value_.bigint);
    case TypeId::Hash:
      return std::to_string(value_.hash);
    case TypeId::Pointer:
      return std::to_string(value_.pointer);
    case TypeId::Float:
      return std::to_string(value_.float_);
    case TypeId::Double:
      return std::to_string(value_.double_);
    case TypeId::Date:
      return value_.date_.ToString();
    case TypeId::Timestamp:
      return value_.timestamp_.ToString();
    case TypeId::Varchar:
      return "'" + str_value_ + "'";
    default:
      throw NotImplementedException(fmt::format(
          "string-ification of '{}' generic value is unsupported", TypeIdToString(type_id_)));
  }
}

std::ostream &operator<<(std::ostream &out, const GenericValue &val) {
  out << val.ToString();
  return out;
}

GenericValue GenericValue::CreateNull(TypeId type_id) {
  GenericValue result(type_id);
  result.is_null_ = true;
  return result;
}

GenericValue GenericValue::CreateBoolean(const bool value) {
  GenericValue result(TypeId::Boolean);
  result.value_.boolean = value;
  result.is_null_ = false;
  return result;
}

GenericValue GenericValue::CreateTinyInt(const int8_t value) {
  GenericValue result(TypeId::TinyInt);
  result.value_.tinyint = value;
  result.is_null_ = false;
  return result;
}

GenericValue GenericValue::CreateSmallInt(const int16_t value) {
  GenericValue result(TypeId::SmallInt);
  result.value_.smallint = value;
  result.is_null_ = false;
  return result;
}

GenericValue GenericValue::CreateInteger(const int32_t value) {
  GenericValue result(TypeId::Integer);
  result.value_.integer = value;
  result.is_null_ = false;
  return result;
}

GenericValue GenericValue::CreateBigInt(const int64_t value) {
  GenericValue result(TypeId::BigInt);
  result.value_.bigint = value;
  result.is_null_ = false;
  return result;
}
GenericValue GenericValue::CreateHash(hash_t value) {
  GenericValue result(TypeId::Hash);
  result.value_.hash = value;
  result.is_null_ = false;
  return result;
}

GenericValue GenericValue::CreatePointer(uintptr_t value) {
  GenericValue result(TypeId::Pointer);
  result.value_.pointer = value;
  result.is_null_ = false;
  return result;
}

GenericValue GenericValue::CreateReal(const float value) {
  GenericValue result(TypeId::Float);
  result.value_.float_ = value;
  result.is_null_ = false;
  return result;
}

GenericValue GenericValue::CreateDouble(const double value) {
  GenericValue result(TypeId::Double);
  result.value_.double_ = value;
  result.is_null_ = false;
  return result;
}

GenericValue GenericValue::CreateDate(Date date) {
  GenericValue result(TypeId::Date);
  result.value_.date_ = date;
  result.is_null_ = false;
  return result;
}

GenericValue GenericValue::CreateDate(uint32_t year, uint32_t month, uint32_t day) {
  return CreateDate(Date::FromYMD(year, month, day));
}

GenericValue GenericValue::CreateTimestamp(Timestamp timestamp) {
  GenericValue result(TypeId::Timestamp);
  result.value_.timestamp_ = timestamp;
  result.is_null_ = false;
  return result;
}

GenericValue GenericValue::CreateTimestamp(int32_t year, int32_t month, int32_t day, int32_t hour,
                                           int32_t min, int32_t sec) {
  return CreateTimestamp(Timestamp::FromYMDHMS(year, month, day, hour, min, sec));
}

GenericValue GenericValue::CreateVarchar(std::string_view str) {
  GenericValue result(TypeId::Varchar);
  result.is_null_ = false;
  result.str_value_ = str;
  return result;
}

GenericValue GenericValue::CreateFromRuntimeValue(const TypeId type_id, const Val &val) {
  switch (type_id) {
    case TypeId::Boolean:
      return GenericValue::CreateBoolean(static_cast<const BoolVal &>(val).val);
    case TypeId::TinyInt:
      return GenericValue::CreateTinyInt(static_cast<const Integer &>(val).val);
    case TypeId::SmallInt:
      return GenericValue::CreateSmallInt(static_cast<const Integer &>(val).val);
    case TypeId::Integer:
      return GenericValue::CreateInteger(static_cast<const Integer &>(val).val);
    case TypeId::BigInt:
      return GenericValue::CreateBigInt(static_cast<const Integer &>(val).val);
    case TypeId::Float:
      return GenericValue::CreateFloat(static_cast<const Real &>(val).val);
    case TypeId::Double:
      return GenericValue::CreateDouble(static_cast<const Real &>(val).val);
    case TypeId::Date:
      return GenericValue::CreateDate(static_cast<const DateVal &>(val).val);
    case TypeId::Timestamp:
      return GenericValue::CreateTimestamp(static_cast<const TimestampVal &>(val).val);
    case TypeId::Varchar:
      return GenericValue::CreateVarchar(static_cast<const StringVal &>(val).val.GetStringView());
    default:
      throw NotImplementedException(fmt::format(
          "run-time value of type '{}' not supported as generic value", TypeIdToString(type_id)));
  }
}

}  // namespace tpl::sql
