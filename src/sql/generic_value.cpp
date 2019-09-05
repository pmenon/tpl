#include "sql/generic_value.h"

#include <iostream>
#include <string>

#include "sql/constant_vector.h"
#include "sql/value.h"
#include "sql/vector.h"
#include "sql/vector_operations/vector_operators.h"
#include "util/macros.h"
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
    case TypeId::Varchar:
      return str_value_ == other.str_value_;
    default:
      TPL_ASSERT(false, "Not allowed");
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
    case TypeId::Varchar:
      return "'" + str_value_ + "'";
    default:
      UNREACHABLE("Impossible type");
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

GenericValue GenericValue::CreateTinyInt(const i8 value) {
  GenericValue result(TypeId::TinyInt);
  result.value_.tinyint = value;
  result.is_null_ = false;
  return result;
}

GenericValue GenericValue::CreateSmallInt(const i16 value) {
  GenericValue result(TypeId::SmallInt);
  result.value_.smallint = value;
  result.is_null_ = false;
  return result;
}

GenericValue GenericValue::CreateInteger(const i32 value) {
  GenericValue result(TypeId::Integer);
  result.value_.integer = value;
  result.is_null_ = false;
  return result;
}

GenericValue GenericValue::CreateBigInt(const i64 value) {
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

GenericValue GenericValue::CreateReal(const f32 value) {
  GenericValue result(TypeId::Float);
  result.value_.float_ = value;
  result.is_null_ = false;
  return result;
}

GenericValue GenericValue::CreateDouble(const f64 value) {
  GenericValue result(TypeId::Double);
  result.value_.double_ = value;
  result.is_null_ = false;
  return result;
}

GenericValue GenericValue::CreateDate(UNUSED i32 year, UNUSED i32 month, UNUSED i32 day) {
  throw std::runtime_error("Creating Date generic value not supported!");
}

GenericValue GenericValue::CreateTimestamp(UNUSED i32 year, UNUSED i32 month, UNUSED i32 day,
                                           UNUSED i32 hour, UNUSED i32 min, UNUSED i32 sec,
                                           UNUSED i32 msec) {
  throw std::runtime_error("Creating Timestamp generic value not supported!");
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
    case TypeId::Varchar:
      return GenericValue::CreateVarchar(static_cast<const StringVal &>(val).ptr);
    default:
      throw std::runtime_error("Type " + std::string(TypeIdToString(type_id)) +
                               " not supported as runtime value");
  }
}

}  // namespace tpl::sql
