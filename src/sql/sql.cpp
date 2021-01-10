#include "sql/sql.h"

#include <string>

#include "common/exception.h"

namespace tpl::sql {

// static
SqlTypeId GetSqlTypeFromInternalType(TypeId type) {
  switch (type) {
    case TypeId::Boolean:
      return SqlTypeId::Boolean;
    case TypeId::TinyInt:
      return SqlTypeId::TinyInt;
    case TypeId::SmallInt:
      return SqlTypeId::SmallInt;
    case TypeId::Integer:
      return SqlTypeId::Integer;
    case TypeId::BigInt:
      return SqlTypeId::BigInt;
    case TypeId::Float:
      return SqlTypeId::Real;
    case TypeId::Double:
      return SqlTypeId::Double;
    case TypeId::Date:
      return SqlTypeId::Date;
    case TypeId::Varchar:
      return SqlTypeId::Varchar;
    case TypeId::Varbinary:
      return SqlTypeId::Varchar;
    default:
      throw InvalidTypeException(type, "type is not a SQL type");
  }
}

// static
std::size_t GetTypeIdSize(TypeId type) {
  switch (type) {
    case TypeId::Boolean:
      return sizeof(bool);
    case TypeId::TinyInt:
      return sizeof(int8_t);
    case TypeId::SmallInt:
      return sizeof(int16_t);
    case TypeId::Integer:
      return sizeof(int32_t);
    case TypeId::BigInt:
      return sizeof(int64_t);
    case TypeId::Hash:
      return sizeof(hash_t);
    case TypeId::Pointer:
      return sizeof(uintptr_t);
    case TypeId::Float:
      return sizeof(float);
    case TypeId::Double:
      return sizeof(double);
    case TypeId::Date:
      return sizeof(Date);
    case TypeId::Timestamp:
      return sizeof(Timestamp);
    case TypeId::Varchar:
      return sizeof(VarlenEntry);
    case TypeId::Varbinary:
      return sizeof(Blob);
    default:
      // All cases handled
      UNREACHABLE("Impossible type");
  }
}

std::size_t GetTypeIdAlignment(TypeId type) {
  switch (type) {
    case TypeId::Boolean:
      return alignof(bool);
    case TypeId::TinyInt:
      return alignof(int8_t);
    case TypeId::SmallInt:
      return alignof(int16_t);
    case TypeId::Integer:
      return alignof(int32_t);
    case TypeId::BigInt:
      return alignof(int64_t);
    case TypeId::Hash:
      return alignof(hash_t);
    case TypeId::Pointer:
      return alignof(uintptr_t);
    case TypeId::Float:
      return alignof(float);
    case TypeId::Double:
      return alignof(double);
    case TypeId::Date:
      return alignof(Date);
    case TypeId::Varchar:
      return alignof(VarlenEntry);
    case TypeId::Varbinary:
      return alignof(Blob);
    default:
      // All cases handled
      UNREACHABLE("Impossible type");
  }
}

// static
bool IsTypeFixedSize(TypeId type) {
  switch (type) {
    case TypeId::Boolean:
    case TypeId::TinyInt:
    case TypeId::SmallInt:
    case TypeId::Integer:
    case TypeId::BigInt:
    case TypeId::Hash:
    case TypeId::Pointer:
    case TypeId::Float:
    case TypeId::Double:
    case TypeId::Date:
      return true;
    case TypeId::Varchar:
    case TypeId::Varbinary:
      return false;
    default:
      UNREACHABLE("Impossible type");
  }
}

bool IsTypeIntegral(TypeId type) {
  switch (type) {
    case TypeId::TinyInt:
    case TypeId::SmallInt:
    case TypeId::Integer:
    case TypeId::BigInt:
      return true;
    case TypeId::Boolean:
    case TypeId::Hash:
    case TypeId::Pointer:
    case TypeId::Float:
    case TypeId::Double:
    case TypeId::Date:
    case TypeId::Varchar:
    case TypeId::Varbinary:
      return false;
    default:
      UNREACHABLE("Impossible type");
  }
}

bool IsTypeFloatingPoint(TypeId type) {
  switch (type) {
    case TypeId::Float:
    case TypeId::Double:
      return true;
    case TypeId::Boolean:
    case TypeId::TinyInt:
    case TypeId::SmallInt:
    case TypeId::Integer:
    case TypeId::BigInt:
    case TypeId::Hash:
    case TypeId::Pointer:
    case TypeId::Date:
    case TypeId::Varchar:
    case TypeId::Varbinary:
      return false;
    default:
      UNREACHABLE("Impossible type");
  }
}

// static
bool IsTypeNumeric(TypeId type) {
  switch (type) {
    case TypeId::Boolean:
    case TypeId::TinyInt:
    case TypeId::SmallInt:
    case TypeId::Integer:
    case TypeId::BigInt:
    case TypeId::Hash:
    case TypeId::Pointer:
    case TypeId::Float:
    case TypeId::Double:
    case TypeId::Date:
      return true;
    case TypeId::Varchar:
    case TypeId::Varbinary:
      return false;
    default:
      // All cases handled
      UNREACHABLE("Impossible type");
  }
}

// static
std::string TypeIdToString(TypeId type) {
  switch (type) {
    case TypeId::Boolean:
      return "Boolean";
    case TypeId::TinyInt:
      return "TinyInt";
    case TypeId::SmallInt:
      return "SmallInt";
    case TypeId::Integer:
      return "Integer";
    case TypeId::BigInt:
      return "BigInt";
    case TypeId::Hash:
      return "Hash";
    case TypeId::Pointer:
      return "Pointer";
    case TypeId::Float:
      return "Float";
    case TypeId::Double:
      return "Double";
    case TypeId::Date:
      return "Date";
    case TypeId::Varchar:
      return "VarChar";
    case TypeId::Varbinary:
      return "VarBinary";
    default:
      // All cases handled
      UNREACHABLE("Impossible type");
  }
}

std::string KnownOperatorToString(KnownOperator op, bool short_str) {
  // clang-format off
  switch (op) {
    case KnownOperator::Neg:         return "Negate";
    case KnownOperator::Add:         return short_str ? "+" : "Add";
    case KnownOperator::Sub:         return short_str ? "-" : "Subtract";
    case KnownOperator::Mul:         return short_str ? "*" : "Multiply";
    case KnownOperator::Div:         return short_str ? "/" : "Divide";
    case KnownOperator::Rem:         return short_str ? "%" : "Modulo";
    case KnownOperator::BitAnd:      return short_str ? "&" : "Bit And";
    case KnownOperator::BitOr:       return short_str ? "|" : "Bit Or";
    case KnownOperator::BitShl:      return short_str ? "<<" : "Bit Shift Left";
    case KnownOperator::BitShr:      return short_str ? ">>" : "Bit Shift Right";
    case KnownOperator::IsNull:      return "Is NULL";
    case KnownOperator::IsNotNull:   return "Is Not NULL";
    case KnownOperator::LogicalAnd:  return "Logical And";
    case KnownOperator::LogicalOr:   return "Logical Or";
    case KnownOperator::LogicalNot:  return "Logical Not";
    case KnownOperator::Like:        return "Like";
    case KnownOperator::NotLike:     return "NotLike";
    case KnownOperator::ExtractYear: return "Extract Year";
    case KnownOperator::ACos:        return "ACos";
    case KnownOperator::ASin:        return "ASin";
    case KnownOperator::ATan:        return "ATan";
    case KnownOperator::ATan2:       return "ATan2";
    case KnownOperator::Cos:         return "Cos";
    case KnownOperator::Cot:         return "Cot";
    case KnownOperator::Tan:         return "Tan";
    case KnownOperator::Sin:         return "Sin";
  }
  // clang-format on
  UNREACHABLE("Impossible known operator. All cases handled above.");
}

}  // namespace tpl::sql
