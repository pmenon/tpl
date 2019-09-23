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
    case TypeId::Varchar:
      return sizeof(VarlenEntry);
    case TypeId::Varbinary:
      return sizeof(Blob);
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

}  // namespace tpl::sql
