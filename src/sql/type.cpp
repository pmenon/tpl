#include "sql/type.h"

#include "spdlog/fmt/fmt.h"

namespace tpl::sql {

Type::Type(SqlTypeId type, bool nullable) : type_id_(type), nullable_(nullable) {}

Type::Type(SqlTypeId type, bool nullable, Type::VarcharInfo varchar_info) : Type(type, nullable) {
  varchar_info_ = varchar_info;
}

Type::Type(SqlTypeId type, bool nullable, Type::CharInfo char_info) : Type(type, nullable) {
  char_info_ = char_info;
}

Type::Type(SqlTypeId type, bool nullable, Type::NumericInfo numeric_info) : Type(type, nullable) {
  numeric_info_ = numeric_info;
}

bool Type::operator==(const Type &that) const {
  if (type_id_ != that.GetTypeId()) {
    return false;
  }
  if (nullable_ != that.nullable_) {
    return false;
  }
  switch (type_id_) {
    case SqlTypeId::Varchar:
      return varchar_info_ == that.varchar_info_;
    case SqlTypeId::Char:
      return char_info_ == that.char_info_;
    case SqlTypeId::Decimal:
      return numeric_info_ == that.numeric_info_;
    default:
      return true;
  }
}

std::string Type::ToStringWithoutNullability() const {
  switch (type_id_) {
    case SqlTypeId::Boolean:
      return "bool";
    case SqlTypeId::TinyInt:
      return "tinyint";
    case SqlTypeId::SmallInt:
      return "smallint";
    case SqlTypeId::Integer:
      return "integer";
    case SqlTypeId::BigInt:
      return "bigint";
    case SqlTypeId::Real:
      return "real";
    case SqlTypeId::Double:
      return "double";
    case SqlTypeId::Decimal:
      return fmt::format("decimal[{},{}]", numeric_info_.precision, numeric_info_.scale);
    case SqlTypeId::Date:
      return "date";
    case SqlTypeId::Timestamp:
      return "timestamp";
    case SqlTypeId::Char:
      return fmt::format("char[{}]", char_info_.len);
    case SqlTypeId::Varchar:
      return fmt::format("varchar[{}]", varchar_info_.max_len);
    default:
      UNREACHABLE("Impossible type.");
  }
}

std::string Type::ToString() const {
  return ToStringWithoutNullability() + (nullable_ ? "(nullable)" : "(not nullable)");
}

uint32_t Type::GetMaxStringLength() const {
  switch (type_id_) {
    case SqlTypeId::Varchar:
      return varchar_info_.max_len;
    case SqlTypeId::Char:
      return char_info_.len;
    default:
      return 0;
  }
}

TypeId Type::GetPrimitiveTypeId() const {
  switch (type_id_) {
    case SqlTypeId::Boolean:
      return TypeId::Boolean;
    case SqlTypeId::TinyInt:
      return TypeId::TinyInt;
    case SqlTypeId::SmallInt:
      return TypeId::SmallInt;
    case SqlTypeId::Integer:
      return TypeId::Integer;
    case SqlTypeId::BigInt:
      return TypeId::BigInt;
    case SqlTypeId::Real:
      return TypeId::Float;
    case SqlTypeId::Double:
      return TypeId::Double;
    case SqlTypeId::Decimal:
      return TypeId::Double;
    case SqlTypeId::Date:
      return TypeId::Date;
    case SqlTypeId::Timestamp:
      return TypeId::Timestamp;
    case SqlTypeId::Char:
    case SqlTypeId::Varchar:
      return TypeId::Varchar;
    default:
      UNREACHABLE("Impossible SQL type.");
  }
}

}  // namespace tpl::sql
