//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// type.cpp
//
// Identification: src/codegen/type/type.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "compiler/type/type.h"

#include "compiler/type/sql_type.h"

namespace tpl {
namespace compiler {
namespace type {

Type::Type(tpl::type::TypeId type_id, bool _nullable)
    : type_id(type_id), nullable(_nullable) {
  aux_info.varlen = 0;
  aux_info.numeric_info.precision = 0;
  aux_info.numeric_info.scale = 0;
}

Type::Type() : Type(type::TypeId::INVALID, false) {}

Type::Type(const SqlType &sql_type, bool _nullable)
    : Type(sql_type.TypeId(), _nullable) {}

bool Type::operator==(const Type &other) const {
  // TODO(pmenon): This isn't correct; we need to check all other fields ...
  return type_id == other.type_id;
}

const SqlType &Type::GetSqlType() const { return SqlType::LookupType(type_id); }

const TypeSystem &Type::GetTypeSystem() const {
  return GetSqlType().GetTypeSystem();
}

Type Type::AsNullable() const {
  if (nullable) {
    return *this;
  }

  Type copy = *this;
  copy.nullable = true;
  return copy;
}

Type Type::AsNonNullable() const {
  if (!nullable) {
    return *this;
  }

  Type copy = *this;
  copy.nullable = false;
  return copy;
}

}  // namespace type
}  // namespace codegen
}  // namespace peloton
