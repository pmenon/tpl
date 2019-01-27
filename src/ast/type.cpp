#include "ast/type.h"

#include "runtime/hashmap.h"

namespace tpl::ast {

bool Type::IsArithmetic() const {
  if (IsIntegerType() || IsFloatType()) {
    return true;
  }
  if (auto *type = SafeAs<SqlType>()) {
    return type->sql_type().IsArithmetic();
  }
  return false;
}

FunctionType::FunctionType(util::RegionVector<Field> &&params, Type *ret)
    : Type(ret->context(), sizeof(void *), alignof(void *),
           Type::Kind::FunctionType),
      params_(std::move(params)),
      ret_(ret) {}

MapType::MapType(Type *key_type, Type *val_type)
    : Type(key_type->context(), sizeof(runtime::HashMap),
           alignof(runtime::HashMap), Kind::MapType),
      key_type_(key_type),
      val_type_(val_type) {}

StructType::StructType(AstContext &ctx, u32 size, u32 alignment,
                       util::RegionVector<Field> &&fields,
                       util::RegionVector<u32> &&field_offsets)
    : Type(ctx, size, alignment, Type::Kind::StructType),
      fields_(std::move(fields)),
      field_offsets_(std::move(field_offsets)) {}

}  // namespace tpl::ast