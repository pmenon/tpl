#include "sql/codegen/compact_storage.h"

#include "spdlog/fmt/fmt.h"

#include "ast/ast.h"
#include "sql/codegen/codegen.h"
#include "sql/codegen/function_builder.h"

namespace tpl::sql::codegen {

#define CODE_LIST(F)                                                              \
  F(TypeId::Boolean, CompactStorageWriteBool, CompactStorageReadBool)             \
  F(TypeId::TinyInt, CompactStorageWriteTinyInt, CompactStorageReadTinyInt)       \
  F(TypeId::SmallInt, CompactStorageWriteSmallInt, CompactStorageReadSmallInt)    \
  F(TypeId::Integer, CompactStorageWriteInteger, CompactStorageReadInteger)       \
  F(TypeId::BigInt, CompactStorageWriteBigInt, CompactStorageReadBigInt)          \
  F(TypeId::Float, CompactStorageWriteReal, CompactStorageReadReal)               \
  F(TypeId::Double, CompactStorageWriteDouble, CompactStorageReadDouble)          \
  F(TypeId::Date, CompactStorageWriteDate, CompactStorageReadDate)                \
  F(TypeId::Timestamp, CompactStorageWriteTimestamp, CompactStorageReadTimestamp) \
  F(TypeId::Varchar, CompactStorageWriteString, CompactStorageReadString)

CompactStorage::CompactStorage(CodeGen *codegen, std::string_view name)
    : codegen_(codegen), struct_(codegen, fmt::format("{}_Compact", name), true) {}

CompactStorage::CompactStorage(CodeGen *codegen, std::string_view name,
                               const std::vector<TypeId> &schema)
    : CompactStorage(codegen, name) {
  Setup(schema);
}

void CompactStorage::Setup(const std::vector<TypeId> &schema) {
  // Set the types.
  col_types_ = schema;

  // Add the fields as described in the schema.
  for (uint32_t i = 0; i < schema.size(); i++) {
    const auto name = fmt::format("member{}", i);
    const auto type = codegen_->GetPrimitiveTPLType(schema[i]);
    struct_.AddMember(name, type);
  }

  // Tack on the NULL indicators for all fields as a bitmap byte array.
  const auto num_null_bytes = util::MathUtil::DivRoundUp(schema.size(), 8);
  const auto null_arr_type = codegen_->ArrayType(num_null_bytes, codegen_->GetType<uint8_t>());
  struct_.AddMember("nulls", null_arr_type);

  // Seal.
  struct_.Seal();
}

void CompactStorage::WriteSQL(const edsl::ReferenceVT &ptr, uint32_t index,
                              const edsl::ValueVT &val) const {
  TPL_ASSERT(ptr.GetType()->IsPointerType() != nullptr, "Buffer must be a pointer!");
  TPL_ASSERT(index < col_types_.size(), "Out-of-bounds index access.");

  FunctionBuilder *function = codegen_->GetCurrentFunction();

  ast::Builtin op;

#define GEN_CASE(Type, WriteCall, ReadCall) \
  case (Type):                              \
    op = ast::Builtin::WriteCall;           \
    break;
  switch (col_types_[index]) {
    CODE_LIST(GEN_CASE)
    default:
      UNREACHABLE("Impossible type in CompactStorage::Write() call!");
  }
#undef GEN_CASE

  // TODO(pmenon): Fix this ...
  auto col_ptr = struct_.MemberPtrGeneric(ptr, index);
  auto nulls = struct_.MemberPtr<uint8_t[]>(ptr, col_types_.size());
  auto call = codegen_->CallBuiltin(
      op, {col_ptr.GetRaw(), nulls.GetRaw(), codegen_->Literal<uint32_t>(index), val.GetRaw()});
  function->Append(edsl::Value<void>(codegen_->MakeStatement(call)));
}

edsl::ValueVT CompactStorage::ReadSQL(const edsl::ReferenceVT &ptr, uint32_t index) const {
  TPL_ASSERT(ptr != nullptr, "Buffer pointer cannot be null.");
  TPL_ASSERT(index < col_types_.size(), "Out-of-bounds index access.");

  ast::Builtin op;

#define GEN_CASE(Type, WriteCall, ReadCall) \
  case (Type):                              \
    op = ast::Builtin::ReadCall;            \
    break;
  switch (col_types_[index]) {
    CODE_LIST(GEN_CASE)
    default:
      UNREACHABLE("Impossible type in CompactStorage::Read() call!");
  }
#undef GEN_CASE

  // Call.
  auto col_ptr = struct_.MemberPtrGeneric(ptr, index);
  auto nulls = struct_.MemberPtr<uint8_t[]>(ptr, col_types_.size());
  auto val = codegen_->CallBuiltin(
      op, {col_ptr.GetRaw(), nulls.GetRaw(), codegen_->Literal<uint32_t>(index)});
  val->SetType(codegen_->GetTPLType(col_types_[index]));
  return edsl::ValueVT(codegen_, val);
}

void CompactStorage::WritePrimitive(const edsl::ReferenceVT &ptr, uint32_t index,
                                    const edsl::ValueVT &val) const {
  auto col_ref = struct_.MemberGeneric(ptr, index);
  ptr.GetCodeGen()->GetCurrentFunction()->Append(edsl::Assign(col_ref, val));
}

edsl::ValueVT CompactStorage::ReadPrimitive(const edsl::ReferenceVT &ptr, uint32_t index) const {
  return struct_.MemberGeneric(ptr, index);
}

}  // namespace tpl::sql::codegen
