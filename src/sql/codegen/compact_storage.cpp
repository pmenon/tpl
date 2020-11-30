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
    : codegen_(codegen),
      type_name_(codegen->MakeFreshIdentifier(fmt::format("{}_Compact", name))),
      nulls_(codegen_->MakeIdentifier("nulls")) {}

CompactStorage::CompactStorage(CodeGen *codegen, std::string_view name,
                               const std::vector<TypeId> &schema)
    : CompactStorage(codegen, name) {
  Setup(schema);
}

void CompactStorage::Setup(const std::vector<TypeId> &schema) {
  std::vector<uint32_t> reordered(schema.size()), reordered_offsets(schema.size());
  std::iota(reordered.begin(), reordered.end(), 0u);

  // Re-order attributes by decreasing size to minimize padding.
  std::ranges::stable_sort(reordered, [&](auto left_idx, auto right_idx) {
    return GetTypeIdSize(schema[left_idx]) > GetTypeIdSize(schema[right_idx]);
  });

  col_info_.resize(schema.size());

  // Generate the compact struct.
  util::RegionVector<ast::FieldDeclaration *> members = codegen_->MakeEmptyFieldList();
  members.resize(schema.size());
  for (uint32_t i = 0; i < schema.size(); i++) {
    ast::Identifier name = codegen_->MakeIdentifier(fmt::format("member{}", i));
    members[i] = codegen_->MakeField(name, codegen_->PrimitiveTplType(schema[reordered[i]]));
    col_info_[reordered[i]] = std::make_pair(schema[reordered[i]], name);
  }
  // Tack on the NULL indicators for all fields as a bitmap byte array.
  const auto null_byte = ast::BuiltinType::UInt8;
  const auto num_null_bytes = util::MathUtil::DivRoundUp(schema.size(), 8);
  members.push_back(codegen_->MakeField(nulls_, codegen_->ArrayType(num_null_bytes, null_byte)));

  // Build the final type.
  codegen_->DeclareStruct(type_name_, std::move(members));
}

ast::Expression *CompactStorage::Nulls(ast::Expression *ptr) const {
  return codegen_->AddressOf(codegen_->AccessStructMember(ptr, nulls_));
}

ast::Expression *CompactStorage::ColumnPtr(ast::Expression *ptr, uint32_t index) const {
  return codegen_->AddressOf(codegen_->AccessStructMember(ptr, col_info_[index].second));
}

void CompactStorage::WriteSQL(ast::Expression *ptr, uint32_t index, ast::Expression *val) const {
  TPL_ASSERT(ptr != nullptr, "Buffer pointer cannot be null.");
  TPL_ASSERT(val != nullptr, "Input value cannot be null.");
  TPL_ASSERT(index < col_info_.size(), "Out-of-bounds index access.");

  FunctionBuilder *function = codegen_->GetCurrentFunction();

  ast::Builtin op;

  // clang-format off
#define GEN_CASE(Type, WriteCall, ReadCall) case (Type): op = ast::Builtin::WriteCall; break;
  // clang-format on
  switch (col_info_[index].first) {
    CODE_LIST(GEN_CASE)
    default:
      UNREACHABLE("Impossible type in CompactStorage::Write() call!");
  }
#undef GEN_CASE

  // Call.
  ast::Expression *col_ptr = ColumnPtr(ptr, index);
  ast::Expression *nulls = Nulls(ptr);
  function->Append(codegen_->CallBuiltin(op, {col_ptr, nulls, codegen_->Const32(index), val}));
}

ast::Expression *CompactStorage::ReadSQL(ast::Expression *ptr, uint32_t index) const {
  TPL_ASSERT(ptr != nullptr, "Buffer pointer cannot be null.");
  TPL_ASSERT(index < col_info_.size(), "Out-of-bounds index access.");

  ast::Builtin op;

  // clang-format off
#define GEN_CASE(Type, WriteCall, ReadCall) case (Type): op = ast::Builtin::ReadCall; break;
  // clang-format on
  switch (col_info_[index].first) {
    CODE_LIST(GEN_CASE)
    default:
      UNREACHABLE("Impossible type in CompactStorage::Read() call!");
  }
#undef GEN_CASE

  // Call.
  return codegen_->CallBuiltin(op, {ColumnPtr(ptr, index), Nulls(ptr), codegen_->Const32(index)});
}

ast::Identifier CompactStorage::FieldNameAtIndex(uint32_t index) const {
  TPL_ASSERT(index < col_info_.size(), "Out-of-bounds field access.");
  return col_info_[index].second;
}

}  // namespace tpl::sql::codegen
