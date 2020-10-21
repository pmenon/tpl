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

codegen::CompactStorage::CompactStorage(CodeGen *codegen, std::string_view name)
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
  std::ranges::sort(reordered, [&](auto left_idx, auto right_idx) {
    return GetTypeIdSize(schema[left_idx]) > GetTypeIdSize(schema[right_idx]);
  });

  // Generate the compact struct.
  util::RegionVector<ast::FieldDecl *> members = codegen_->MakeEmptyFieldList();
  members.reserve(schema.size() + 1);
  for (uint32_t i = 0; i < schema.size(); i++) {
    ast::Identifier name = codegen_->MakeIdentifier(fmt::format("_m{}", i));
    members.push_back(codegen_->MakeField(name, codegen_->PrimitiveTplType(schema[reordered[i]])));
  }
  // Tack on the NULL indicators.
  const uint32_t null_bytes = util::MathUtil::DivRoundUp(schema.size(), 8);
  members.push_back(
      codegen_->MakeField(nulls_, codegen_->ArrayType(null_bytes, ast::BuiltinType::Uint8)));

  // Fill out the column information. We can only do this after all fields have
  // been added since we rely on the field names for access.
  for (uint32_t i = 0; i < schema.size(); i++) {
    col_info_.emplace_back(schema[i], members[reordered[i]]->Name());
  }

  // Build the final type.
  codegen_->DeclareStruct(type_name_, std::move(members));
}

ast::Expr *CompactStorage::Nulls(ast::Expr *ptr) const {
  return codegen_->AddressOf(codegen_->AccessStructMember(ptr, nulls_));
}

ast::Expr *CompactStorage::ColumnPtr(ast::Expr *ptr, uint32_t index) const {
  return codegen_->AddressOf(codegen_->AccessStructMember(ptr, col_info_[index].second));
}

void CompactStorage::WriteSQL(ast::Expr *ptr, uint32_t index, ast::Expr *val) const {
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
  ast::Expr *col_ptr = ColumnPtr(ptr, index);
  ast::Expr *nulls = Nulls(ptr);
  function->Append(codegen_->CallBuiltin(op, {col_ptr, nulls, codegen_->Const32(index), val}));
}

ast::Expr *CompactStorage::ReadSQL(ast::Expr *ptr, uint32_t index) const {
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

}  // namespace tpl::sql::codegen
