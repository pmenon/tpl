#include "sql/codegen/edsl/struct.h"

#include <algorithm>

#include "common/macros.h"

namespace tpl::sql::codegen::edsl {

Struct::Struct(CodeGen *codegen, std::string_view name, bool optimize_layout)
    : codegen_(codegen),
      name_(codegen->MakeFreshIdentifier(name)),
      type_(nullptr),
      ptr_to_type_(nullptr),
      optimize_layout_(optimize_layout) {}

Struct::RTSlot Struct::AddMember(std::string_view name, ast::Type *type) {
  TPL_ASSERT(!IsSealed(), "Cannot add member to struct after it's been sealed.");
  TPL_ASSERT(std::ranges::none_of(members_, [](const auto &m) { return name == m.name; }),
             "Duplicate member name provided.");
  members_.emplace_back(codegen_->MakeFreshIdentifier(name), type);
  return RTSlot{static_cast<uint32_t>(members_.size()) - 1};
}

void Struct::Seal() {
  std::vector<ast::Field> optimized_members = members_;
  if (optimize_layout_) {
    std::ranges::sort(optimized_members, [](const auto &l, const auto &r) {
      return l.type->GetSize() > r.type->GetSize();
    });
  }
  type_ = codegen_->DeclareStruct(name_, optimized_members);
  ptr_to_type_ = type_->PointerTo();
}

ReferenceVT Struct::MemberGeneric(const ValueVT &ptr, RTSlot slot) const {
  TPL_ASSERT(IsSealed(), "Can only access struct after it's been sealed!");
  TPL_ASSERT(slot < members_.size(), "Out-of-bounds member_id access.");
  TPL_ASSERT(ptr.GetType() == type_->PointerTo(),
             "Provided pointer doesn't point to type of this structure.");
  const auto member_name = members_[slot].name;
  return ReferenceVT(codegen_, codegen_->StructMember(ptr.GetRaw(), member_name.GetView()));
}

ValueVT Struct::MemberPtrGeneric(const ValueVT &ptr, RTSlot slot) const {
  TPL_ASSERT(IsSealed(), "Can only access struct after it's been sealed!");
  TPL_ASSERT(slot < members_.size(), "Out-of-bounds member access.");
  TPL_ASSERT(ptr.GetType() == type_->PointerTo(),
             "Provided pointer doesn't point to type of this structure.");
  return MemberGeneric(ptr, slot).Addr();
}

std::size_t Struct::GetSizeRaw() const noexcept { return type_->GetSize(); }

Value<uint32_t> Struct::GetSize() const { return Literal<uint32_t>(codegen_, GetSizeRaw()); }

std::size_t Struct::OffsetOfRaw(Struct::RTSlot slot) const {
  TPL_ASSERT(IsSealed(), "Can only access struct after it's been sealed!");
  TPL_ASSERT(slot < members_.size(), "Out-of-bounds member access.");
  return type_->GetOffsetOfFieldByName(members_[slot].name);
}

Value<uint32_t> Struct::OffsetOf(Struct::RTSlot slot) const {
  return Literal<uint32_t>(codegen_, OffsetOfRaw(slot));
}

}  // namespace tpl::sql::codegen::edsl
