#include "sql/codegen/state_descriptor.h"

#include <utility>

namespace tpl::sql::codegen {

StateDescriptor::StateDescriptor(CodeGen *codegen, std::string_view name, InstanceProvider access)
    : struct_(codegen, name, false), access_(std::move(access)) {}

StateDescriptor::Slot StateDescriptor::DeclareStateEntry(std::string_view name, ast::Type *type) {
  return Slot{struct_.AddMember(name, type)};
}

void StateDescriptor::ConstructFinalType() { struct_.Seal(); }

edsl::ValueVT StateDescriptor::GetStatePtr(CodeGen *codegen) const {
  TPL_ASSERT(access_ != nullptr, "No instance accessor provided");
  return access_(codegen);
}

edsl::ReferenceVT StateDescriptor::GetStateEntryGeneric(CodeGen *codegen, Slot slot) const {
  return struct_.MemberGeneric(GetStatePtr(codegen), slot);
}

edsl::ValueVT StateDescriptor::GetStateEntryPtrGeneric(CodeGen *codegen, Slot slot) const {
  TPL_ASSERT(slot < slots_.size(), "Invalid slot");
  return struct_.MemberPtrGeneric(GetStatePtr(codegen), slot);
}

edsl::Value<uint32_t> StateDescriptor::GetStateEntryOffset(CodeGen *codegen, Slot slot) const {
  return struct_.OffsetOf(slot);
}

std::size_t StateDescriptor::GetSizeRaw() const { return struct_.GetSizeRaw(); }

}  // namespace tpl::sql::codegen
