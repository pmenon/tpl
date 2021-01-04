#include "sql/codegen/execution_state.h"

#include <utility>

namespace tpl::sql::codegen {

ExecutionState::ExecutionState(CodeGen *codegen, std::string_view name, InstanceProvider access)
    : struct_(codegen, name, false), access_(std::move(access)) {}

ExecutionState::RTSlot ExecutionState::DeclareStateEntry(std::string_view name, ast::Type *type) {
  return RTSlot{struct_.AddMember(name, type)};
}

void ExecutionState::ConstructFinalType() { struct_.Seal(); }

edsl::ValueVT ExecutionState::GetStatePtr(CodeGen *codegen) const {
  TPL_ASSERT(access_ != nullptr, "No instance accessor provided");
  return access_(codegen);
}

edsl::ReferenceVT ExecutionState::GetStateEntryGeneric(CodeGen *codegen, RTSlot slot) const {
  return struct_.GetMember(GetStatePtr(codegen), slot);
}

edsl::ValueVT ExecutionState::GetStateEntryPtrGeneric(CodeGen *codegen, RTSlot slot) const {
  TPL_ASSERT(slot < slots_.size(), "Invalid slot");
  return struct_.GetMemberPtr(GetStatePtr(codegen), slot);
}

edsl::Value<uint32_t> ExecutionState::GetStateEntryOffset(CodeGen *codegen, RTSlot slot) const {
  return struct_.OffsetOf(slot);
}

std::size_t ExecutionState::GetSizeRaw() const { return struct_.GetSizeRaw(); }

}  // namespace tpl::sql::codegen
