#include "sql/codegen/state_descriptor.h"

#include <utility>

#include "ast/ast.h"
#include "sql/codegen/codegen.h"
#include "sql/codegen/function_builder.h"

namespace tpl::sql::codegen {

StateDescriptor::StateDescriptor(ast::Identifier name, StateDescriptor::InstanceProvider access)
    : name_(name), access_(std::move(access)), state_type_(nullptr) {}

StateDescriptor::Slot StateDescriptor::DeclareStateEntry(CodeGen *codegen, const std::string &name,
                                                         ast::Expression *type_repr) {
  TPL_ASSERT(state_type_ == nullptr, "Cannot add to state after it's been finalized");
  ast::Identifier member = codegen->MakeFreshIdentifier(name);
  slots_.emplace_back(member, type_repr);
  return slots_.size() - 1;
}

ast::StructDeclaration *StateDescriptor::ConstructFinalType(CodeGen *codegen) {
  // Early exit if the state is already constructed.
  if (state_type_ != nullptr) {
    return state_type_;
  }

  // Collect fields and build the structure type.
  util::RegionVector<ast::FieldDeclaration *> fields = codegen->MakeEmptyFieldList();
  fields.reserve(slots_.size());
  for (auto &slot : slots_) {
    fields.push_back(codegen->MakeField(slot.name, slot.type_repr));
  }
  state_type_ = codegen->DeclareStruct(name_, std::move(fields));

  // Done
  return state_type_;
}

ast::Expression *StateDescriptor::GetStatePointer(CodeGen *codegen) const {
  TPL_ASSERT(access_ != nullptr, "No instance accessor provided");
  return access_(codegen);
}

ast::Expression *StateDescriptor::GetStateEntry(CodeGen *codegen,
                                                StateDescriptor::Slot slot) const {
  TPL_ASSERT(slot < slots_.size(), "Invalid slot");
  return codegen->AccessStructMember(GetStatePointer(codegen), slots_[slot].name);
}

ast::Expression *StateDescriptor::GetStateEntryPtr(CodeGen *codegen,
                                                   StateDescriptor::Slot slot) const {
  TPL_ASSERT(slot < slots_.size(), "Invalid slot");
  return codegen->AddressOf(GetStateEntry(codegen, slot));
}

ast::Expression *StateDescriptor::GetStateEntryOffset(CodeGen *codegen,
                                                      StateDescriptor::Slot slot) const {
  TPL_ASSERT(slot < slots_.size(), "Invalid slot");
  return codegen->OffsetOf(state_type_->GetName(), slots_[slot].name);
}

std::size_t StateDescriptor::GetSize() const {
  TPL_ASSERT(state_type_ != nullptr, "State has not been constructed");
  TPL_ASSERT(state_type_->GetTypeRepr()->GetType() != nullptr, "Type-checking not completed!");
  return state_type_->GetTypeRepr()->GetType()->GetSize();
}

}  // namespace tpl::sql::codegen
