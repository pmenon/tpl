#include "sql/codegen/state_descriptor.h"

#include "sql/codegen/codegen.h"
#include "sql/codegen/function_builder.h"

namespace tpl::sql::codegen {

StateDescriptor::StateDescriptor(ast::Identifier name, StateDescriptor::StateAccess *access)
    : name_(name), access_(access), state_type_(nullptr) {}

StateDescriptor::Slot StateDescriptor::DeclareStateEntry(CodeGen *codegen, const std::string &name,
                                                         ast::Expr *type_repr) {
  TPL_ASSERT(state_type_ == nullptr, "Cannot add to state after it's been finalized");
  slots_.emplace_back(codegen->MakeFreshIdentifier(name), type_repr);
  return slots_.size() - 1;
}

ast::StructDecl *StateDescriptor::ConstructFinalType(CodeGen *codegen) {
  // Early exit if the state is already constructed.
  if (state_type_ != nullptr) {
    return state_type_;
  }

  // Collect fields and build the structure type.
  util::RegionVector<ast::FieldDecl *> fields = codegen->MakeEmptyFieldList();
  for (auto &slot : slots_) {
    fields.push_back(codegen->MakeField(slot.name, slot.type_repr));
  }
  state_type_ = codegen->DeclareStruct(name_, std::move(fields));

  // Done
  return state_type_;
}

ast::Expr *StateDescriptor::GetStateEntry(CodeGen *codegen,
                                          const StateDescriptor::Slot slot) const {
  return codegen->AccessStructMember(GetStatePointer(codegen), slots_[slot].name);
}

ast::Expr *StateDescriptor::GetStateEntryPtr(CodeGen *codegen,
                                             const StateDescriptor::Slot slot) const {
  return codegen->AddressOf(GetStateEntry(codegen, slot));
}

ast::Expr *StateDescriptor::GetStateEntryOffset(CodeGen *codegen,
                                                StateDescriptor::Slot slot) const {
  return codegen->OffsetOf(name_, slots_[slot].name);
}

std::size_t StateDescriptor::GetSize() const {
  TPL_ASSERT(state_type_ != nullptr, "State has not been constructed");
  TPL_ASSERT(state_type_->TypeRepr()->GetType() != nullptr, "Type-checking not completed!");
  return state_type_->TypeRepr()->GetType()->GetSize();
}

}  // namespace tpl::sql::codegen
