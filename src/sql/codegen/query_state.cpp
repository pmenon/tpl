#include "sql/codegen/query_state.h"

#include "sql/codegen/codegen.h"
#include "sql/codegen/function_builder.h"

namespace tpl::sql::codegen {

const QueryState::DefaultStateAccess QueryState::kDefaultStateAccess{};

ast::Expr *QueryState::DefaultStateAccess::GetStatePtr(CodeGen *codegen) const {
  return codegen->CurrentFunction()->GetParameterByPosition(0);
}

QueryState::QueryState() : QueryState(kDefaultStateAccess) {}

QueryState::QueryState(const QueryState::StateAccess &access)
    : access_(access), state_type_(nullptr) {}

QueryState::Slot QueryState::DeclareStateEntry(CodeGen *codegen, const std::string &name,
                                               ast::Expr *type_repr) {
  TPL_ASSERT(state_type_ == nullptr, "Cannot add to state after it's been finalized");
  slots_.emplace_back(codegen->MakeFreshIdentifier(name), type_repr);
  return slots_.size() - 1;
}

ast::StructDecl *QueryState::ConstructFinalType(CodeGen *codegen, ast::Identifier name) {
  // Early exit if the state is already constructed.
  if (IsSealed()) {
    return state_type_;
  }

  // Collect fields and build the structure type.
  util::RegionVector<ast::FieldDecl *> fields = codegen->MakeEmptyFieldList();
  for (auto &slot : slots_) {
    fields.push_back(codegen->MakeField(slot.name, slot.type_repr));
  }
  state_type_ = codegen->DeclareStruct(name, std::move(fields));

  // Done
  return state_type_;
}

ast::Expr *QueryState::GetStateEntry(CodeGen *codegen, const QueryState::Slot slot) const {
  return codegen->AccessStructMember(GetStatePointer(codegen), slots_[slot].name);
}

ast::Expr *QueryState::GetStateEntryPtr(CodeGen *codegen, const QueryState::Slot slot) const {
  return codegen->AddressOf(GetStateEntry(codegen, slot));
}

std::size_t QueryState::GetSize() const {
  TPL_ASSERT(state_type_ != nullptr, "State has not been constructed");
  return state_type_->TypeRepr()->GetType()->GetSize();
}

}  // namespace tpl::sql::codegen
