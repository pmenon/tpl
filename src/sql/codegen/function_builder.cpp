#include "sql/codegen/function_builder.h"

#include "ast/ast_node_factory.h"
#include "sql/codegen/codegen.h"

namespace tpl::sql::codegen {

FunctionBuilder::FunctionBuilder(CodeGen *codegen, ast::Identifier name,
                                 std::vector<Param> &&params, ast::Type *ret_type)
    : codegen_(codegen),
      prev_function_(nullptr),
      name_(name),
      ret_type_(ret_type),
      start_(codegen->GetPosition()),
      statements_(codegen->MakeEmptyBlock()),
      decl_(nullptr) {
  // Populate the parameters.
  params_.reserve(params.size());
  for (const auto &[p_name, p_type] : params) {
    params_.emplace_back(codegen, p_name, p_type);
  }

  // Swap in this function as the "active" function in the code-gen instance.
  prev_function_ = codegen_->function_;
  codegen_->function_ = this;
}

FunctionBuilder::~FunctionBuilder() { Finish(); }

edsl::ValueVT FunctionBuilder::GetParameterByPosition(const uint32_t param_idx) const {
  TPL_ASSERT(param_idx < params_.size(), "Out-of-bounds parameter access.");
  return params_[param_idx];
}

void FunctionBuilder::Append(const edsl::Value<void> &stmt) {
  if (stmt.GetNode() == nullptr) return;
  statements_->AppendStatement(stmt.GetNode());
  codegen_->NewLine();
}

ast::FunctionDeclaration *FunctionBuilder::Finish() {
  if (decl_ != nullptr) return decl_;

  statements_->SetRightBracePosition(codegen_->GetPosition());

  // Build the function's type by building the type representations of the
  // parameters and return type.
  util::RegionVector<ast::FieldDeclaration *> param_decls(codegen_->Context()->GetRegion());
  param_decls.reserve(params_.size());
  for (const auto &p : params_) {
    auto type = codegen_->BuildTypeRepresentation(p.GetType(), false /* not for struct */);
    param_decls.push_back(codegen_->NodeFactory()->NewFieldDeclaration(start_, p.GetName(), type));
  }
  auto ret = codegen_->BuildTypeRepresentation(ret_type_, false /* not for struct */);
  auto func_type = codegen_->NodeFactory()->NewFunctionType(start_, std::move(param_decls), ret);
  auto func_lit = codegen_->NodeFactory()->NewFunctionLiteralExpression(func_type, statements_);
  decl_ = codegen_->NodeFactory()->NewFunctionDeclaration(start_, name_, func_lit);

  codegen_->container_->RegisterFunction(decl_);

  codegen_->function_ = prev_function_;

  codegen_->NewLine();

  return decl_;
}

}  // namespace tpl::sql::codegen
