#include "sql/codegen/function_builder.h"

#include "ast/ast_node_factory.h"
#include "sql/codegen/codegen.h"

namespace tpl::sql::codegen {

FunctionBuilder::FunctionBuilder(CodeGen *codegen, ast::Identifier name,
                                 util::RegionVector<ast::FieldDeclaration *> &&params,
                                 ast::Expr *ret_type)
    : codegen_(codegen),
      prev_function_(nullptr),
      name_(name),
      params_(std::move(params)),
      ret_type_(ret_type),
      start_(codegen->GetPosition()),
      statements_(codegen->MakeEmptyBlock()),
      decl_(nullptr) {
  // Stash the previously active function so we can restore it upon completion.
  prev_function_ = codegen_->function_;
  // Swap in ourselves in as the current active function.
  codegen_->function_ = this;
}

FunctionBuilder::~FunctionBuilder() { Finish(); }

ast::Expr *FunctionBuilder::GetParameterByPosition(uint32_t param_idx) {
  if (param_idx < params_.size()) {
    return codegen_->MakeExpr(params_[param_idx]->GetName());
  }
  return nullptr;
}

void FunctionBuilder::Append(ast::Statement *stmt) {
  // Append the statement to the block.
  statements_->AppendStatement(stmt);
  // Bump line number.
  codegen_->NewLine();
}

void FunctionBuilder::Append(ast::Expr *expr) {
  Append(codegen_->NodeFactory()->NewExpressionStatement(expr));
}

void FunctionBuilder::Append(ast::VariableDeclaration *decl) {
  Append(codegen_->NodeFactory()->NewDeclStatement(decl));
}

ast::FunctionDeclaration *FunctionBuilder::Finish(ast::Expr *ret) {
  if (decl_ != nullptr) {
    return decl_;
  }

  TPL_ASSERT(
      ret == nullptr || statements_->IsEmpty() || !statements_->GetLast()->IsReturnStatement(),
      "Double-return at end of function. You should either call FunctionBuilder::Finish() "
      "with an explicit return expression, or use the factory to manually append a return "
      "statement and call FunctionBuilder::Finish() with a null return.");

  // Add the return.
  if (!statements_->IsEmpty() && !statements_->GetLast()->IsReturnStatement()) {
    Append(codegen_->NodeFactory()->NewReturnStatement(codegen_->GetPosition(), ret));
  }

  // Finalize everything.
  statements_->SetRightBracePosition(codegen_->GetPosition());

  // Build the function's type.
  auto func_type = codegen_->NodeFactory()->NewFunctionType(start_, std::move(params_), ret_type_);

  // Create the declaration.
  auto func_lit = codegen_->NodeFactory()->NewFunctionLitExpr(func_type, statements_);
  decl_ = codegen_->NodeFactory()->NewFunctionDeclaration(start_, name_, func_lit);

  // Register the function in the container.
  codegen_->container_->RegisterFunction(decl_);

  // Restore the previous function in the codegen instance.
  codegen_->function_ = prev_function_;

  // Next line.
  codegen_->NewLine();

  // Done
  return decl_;
}

}  // namespace tpl::sql::codegen
