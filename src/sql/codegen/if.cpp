#include "sql/codegen/if.h"

#include "sql/codegen/codegen.h"
#include "sql/codegen/function_builder.h"

namespace tpl::sql::codegen {

If::If(CodeGen *codegen, ast::Expr *condition)
    : codegen_(codegen),
      position_(codegen_->GetPosition()),
      prev_func_stmt_list_(nullptr),
      condition_(condition),
      then_stmts_(codegen_->MakeEmptyBlock()),
      else_stmts_(nullptr),
      completed_(false) {
  TPL_ASSERT(codegen_->CurrentFunction() != nullptr, "Not within a function!");
  auto func = codegen_->CurrentFunction();
  prev_func_stmt_list_ = func->statements_;
  func->statements_ = then_stmts_;
}

If::~If() { EndIf(); }

void If::Else() {
  else_stmts_ = codegen_->MakeEmptyBlock();
  codegen_->CurrentFunction()->statements_ = else_stmts_;
}

void If::EndIf() {
  if (completed_) {
    return;
  }

  TPL_ASSERT(codegen_->CurrentFunction() != nullptr, "Not within a function!");
  auto func = codegen_->CurrentFunction();
  func->statements_ = prev_func_stmt_list_;

  // Create and append the if statement.
  auto if_stmt = codegen_->GetFactory()->NewIfStmt(position_, condition_, then_stmts_, else_stmts_);
  func->Append(if_stmt);

  // Done.
  completed_ = true;
}

}  // namespace tpl::sql::codegen
