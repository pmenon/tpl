#include "sql/codegen/loop.h"

#include "sql/codegen/codegen.h"
#include "sql/codegen/function_builder.h"

namespace tpl::sql::codegen {

Loop::Loop(CodeGen *codegen, ast::Stmt *init, ast::Expr *condition, ast::Stmt *next)
    : codegen_(codegen),
      position_(codegen_->GetPosition()),
      prev_statements_(nullptr),
      init_(init),
      condition_(condition),
      next_(next),
      loop_body_(codegen_->MakeEmptyBlock()),
      completed_(false) {
  // The current function begin generated is stashed in the codegen instance.
  auto func = codegen_->CurrentFunction();
  TPL_ASSERT(func != nullptr, "No function is being generated!");

  // Stash the current block list and set our loop body as the new list.
  prev_statements_ = func->statements_;
  func->statements_ = loop_body_;
}

// Convert initial and next expressions to statements.
Loop::Loop(CodeGen *codegen, ast::Expr *init, ast::Expr *condition, ast::Expr *next)
    : Loop(codegen, codegen->MakeStmt(init), condition, codegen->MakeStmt(next)) {}

// Static cast to disambiguate constructor.
Loop::Loop(CodeGen *codegen, ast::Expr *condition)
    : Loop(codegen, static_cast<ast::Stmt *>(nullptr), condition, nullptr) {}

// Static cast to disambiguate constructor.
Loop::Loop(CodeGen *codegen) : Loop(codegen, static_cast<ast::Stmt *>(nullptr), nullptr, nullptr) {}

Loop::~Loop() { EndLoop(); }

void Loop::EndLoop() {
  if (completed_) {
    return;
  }

  TPL_ASSERT(codegen_->CurrentFunction() != nullptr, "Not within a function!");
  auto func = codegen_->CurrentFunction();
  func->statements_ = prev_statements_;

  // Create and append the if statement.
  auto loop = codegen_->GetFactory()->NewForStmt(position_, init_, condition_, next_, loop_body_);
  func->Append(loop);

  // Done.
  completed_ = true;
}

}  // namespace tpl::sql::codegen
