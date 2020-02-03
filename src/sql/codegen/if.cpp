#include "sql/codegen/if.h"

#include "sql/codegen/codegen.h"
#include "sql/codegen/function_builder.h"

namespace tpl::sql::codegen {

If::If(FunctionBuilder *function, ast::Expr *condition)
    : function_(function),
      position_(function_->GetCodeGen()->GetPosition()),
      prev_func_stmt_list_(nullptr),
      condition_(condition),
      then_stmts_(function_->GetCodeGen()->MakeEmptyBlock()),
      else_stmts_(nullptr),
      completed_(false) {
  TPL_ASSERT(codegen_->CurrentFunction() != nullptr, "Not within a function!");
  prev_func_stmt_list_ = function_->statements_;
  function_->statements_ = then_stmts_;
}

If::~If() { EndIf(); }

void If::Else() {
  else_stmts_ = function_->GetCodeGen()->MakeEmptyBlock();
  function_->statements_ = else_stmts_;
}

void If::EndIf() {
  if (completed_) {
    return;
  }

  TPL_ASSERT(codegen_->CurrentFunction() != nullptr, "Not within a function!");
  function_->statements_ = prev_func_stmt_list_;

  // Create and append the if statement.
  auto codegen = function_->GetCodeGen();
  auto if_stmt = codegen->GetFactory()->NewIfStmt(position_, condition_, then_stmts_, else_stmts_);
  function_->Append(if_stmt);

  // Done.
  completed_ = true;
}

}  // namespace tpl::sql::codegen
