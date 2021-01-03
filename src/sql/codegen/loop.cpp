#include "sql/codegen/loop.h"

#include "ast/ast_node_factory.h"
#include "sql/codegen/codegen.h"
#include "sql/codegen/function_builder.h"

namespace tpl::sql::codegen {

Loop::Loop(FunctionBuilder *function, const edsl::Value<void> &init,
           const edsl::Value<bool> &condition, const edsl::Value<void> &next)
    : function_(function),
      position_(function_->GetCodeGen()->GetPosition()),
      prev_statements_(nullptr),
      init_(init.GetNode()),
      cond_(condition.GetRaw()),
      next_(next.GetNode()),
      body_(function_->GetCodeGen()->MakeEmptyBlock()),
      completed_(false) {
  // Stash the previous statement list so we can restore it upon completion.
  prev_statements_ = function_->statements_;
  // Swap in our loop-body statement list as the active statement list.
  function_->statements_ = body_;
  // Bump indent for loop body.
  function_->GetCodeGen()->Indent();
}

// Static cast to disambiguate constructor.
Loop::Loop(FunctionBuilder *function, const edsl::Value<bool> &condition)
    : Loop(function, edsl::Value<void>(nullptr), condition, edsl::Value<void>(nullptr)) {}

Loop::~Loop() { EndLoop(); }

void Loop::EndLoop() {
  if (completed_) {
    return;
  }

  // Restore the previous statement list, now that we're done.
  function_->statements_ = prev_statements_;

  // Create and append the if statement.
  auto codegen = function_->GetCodeGen();
  auto loop = codegen->NodeFactory()->NewForStatement(position_, init_, cond_, next_, body_);
  function_->Append(loop);

  // Bump line.
  codegen->NewLine();

  // Done.
  completed_ = true;
}

}  // namespace tpl::sql::codegen
