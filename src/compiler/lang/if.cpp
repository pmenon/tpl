//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// if.cpp
//
// Identification: src/codegen/lang/if.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "compiler/lang/if.h"

#include <compiler/function_builder.h>
#include <llvm/Transforms/Utils/BasicBlockUtils.h>

#include "compiler/type/boolean_type.h"
#include "compiler/value.h"

namespace tpl {
namespace compiler {
namespace lang {

If::If(CodeGen &codegen, llvm::Value *cond, std::string name, Block *then_bb,
       Block *else_bb)
    : codegen_(codegen),
      fn_(codegen_->GetInsertBlock()->getParent()),
      then_bb_(nullptr),
      last_bb_in_then_(nullptr),
      else_bb_(nullptr),
      last_bb_in_else_(nullptr) {
  Init(cond, then_bb, else_bb);
  // then_bb_->setName(name);
}

If::If(CodeGen &codegen, const compiler::Value &cond, std::string name,
       Block *then_bb, Block *else_bb)
    : If(codegen, type::Boolean::Instance().Reify(codegen, cond),
         std::move(name), then_bb, else_bb) {}

void If::Init(Value *cond, Block *then_bb, Block *else_bb) {
  // Set up the "then" block. If one was provided, use it. Otherwise, create a
  // new one now.
  then_bb_ = then_bb;
  else_bb_ = else_bb;

  if (then_bb_ == nullptr) {
    then_bb_ = llvm::BasicBlock::Create(codegen_.GetContext(), "then", fn_);
  }

  // The merging block where both "if-then" and "else" blocks merge into
  merge_bb_ = llvm::BasicBlock::Create(codegen_.GetContext(), "ifCont");

  // This instruction needs to be saved in case we have an else block
  branch_ = codegen_->CreateCondBr(cond, then_bb_,
                                   (else_bb != nullptr ? else_bb : merge_bb_));

  // Move to the "then" block so the user can generate code in the "true" branch
  codegen_->SetInsertPoint(then_bb_);
}

void If::ElseBlock() {
  // Branch to the merging block if the caller hasn't specifically branched
  // somewhere else
  BranchIfNotTerminated(merge_bb_);

  // If no else block was provided by the caller, generate one now
  if (else_bb_ == nullptr) {
    // Create a new else block
    else_bb_ = llvm::BasicBlock::Create(codegen_.GetContext(), "else", fn_);
    last_bb_in_else_ = else_bb_;

    // Replace the previous branch instruction that normally went to the merging
    // block on a false predicate to now branch into the new else block
    llvm::BranchInst *new_branch =
        llvm::BranchInst::Create(then_bb_, else_bb_, branch_->getCondition());
    ReplaceInstWithInst(branch_, new_branch);

    last_bb_in_then_ = codegen_->GetInsertBlock();
  }

  // Switch to the else block (either externally provided or created here)
  codegen_->SetInsertPoint(else_bb_);
}

void If::EndIf() {
  // Branch to the merging block if the caller hasn't specifically branched
  // somewhere else
  BranchIfNotTerminated(merge_bb_);

  llvm::BasicBlock *curr_bb = codegen_->GetInsertBlock();
  if (else_bb_ == nullptr) {
    // There was no else block, the current block is the last in the "then"
    last_bb_in_then_ = curr_bb;
  } else {
    // There was an else block, the current block is the last in the "else"
    last_bb_in_else_ = curr_bb;
  }

  // Append the merge block to the end, and set it as the new insertion point
  fn_->getBasicBlockList().push_back(merge_bb_);
  codegen_->SetInsertPoint(merge_bb_);
}

void If::BranchIfNotTerminated(llvm::BasicBlock *block) const {
  // Get the current block we're generating in
  llvm::BasicBlock *curr_bb = codegen_->GetInsertBlock();

  // Get the terminator instruction in the current block, if one exists.
  const llvm::TerminatorInst *terminator = curr_bb->getTerminator();

  // If no terminator exists, branch to the provided block
  if (terminator == nullptr) {
    codegen_->CreateBr(block);
  }
}

}  // namespace lang
}  // namespace compiler
}  // namespace tpl