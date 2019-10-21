#include "vm/control_flow_builders.h"

#include "vm/bytecode_emitter.h"

namespace tpl::vm {

// ---------------------------------------------------------
// Breakable blocks
// ---------------------------------------------------------

BreakableBlockBuilder::~BreakableBlockBuilder() {
  TPL_ASSERT(!GetBreakLabel()->IsBound(), "Break label cannot be bound!");
  GetGenerator()->GetEmitter()->Bind(GetBreakLabel());
}

void BreakableBlockBuilder::Break() { EmitJump(GetBreakLabel()); }

void BreakableBlockBuilder::EmitJump(BytecodeLabel *label) {
  GetGenerator()->GetEmitter()->EmitJump(Bytecode::Jump, label);
}

// ---------------------------------------------------------
// Loop Builders
// ---------------------------------------------------------

LoopBuilder::~LoopBuilder() = default;

void LoopBuilder::LoopHeader() {
  TPL_ASSERT(!GetHeaderLabel()->IsBound(), "Header cannot be rebound");
  GetGenerator()->GetEmitter()->Bind(GetHeaderLabel());
}

void LoopBuilder::JumpToHeader() {
  GetGenerator()->GetEmitter()->EmitJump(Bytecode::Jump, GetHeaderLabel());
}

void LoopBuilder::Continue() { EmitJump(GetContinueLabel()); }

void LoopBuilder::BindContinueTarget() {
  TPL_ASSERT(!GetContinueLabel()->IsBound(), "Continue label can only be bound once");
  GetGenerator()->GetEmitter()->Bind(GetContinueLabel());
}

// ---------------------------------------------------------
// If-Then-Else Builders
// ---------------------------------------------------------

IfThenElseBuilder::~IfThenElseBuilder() {
  if (!GetElseLabel()->IsBound()) {
    GetGenerator()->GetEmitter()->Bind(GetElseLabel());
  }

  TPL_ASSERT(!end_label()->IsBound(), "End label should not be bound yet");
  GetGenerator()->GetEmitter()->Bind(end_label());
}

void IfThenElseBuilder::Then() { GetGenerator()->GetEmitter()->Bind(GetThenLabel()); }

void IfThenElseBuilder::Else() { GetGenerator()->GetEmitter()->Bind(GetElseLabel()); }

void IfThenElseBuilder::JumpToEnd() {
  GetGenerator()->GetEmitter()->EmitJump(Bytecode::Jump, end_label());
}

}  // namespace tpl::vm
