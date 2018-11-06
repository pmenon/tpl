#include "vm/control_flow_builders.h"

#include "vm/bytecode_emitter.h"

namespace tpl::vm {

////////////////////////////////////////////////////////////////////////////////
///
/// Control flow builder
///
////////////////////////////////////////////////////////////////////////////////

void ControlFlowBuilder::EmitJump(BytecodeLabel *label) {
  generator()->emitter()->EmitJump(label);
}

////////////////////////////////////////////////////////////////////////////////
///
/// Loop builder
///
////////////////////////////////////////////////////////////////////////////////

LoopBuilder::~LoopBuilder() = default;

void LoopBuilder::LoopHeader() {
  TPL_ASSERT(!header_label()->is_bound(), "Header cannot be rebound");
  generator()->emitter()->Bind(header_label());
}

void LoopBuilder::JumpToHeader() {
  generator()->emitter()->EmitJump(header_label());
}

void LoopBuilder::LoopBody() {
  TPL_ASSERT(!body_label()->is_bound(), "Body cannot be rebound");
  generator()->emitter()->Bind(body_label());
}

void LoopBuilder::Continue() { EmitJump(continue_label()); }

void LoopBuilder::BindContinueTarget() {
  TPL_ASSERT(!continue_label()->is_bound(),
             "Continue label can only be bound once");
  generator()->emitter()->Bind(continue_label());
}

////////////////////////////////////////////////////////////////////////////////
///
/// If-then-else
///
////////////////////////////////////////////////////////////////////////////////

IfThenElseBuilder::~IfThenElseBuilder() {
  if (!else_label()->is_bound()) {
    generator()->emitter()->Bind(else_label());
  }
}

void IfThenElseBuilder::Then() { generator()->emitter()->Bind(then_label()); }

void IfThenElseBuilder::Else() { generator()->emitter()->Bind(else_label()); }

void IfThenElseBuilder::JumpToEnd(BytecodeLabel *end_label) {
  EmitJump(end_label);
}

}  // namespace tpl::vm