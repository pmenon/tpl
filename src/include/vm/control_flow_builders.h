#pragma once

#include "vm/bytecode_generator.h"
#include "vm/bytecode_label.h"

namespace tpl::vm {

/**
 * Base class for all control-flow builders
 */
class ControlFlowBuilder {
 public:
  explicit ControlFlowBuilder(BytecodeGenerator *generator)
      : generator_(generator) {}

  virtual ~ControlFlowBuilder() = default;

  void EmitJump(BytecodeLabel *label);

 protected:
  BytecodeGenerator *generator() { return generator_; }

 private:
  BytecodeGenerator *generator_;
};

class LoopBuilder : public ControlFlowBuilder {
 public:
  explicit LoopBuilder(BytecodeGenerator *generator)
      : ControlFlowBuilder(generator) {}

  ~LoopBuilder() override;

  void LoopHeader();

  void JumpToHeader();

  void LoopBody();

  void Continue();

  void BindContinueTarget();

  BytecodeLabel *header_label() { return &header_label_; }
  BytecodeLabel *body_label() { return &body_label_; }
  BytecodeLabel *continue_label() { return &continue_label_; }

 private:
  BytecodeLabel header_label_;
  BytecodeLabel body_label_;
  BytecodeLabel continue_label_;
};

/**
 * A class to help in creating if-then-else constructs in VM bytecode.
 */
class IfThenElseBuilder : public ControlFlowBuilder {
 public:
  explicit IfThenElseBuilder(BytecodeGenerator *generator)
      : ControlFlowBuilder(generator) {}

  ~IfThenElseBuilder() override;

  void Then();
  void Else();

  void JumpToEnd(BytecodeLabel *end_label);

  BytecodeLabel *then_label() { return &then_label_; }
  BytecodeLabel *else_label() { return &else_label_; }

 private:
  BytecodeLabel then_label_;
  BytecodeLabel else_label_;
};

}  // namespace tpl::vm