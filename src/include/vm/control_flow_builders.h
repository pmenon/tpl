#pragma once

#include "vm/bytecode_generator.h"
#include "vm/label.h"

namespace tpl::vm {

/**
 * Base class for all control-flow builders
 */
class ControlFlowBuilder {
 public:
  explicit ControlFlowBuilder(BytecodeGenerator *generator)
      : generator_(generator) {}

  virtual ~ControlFlowBuilder() = default;

 protected:
  BytecodeGenerator *generator() { return generator_; }

 private:
  BytecodeGenerator *generator_;
};

class BreakableBlockBuilder : public ControlFlowBuilder {
 public:
  explicit BreakableBlockBuilder(BytecodeGenerator *generator)
      : ControlFlowBuilder(generator) {}

  ~BreakableBlockBuilder() override;

  void Break();

  Label *break_label() { return &break_label_; }

 protected:
  void EmitJump(Label *label);

 private:
  Label break_label_;
};

class LoopBuilder : public BreakableBlockBuilder {
 public:
  explicit LoopBuilder(BytecodeGenerator *generator)
      : BreakableBlockBuilder(generator) {}

  ~LoopBuilder() override;

  void LoopHeader();
  void JumpToHeader();

  void Continue();

  void BindContinueTarget();

 private:
  Label *header_label() { return &header_label_; }
  Label *continue_label() { return &continue_label_; }

 private:
  Label header_label_;
  Label continue_label_;
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
  void JumpToEnd();

  Label *then_label() { return &then_label_; }
  Label *else_label() { return &else_label_; }

 private:
  Label *end_label() { return &end_label_; }

 private:
  Label then_label_;
  Label else_label_;
  Label end_label_;
};

}  // namespace tpl::vm