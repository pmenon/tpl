#pragma once

#include "vm/bytecode_generator.h"
#include "vm/bytecode_label.h"

namespace tpl::vm {

/**
 * Base class for all control-flow builders.
 */
class ControlFlowBuilder {
 public:
  /**
   * Construct a builder that will modify the given generator instance.
   * @param generator The generator.
   */
  explicit ControlFlowBuilder(BytecodeGenerator *generator) : generator_(generator) {}

  /**
   * Destructor.
   */
  virtual ~ControlFlowBuilder() = default;

 protected:
  // Access the generator.
  BytecodeGenerator *GetGenerator() { return generator_; }

 private:
  BytecodeGenerator *generator_;
};

/**
 * Base class for code-blocks that can be broken out of.
 */
class BreakableBlockBuilder : public ControlFlowBuilder {
 public:
  /**
   * Construct a builder that will modify the given generator instance.
   * @param generator The generator.
   */
  explicit BreakableBlockBuilder(BytecodeGenerator *generator) : ControlFlowBuilder(generator) {}

  /**
   * Destructor.
   */
  ~BreakableBlockBuilder() override;

  /**
   * Break out of the current block.
   */
  void Break();

  /**
   * @return The label that all breaks from this block jump to.
   */
  BytecodeLabel *GetBreakLabel() { return &break_label_; }

 protected:
  void EmitJump(BytecodeLabel *label);

 private:
  // The label we jump to from breaks.
  BytecodeLabel break_label_;
};

/**
 * Helper class to build loops.
 */
class LoopBuilder : public BreakableBlockBuilder {
 public:
  /**
   * Construct a loop builder.
   * @param generator The generator the loop writes.
   */
  explicit LoopBuilder(BytecodeGenerator *generator) : BreakableBlockBuilder(generator) {}

  /**
   * Destructor.
   */
  ~LoopBuilder() override;

  /**
   * Generate the header of the loop.
   */
  void LoopHeader();

  /**
   * Jump to the header of the loop from the current position in the bytecode.
   */
  void JumpToHeader();

  /**
   * Generate a 'continue' to skip the remainder of the loop and jump to the header.
   */
  void Continue();

  /**
   * Bind the target of the continue.
   */
  void BindContinueTarget();

 private:
  // Return the label associated with the header of the loop
  BytecodeLabel *GetHeaderLabel() { return &header_label_; }

  // Return the label associated with the target of the continue
  BytecodeLabel *GetContinueLabel() { return &continue_label_; }

 private:
  BytecodeLabel header_label_;
  BytecodeLabel continue_label_;
};

/**
 * A class to help in creating if-then-else constructs in VM bytecode.
 */
class IfThenElseBuilder : public ControlFlowBuilder {
 public:
  /**
   * Construct an if-then-else generator.
   * @param generator The generator the if-then-else writes.
   */
  explicit IfThenElseBuilder(BytecodeGenerator *generator) : ControlFlowBuilder(generator) {}

  /**
   * Destructor.
   */
  ~IfThenElseBuilder() override;

  /**
   * Generate the 'then' part of the if-then-else.
   */
  void Then();

  /**
   * Generate the 'else' part of the if-then-else.
   */
  void Else();

  /**
   * Jump to the end of the if-then-else.
   */
  void JumpToEnd();

  /**
   * @return The label associated with the 'then' block of this if-then-else.
   */
  BytecodeLabel *GetThenLabel() { return &then_label_; }

  /**
   * @return THe label associated with the 'else' block of this if-then-else
   */
  BytecodeLabel *GetElseLabel() { return &else_label_; }

 private:
  BytecodeLabel *end_label() { return &end_label_; }

 private:
  BytecodeLabel then_label_;
  BytecodeLabel else_label_;
  BytecodeLabel end_label_;
};

}  // namespace tpl::vm
