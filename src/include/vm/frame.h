#pragma once

#include "vm/stack.h"

namespace tpl::vm {

class BytecodeFunction;

/*
 * A frame encapsulates interpretation state for a function.
 */
class Frame {
 public:
  Frame(uint32_t max_locals, uint32_t max_stack)
      : previous_(nullptr),
        func_(nullptr),
        return_pc_(nullptr),
        stack_(max_stack),
        locals_() {}

  DISALLOW_COPY_AND_MOVE(Frame);

  Frame *previous() { return previous_; }
  void set_previous(Frame *previous) { previous_ = previous; }

  BytecodeFunction *function() const { return func_; }
  void set_function(BytecodeFunction *func) { func_ = func; }

  const uint8_t *return_pc() const { return return_pc_; }
  void set_return_pc(const uint8_t *return_pc) { return_pc_ = return_pc; }

  Stack &stack() { return stack_; }

  Locals &locals() { return locals_; }

 private:
  Frame *previous_;
  BytecodeFunction *func_;
  const uint8_t *return_pc_;
  Stack stack_;
  Locals locals_;
};

}  // namespace tpl::vm