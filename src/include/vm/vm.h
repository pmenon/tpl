#pragma once

#include "logging/logger.h"
#include "util/common.h"
#include "util/region_containers.h"
#include "vm/bytecode_function_info.h"
#include "vm/bytecodes.h"

namespace tpl::vm {

class BytecodeModule;

/// Our virtual machine
class VM {
 public:
  /// Invoke the function with ID \a func_id in the module \a module. \a args
  /// contains the output and input parameters stored contiguously.
  static void InvokeFunction(const BytecodeModule &module, FunctionId func_id,
                             const u8 args[]);

 private:
  // Use a 1K stack initially
  static constexpr u32 kDefaultInitialStackSize = 1024;

  // Private constructor to force users to use InvokeFunction
  explicit VM(const BytecodeModule &module, util::Region *region = nullptr);

  // This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(VM);

  // Forward declare the frame
  class Frame;

  // Interpret the given instruction stream using the given execution frame
  void Interpret(const u8 *ip, Frame *frame);

  // Execute a call instruction
  const u8 *ExecuteCall(const u8 *ip, Frame *caller);

  // -------------------------------------------------------
  // Stack/Frame operations
  // TODO(pmenon): Move these stack operations into a separate Stack class
  // -------------------------------------------------------

  std::size_t stack_capacity() const { return stack().size(); }

  void GrowStackIfNeeded(std::size_t size) {
    TPL_ASSERT(sp() < stack_capacity(), "Stack overflow!");

    std::size_t available = stack_capacity() - sp();
    if (size < available) {
      return;
    }

    // Need to grow
    stack().resize(stack_capacity() + size);
  }

  u8 *AllocateFrame(std::size_t frame_size) {
    GrowStackIfNeeded(frame_size);

    std::size_t curr_sp = sp();
    sp_ += frame_size;
    return &stack_[curr_sp];
  }

  void ReleaseFrame(std::size_t frame_size) {
    TPL_ASSERT(sp() >= frame_size, "Stack underflow!");
    sp_ -= frame_size;
  }

  // -------------------------------------------------------
  // Accessors
  // -------------------------------------------------------

  util::RegionVector<u8> &stack() { return stack_; }

  const util::RegionVector<u8> &stack() const { return stack_; }

  const std::size_t sp() const { return sp_; }

  const BytecodeModule &module() const { return module_; }

 private:
  // A region managed by this virtual machine
  util::Region our_region_;

  // A pointer to either the internally managed region, or an injected one.
  // Regardless, this is the one we use for any and all allocations.
  util::Region *region_;

  // The stack and the current stack pointer. We use a vanilla byte-vector with
  // a stack position for our stack implementation
  util::RegionVector<u8> stack_;
  std::size_t sp_;

  const BytecodeModule &module_;

  UNUSED u64 bytecode_counts_[Bytecodes::kBytecodeCount];
};

}  // namespace tpl::vm
