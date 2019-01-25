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
  // Use a 1K stack initially
  static constexpr u32 kDefaultInitialStackSize = 1024;

 public:
  VM(util::Region *region, const BytecodeModule &module);

  /// This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(VM);

  /// Execute the given function in this virtual machine
  ///
  /// \tparam ArgTypes Template arguments specifying the arguments to the
  /// function. At this point, we don't do any type-checking!
  /// \param func The function to run
  /// \param ip The instruction pointer pointing to the first instruction in the
  /// function's bytecode
  /// \param args The arguments to pass to the function. These are copied into
  /// the function's execution frame, so beware!
  template <typename... ArgTypes>
  void Execute(const FunctionInfo &func, const u8 *ip, ArgTypes... args);

 private:
  class Frame;
  class FrameBuilder;
  class InstructionStream;

#define DECLARE_HANDLER(Name, ...) \
  void XOp##Name(InstructionStream *is, Frame *frame);
  BYTECODE_LIST(DECLARE_HANDLER)
#undef DECLARE_HANDLER

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
  // The stack and the current stack pointer. We use a vanilla byte-vector with
  // a stack position for our stack implementation
  util::RegionVector<u8> stack_;
  std::size_t sp_;

  const BytecodeModule &module_;

  u64 bytecode_counts_[Bytecodes::kBytecodeCount];
};

// ---------------------------------------------------------
// VM Frame
// ---------------------------------------------------------

/// An execution frame where all function's local variables and parameters live
/// for the duration of the function's lifetime.
class VM::Frame {
  friend class VM;

 public:
  Frame(VM *vm, std::size_t frame_size) : vm_(vm), frame_size_(frame_size) {
    frame_data_ = vm->AllocateFrame(frame_size);
    TPL_ASSERT(frame_data_ != nullptr, "Frame data cannot be null");
    TPL_ASSERT(frame_size_ >= 0, "Frame size must be >= 0");
  }

  ~Frame() { vm()->ReleaseFrame(frame_size()); }

  /// Access the local variable at the given index in the fame. The \ref 'index'
  /// attribute is encoded and indicates whether the local variable is accessed
  /// through an indirection (i.e., if the variable has to be dereferenced or
  /// loaded)
  /// \tparam T The type of the variable the user expects
  /// \param index The encoded index into the frame where the variable is
  /// \return The value of the variable. Note that this is copied!
  template <typename T>
  T LocalAt(u32 index) const {
    LocalVar local = LocalVar::Decode(index);

    EnsureInFrame(local);

    auto val = reinterpret_cast<uintptr_t>(&frame_data_[local.GetOffset()]);

    if (local.GetAddressMode() == LocalVar::AddressMode::Value) {
      return *(T *)(val);
    }

    return (T)val;
  }

 private:
#ifndef NDEBUG
  // Ensure the local variable is valid
  void EnsureInFrame(LocalVar var) const {
    if (var.GetOffset() >= frame_size()) {
      std::string error_msg =
          fmt::format("Accessing local at offset {}, beyond frame of size {}",
                      var.GetOffset(), frame_size());
      LOG_ERROR("{}", error_msg);
      throw std::runtime_error(error_msg);
    }
  }
#else
  void EnsureInFrame(UNUSED LocalVar var) const {}
#endif

  VM *vm() const { return vm_; }

  u8 *raw_frame() const { return frame_data_; }

  std::size_t frame_size() const { return frame_size_; }

 private:
  VM *vm_;
  u8 *frame_data_;
  std::size_t frame_size_;
};

// ---------------------------------------------------------
// VM Frame Builder
// ---------------------------------------------------------

/// Helper class to construct a frame from a set of user-provided C/C++ function
/// arguments. Note that TPL has call-by-value semantics, hence all arguments
/// are copied.
class VM::FrameBuilder {
 public:
  template <typename... ArgTypes>
  static void Prepare(Frame *frame, ArgTypes... args) {
    FrameBuilder builder;
    builder.WriteArgs(frame->raw_frame(), args...);
  }

 private:
  inline void WriteArgs(UNUSED u8 *buffer) {}

  template <typename HeadT, typename... RestT>
  inline void WriteArgs(u8 *buffer, const HeadT &head, const RestT &... rest) {
    TPL_MEMCPY(buffer, reinterpret_cast<const u8 *>(&head), sizeof(head));
    WriteArgs(buffer + sizeof(head), rest...);
  }
};

template <typename... ArgTypes>
void VM::Execute(const FunctionInfo &func, const u8 *ip, ArgTypes... args) {
  // Create and prepare frame for interpretation
  VM::Frame frame(this, func.frame_size());
  VM::FrameBuilder::Prepare(&frame, args...);

  // All good, let's go
  Interpret(ip, &frame);
}

}  // namespace tpl::vm