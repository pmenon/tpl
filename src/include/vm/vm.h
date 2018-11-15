#pragma once

#include <memory>
#include <vector>

#include "util/common.h"
#include "util/region_containers.h"
#include "vm/bytecodes.h"

namespace tpl::vm {

class Module;

class VM {
  // Use a 1K stack initially
  static constexpr u32 kDefaultInitialStackSize = 1024;

 public:
  DISALLOW_COPY_AND_MOVE(VM);

  static void Execute(util::Region *region, const Module &module,
                      const std::string &name);

 private:
  class Frame;

  VM(util::Region *region, const Module &module);

  void Invoke(u32 func_id);

  void Interpret(const u8 *ip, Frame *frame);

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Stack/Frame operations
  ///
  //////////////////////////////////////////////////////////////////////////////

  std::size_t stack_capacity() const { return stack().size(); }

  void GrowStackIfNeeded(std::size_t size) {
    TPL_ASSERT(sp() < stack_capacity(), "Stack overflow!");

    std::size_t room_left = stack_capacity() - sp();
    if (size < room_left) {
      return;
    }

    // Need to grow
    stack().reserve(stack_capacity() + size);
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

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Accessors
  ///
  //////////////////////////////////////////////////////////////////////////////

  util::RegionVector<u8> &stack() { return stack_; }
  const util::RegionVector<u8> &stack() const { return stack_; }

  std::size_t sp() const { return sp_; }

  const Module &module() const { return module_; }

 private:
  util::RegionVector<u8> stack_;
  std::size_t sp_;

  const Module &module_;

  u64 bytecode_counts_[Bytecodes::kBytecodeCount];
};

}  // namespace tpl::vm