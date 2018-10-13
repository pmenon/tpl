#pragma once

#include <memory>
#include <vector>

#include "util/common.h"
#include "vm/bytecode_unit.h"
#include "vm/bytecodes.h"

namespace tpl::vm {

class VM {
 public:
  static void Execute(const BytecodeUnit &unit, const std::string &name);

  class Frame;

 private:
  VM(const BytecodeUnit &unit);

  void Invoke(FunctionId func_id);

  void Run(Frame *frame);

  const BytecodeUnit &unit() const { return unit_; }

  Frame *current_frame() { return curr_frame_; }
  void set_current_frame(Frame *frame) { curr_frame_ = frame; }

 private:
  const BytecodeUnit &unit_;
  Frame *curr_frame_;

  u64 bytecode_counts_[Bytecodes::kBytecodeCount];
};

}  // namespace tpl::vm