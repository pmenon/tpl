#pragma once

#include <cstdint>

#include "vm/bytecodes.h"
#include "vm/types.h"

namespace tpl::vm {

class BytecodeFunction;
class Frame;

class VM {
 public:
  static VmValue Invoke(BytecodeFunction &function);

 private:
  void Run(Frame &frame);

 private:
  uint64_t bytecode_counts_[Bytecodes::kBytecodeCount];
};

}  // namespace tpl::vm