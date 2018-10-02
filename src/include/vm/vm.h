#pragma once

#include <memory>
#include <vector>

#include "util/common.h"
#include "vm/bytecodes.h"

namespace tpl::vm {

class VM {
 public:
 private:
  VM();

  class Frame;
  void Run(Frame *frame);

 private:
  u64 bytecode_counts_[Bytecodes::kBytecodeCount];
};

}  // namespace tpl::vm