#include "vm/bytecodes.h"

#include <algorithm>

namespace tpl::vm {

// static
u32 Bytecodes::MaxBytecodeNameLength() {
  return std::max({
#define HANDLE_INST(name, ...) sizeof(#name),
#include "vm/bytecodes.def"
#undef HANDLE_INST
                  });
}

// static
const char *Bytecodes::ToString(Bytecode bytecode) {
  const char *kNames[] = {
#define HANDLE_INST(name, ...) #name,
#include "vm/bytecodes.def"
#undef HANDLE_INST
  };
  return kNames[static_cast<u32>(bytecode)];
}

template <OperandType ...operands>
struct BytecodeTraits {
  static const int kOperandCount = sizeof...(operands);
};

// static
u8 Bytecodes::NumOperands(Bytecode bytecode) {
  u8 kNumBytecodeOperands[] = {
#define HANDLE_INST(name, ...) BytecodeTraits<__VA_ARGS__>::kOperandCount,
#include "vm/bytecodes.def"
#undef HANDLE_INST
  };
  return kNumBytecodeOperands[static_cast<u32>(bytecode)];
}

}  // namespace tpl::vm