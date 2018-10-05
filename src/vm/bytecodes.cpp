#include "vm/bytecodes.h"

#include <algorithm>

namespace tpl::vm {

// static
u32 Bytecodes::MaxBytecodeNameLength() {
  static constexpr const u32 kMaxInstNameLength = std::max({
#define HANDLE_INST(name, ...) sizeof(#name),
#include "vm/bytecodes.def"
#undef HANDLE_INST
  });
  return kMaxInstNameLength;
}

// static
const char *Bytecodes::ToString(Bytecode bytecode) {
  static constexpr const char *kNames[] = {
#define HANDLE_INST(name, ...) #name,
#include "vm/bytecodes.def"
#undef HANDLE_INST
  };
  return kNames[static_cast<u32>(bytecode)];
}

template <OperandType... operands>
struct BytecodeTraits {
  static const int kOperandCount = sizeof...(operands);
  static const OperandType kOperandTypes[];
};

template <OperandType... operands>
const OperandType BytecodeTraits<operands...>::kOperandTypes[] = {operands...};

// static
u8 Bytecodes::NumOperands(Bytecode bytecode) {
  static constexpr u8 kNumBytecodeOperands[] = {
#define HANDLE_INST(name, ...) BytecodeTraits<__VA_ARGS__>::kOperandCount,
#include "vm/bytecodes.def"
#undef HANDLE_INST
  };
  return kNumBytecodeOperands[static_cast<u32>(bytecode)];
}

const OperandType *Bytecodes::GetOperandTypes(Bytecode bytecode) {
  static constexpr const OperandType *kOperandTypes[] = {
#define HANDLE_INST(name, ...) BytecodeTraits<__VA_ARGS__>::kOperandTypes,
#include "vm/bytecodes.def"
#undef HANDLE_INST
  };
  return kOperandTypes[static_cast<u32>(bytecode)];
}

}  // namespace tpl::vm