#include "vm/bytecodes.h"

#include <algorithm>

#include "vm/bytecode_traits.h"

namespace tpl::vm {

// static
const char *Bytecodes::kBytecodeNames[] = {
#define HANDLE_INST(name, ...) #name,
    BYTECODE_LIST(HANDLE_INST)
#undef HANDLE_INST
};

// static
u32 Bytecodes::kBytecodeOperandCounts[] = {
#define HANDLE_INST(name, ...) BytecodeTraits<__VA_ARGS__>::kOperandCount,
    BYTECODE_LIST(HANDLE_INST)
#undef HANDLE_INST
};

// static
const OperandType *Bytecodes::kBytecodeOperandTypes[] = {
#define HANDLE_INST(name, ...) BytecodeTraits<__VA_ARGS__>::kOperandTypes,
    BYTECODE_LIST(HANDLE_INST)
#undef HANDLE_INST
};

// static
const OperandSize *Bytecodes::kBytecodeOperandSizes[] = {
#define HANDLE_INST(name, ...) BytecodeTraits<__VA_ARGS__>::kOperandSizes,
    BYTECODE_LIST(HANDLE_INST)
#undef HANDLE_INST
};

// static
u32 Bytecodes::kBytecodeSizes[] = {
#define HANDLE_INST(name, ...) BytecodeTraits<__VA_ARGS__>::kSize,
    BYTECODE_LIST(HANDLE_INST)
#undef HANDLE_INST
};

// static
u32 Bytecodes::MaxBytecodeNameLength() {
  static constexpr const u32 kMaxInstNameLength = std::max({
#define HANDLE_INST(name, ...) sizeof(#name),
      BYTECODE_LIST(HANDLE_INST)
#undef HANDLE_INST
  });
  return kMaxInstNameLength;
}

}  // namespace tpl::vm