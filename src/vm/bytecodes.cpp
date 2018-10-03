#include "vm/bytecodes.h"

#include <algorithm>

namespace tpl::vm {

const char *Bytecodes::ToString(Bytecode bytecode) {
  switch (bytecode) {
    default:
      break;
#define HANDLE_INST(name, ...) \
  case Bytecode::name:         \
    return #name;
#include "vm/bytecodes.def"
#undef HANDLE_INST
  }
  UNREACHABLE("Invalid bytecode");
}

constexpr u32 Bytecodes::MaxBytecodeNameLength() {
  return std::max({
#define HANDLE_INST(name, ...) sizeof(#name),
#include "vm/bytecodes.def"
#undef HANDLE_INST
  });
}

}  // namespace tpl::vm