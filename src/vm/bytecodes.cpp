#include "vm/bytecodes.h"

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

}  // namespace tpl::vm