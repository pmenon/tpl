#include "vm/bytecodes.h"

namespace tpl::vm {

const char *Bytecodes::ToString(Bytecode bytecode) {
  switch (bytecode) {
    default:
      break;
#define CASE(name, ...)     \
  case Bytecode::name: \
    return #name;
      BYTECODES_LIST(CASE)
#undef CASE
  }
  UNREACHABLE("Invalid bytecode");
}

}  // namespace tpl::vm