#pragma once

#include <cstdint>

#include "util/common.h"
#include "util/macros.h"

namespace tpl::vm {

/**
 * The enumeration of all possible bytecode instructions.
 */
enum class Bytecode : u8 {
#define HANDLE_INST(inst) inst,
#include "vm/bytecodes.def"
#undef HANDLE_INST
#define HANDLE_INST(inst) +1
  Last = -1
#include "vm/bytecodes.def"
#undef HANDLE_INST
};

/**
 * Handy class for interacting with bytecode instructions.
 */
class Bytecodes {
 public:
  // The total number of bytecode instructions
  static const u32 kBytecodeCount = static_cast<u32>(Bytecode::Last) + 1;

  // Returns the string representation of the given bytecode
  static const char *ToString(Bytecode bytecode);

  // Returns if the provided bytecode is a prefix-scaling bytecode
  static bool IsPrefixScalingCode(Bytecode bytecode) {
    return bytecode == Bytecode::Wide || bytecode == Bytecode::ExtraWide;
  }

  // Converts the given bytecode to a single-byte representation
  static u8 ToByte(Bytecode bytecode) {
    TPL_ASSERT(bytecode <= Bytecode::Last, "Invalid bytecode");
    return static_cast<u8>(bytecode);
  }

  // Converts the given unsigned byte into the associated bytecode
  static Bytecode FromByte(u8 val) {
    auto bytecode = static_cast<Bytecode>(val);
    TPL_ASSERT(bytecode <= Bytecode::Last, "Invalid bytecode");
    return bytecode;
  }
};

}  // namespace tpl::vm