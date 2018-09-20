#pragma once

#include <cstdint>

#include "util/macros.h"

namespace tpl::vm {

/*
 * The full macro list of bytecode instructions. The list is centralized here
 * to make modification easier because they are used in multiple contexts
 * throughout the codebase.
 */
#define BYTECODES_LIST(V) \
  V(Lt)                   \
  V(Le)                   \
  V(Eq)                   \
  V(Gt)                   \
  V(Ge)                   \
  V(Add)                  \
  V(Sub)                  \
  V(Mul)                  \
  V(Div)                  \
  V(Rem)                  \
  V(Shl)                  \
  V(Shr)                  \
  V(Ushr)                 \
  V(Neg)                  \
  V(Ldc)                  \
  V(Ret)                  \
  V(Beq)                  \
  V(Bge)                  \
  V(Bgt)                  \
  V(Ble)                  \
  V(Blt)                  \
  V(Bne)                  \
  V(Br)

/**
 * The enumeration of all possible bytecode instructions.
 */
enum class Bytecode : uint8_t {
#define F(inst, ...) inst,
  BYTECODES_LIST(F)
#undef F
#define COUNT_BYTECODE(inst, ...) +1
      Last = -1 BYTECODES_LIST(COUNT_BYTECODE)
#undef COUNT_BYTECODE
};

/**
 * Handy class for interacting with bytecode instructions.
 */
class Bytecodes {
 public:
  // The total number of bytecode instructions
  static const uint32_t kBytecodeCount =
      static_cast<uint32_t>(Bytecode::Last) + 1;

  // Returns the string representation of the given bytecode
  static const char *ToString(Bytecode bytecode);

  // Converts the given bytecode to a single-byte representation
  static uint8_t ToByte(Bytecode bytecode) {
    TPL_ASSERT(bytecode <= Bytecode::Last, "Invalid bytecode");
    return static_cast<uint8_t>(bytecode);
  }

  // Converts the given unsigned byte into the associated bytecode
  static Bytecode FromByte(uint8_t val) {
    auto bytecode = static_cast<Bytecode>(val);
    TPL_ASSERT(bytecode <= Bytecode::Last, "Invalid bytecode");
    return bytecode;
  }
};

}  // namespace tpl::vm