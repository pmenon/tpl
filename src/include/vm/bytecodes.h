#pragma once

#include <algorithm>
#include <cstdint>

#include "util/common.h"
#include "util/macros.h"
#include "vm/bytecode_operands.h"

namespace tpl::vm {

/**
 * The enumeration of all possible bytecode instructions. A bytecode takes up
 * two unsigned bytes, meaning we can support upto 2^16 = 65,536 bytecode ops,
 * more than enough.
 */
enum class Bytecode : u16 {
#define HANDLE_INST(inst, ...) inst,
#include "vm/bytecodes.def"
#undef HANDLE_INST
#define HANDLE_INST(inst, ...) +1
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

  // Return the total number of bytecodes
  static constexpr u32 NumBytecodes() { return kBytecodeCount; }

  // Return the maximum length of any bytecode instruction in bytes
  static u32 MaxBytecodeNameLength();

  // Returns the string representation of the given bytecode
  static const char *ToString(Bytecode bytecode) {
    return kBytecodeNames[static_cast<u32>(bytecode)];
  }

  // Return the number of operands a bytecode accepts
  static u32 NumOperands(Bytecode bytecode) {
    return kBytecodeOperandCounts[static_cast<u32>(bytecode)];
  }

  // Return an array of the operand types to the given bytecode
  static const OperandType *GetOperandTypes(Bytecode bytecode) {
    return kBytecodeOperandTypes[static_cast<u32>(bytecode)];
  }

  // Return an array of the sizes of all operands to the given bytecode
  static const OperandSize *GetOperandSizes(Bytecode bytecode) {
    return kBytecodeOperandSizes[static_cast<u32>(bytecode)];
  }

  // Return the type of the Nth operand to the given bytecode
  static OperandType GetNthOperandType(Bytecode bytecode, u8 idx) {
    TPL_ASSERT(idx < NumOperands(bytecode),
               "Accessing out-of-bounds operand number for bytecode");
    return GetOperandTypes(bytecode)[idx];
  }

  // Return the type of the Nth operand to the given bytecode
  static OperandSize GetNthOperandSize(Bytecode bytecode, u8 idx) {
    TPL_ASSERT(idx < NumOperands(bytecode),
               "Accessing out-of-bounds operand number for bytecode");
    return GetOperandSizes(bytecode)[idx];
  }

  // Return the total size (in bytes) of the bytecode including it's operands
  static u32 Size(Bytecode bytecode) {
    return kBytecodeSizes[static_cast<u32>(bytecode)];
  }

  // Returns if the provided bytecode is a prefix-scaling bytecode
  static bool IsPrefixScalingCode(Bytecode bytecode) {
    return bytecode == Bytecode::Wide || bytecode == Bytecode::ExtraWide;
  }

  // Converts the given bytecode to a single-byte representation
  static std::underlying_type_t<Bytecode> ToByte(Bytecode bytecode) {
    TPL_ASSERT(bytecode <= Bytecode::Last, "Invalid bytecode");
    return static_cast<std::underlying_type_t<Bytecode>>(bytecode);
  }

  // Converts the given unsigned byte into the associated bytecode
  static Bytecode FromByte(std::underlying_type_t<Bytecode> val) {
    auto bytecode = static_cast<Bytecode>(val);
    TPL_ASSERT(bytecode <= Bytecode::Last, "Invalid bytecode");
    return bytecode;
  }

 private:
  static const char *kBytecodeNames[];
  static u32 kBytecodeOperandCounts[];
  static const OperandType *kBytecodeOperandTypes[];
  static const OperandSize *kBytecodeOperandSizes[];
  static u32 kBytecodeSizes[];
};

}  // namespace tpl::vm