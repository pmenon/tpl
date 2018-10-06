#pragma once

#include <algorithm>
#include <cstdint>

#include "util/common.h"
#include "util/macros.h"
#include "vm/bytecode_operands.h"

namespace tpl::vm {

// clang-format off

// Creates instances of a given opcode for all integer primitive types
#define CREATE_FOR_INT_TYPES(func, op, ...)      \
  func(op##_##i8, __VA_ARGS__)                   \
  func(op##_##i16, __VA_ARGS__)                  \
  func(op##_##i32, __VA_ARGS__)                  \
  func(op##_##i64, __VA_ARGS__)                  \
  func(op##_##u8, __VA_ARGS__)                   \
  func(op##_##u16, __VA_ARGS__)                  \
  func(op##_##u32, __VA_ARGS__)                  \
  func(op##_##u64, __VA_ARGS__)

// Creates instances of a given opcode for all floating-point primitive types
#define CREATE_FOR_FLOAT_TYPES(func, op) func(op, f32) func(op, f64)

// Creates instances of a given opcode for *ALL* primitive types
#define CREATE_FOR_ALL_TYPES(func, op, ...)   \
  CREATE_FOR_INT_TYPES(func, op, __VA_ARGS__) \
  CREATE_FOR_FLOAT_TYPES(func, op, __VA_ARGS__)

#define GET_BASE_FOR_INT_TYPES(op) (op##_i8)
#define GET_BASE_FOR_FLOAT_TYPES(op) (op##_f32)

/**
 * This macro lists all bytecodes and its arguments
 */
#define BYTECODE_LIST(V)                                                                           \
  V(Wide)                                                                                          \
  V(ExtraWide)                                                                                     \
                                                                                                   \
  /* Constants */                                                                                  \
  V(LoadConstant1, OperandType::Reg, OperandType::Imm1)                                            \
  V(LoadConstant2, OperandType::Reg, OperandType::Imm2)                                            \
  V(LoadConstant4, OperandType::Reg, OperandType::Imm4)                                            \
  V(LoadConstant8, OperandType::Reg, OperandType::Imm8)                                            \
                                                                                                   \
  /* Branching */                                                                                  \
  V(Jump, OperandType::UImm2)                                                                      \
  V(JumpIfTrue, OperandType::Reg, OperandType::UImm2)                                              \
  V(JumpIfFalse, OperandType::Reg, OperandType::UImm2)                                             \
                                                                                                   \
  /* Table scanning */                                                                             \
  V(ScanOpen)                                                                                      \
  V(ScanNext)                                                                                      \
  V(ScanClose)                                                                                     \
                                                                                                   \
  V(Return)                                                                                        \
                                                                                                   \
  /* Primitive operations */                                                                       \
  CREATE_FOR_INT_TYPES(V, Add, OperandType::Reg, OperandType::Reg, OperandType::Reg)               \
  CREATE_FOR_INT_TYPES(V, Sub, OperandType::Reg, OperandType::Reg, OperandType::Reg)               \
  CREATE_FOR_INT_TYPES(V, Mul, OperandType::Reg, OperandType::Reg, OperandType::Reg)               \
  CREATE_FOR_INT_TYPES(V, Div, OperandType::Reg, OperandType::Reg, OperandType::Reg)               \
  CREATE_FOR_INT_TYPES(V, Rem, OperandType::Reg, OperandType::Reg, OperandType::Reg)               \
  CREATE_FOR_INT_TYPES(V, BitAnd, OperandType::Reg, OperandType::Reg, OperandType::Reg)            \
  CREATE_FOR_INT_TYPES(V, BitOr, OperandType::Reg, OperandType::Reg, OperandType::Reg)             \
  CREATE_FOR_INT_TYPES(V, BitXor, OperandType::Reg, OperandType::Reg, OperandType::Reg)            \
  CREATE_FOR_INT_TYPES(V, Neg, OperandType::Reg, OperandType::Reg)                                 \
  CREATE_FOR_INT_TYPES(V, BitNeg, OperandType::Reg, OperandType::Reg)                              \
  CREATE_FOR_INT_TYPES(V, GreaterThan, OperandType::Reg, OperandType::Reg, OperandType::Reg)       \
  CREATE_FOR_INT_TYPES(V, GreaterThanEqual, OperandType::Reg, OperandType::Reg, OperandType::Reg)  \
  CREATE_FOR_INT_TYPES(V, Equal, OperandType::Reg, OperandType::Reg, OperandType::Reg)             \
  CREATE_FOR_INT_TYPES(V, LessThan, OperandType::Reg, OperandType::Reg, OperandType::Reg)          \
  CREATE_FOR_INT_TYPES(V, LessThanEqual, OperandType::Reg, OperandType::Reg, OperandType::Reg)     \
  CREATE_FOR_INT_TYPES(V, NotEqual, OperandType::Reg, OperandType::Reg, OperandType::Reg)

// clang-format on

/**
 * The enumeration of all possible bytecode instructions. A bytecode takes up
 * two unsigned bytes, meaning we can support upto 2^16 = 65,536 bytecode ops,
 * more than enough.
 */
enum class Bytecode : u16 {
#define HANDLE_INST(inst, ...) inst,
  BYTECODE_LIST(HANDLE_INST)
#undef HANDLE_INST
#define HANDLE_INST(inst, ...) +1
      Last = -1 BYTECODE_LIST(HANDLE_INST)
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