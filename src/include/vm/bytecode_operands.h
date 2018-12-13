#pragma once

#include "util/common.h"

namespace tpl::vm {

/**
 * This enumeration lists all possible sizes of operands to any bytecode
 */
enum class OperandSize : u8 {
  None = 0,
  Byte = 1,
  Short = 2,
  Int = 4,
  Long = 8
};

/**
 * This macro list provides information about all possible operand types to a
 * bytecode operation. The format is: Name, IsSigned, BaseSize
 */
#define OPERAND_TYPE_LIST(V)               \
  V(None, false, OperandSize::None)        \
  V(Imm1, true, OperandSize::Byte)         \
  V(Imm2, true, OperandSize::Short)        \
  V(Imm4, true, OperandSize::Int)          \
  V(Imm8, true, OperandSize::Long)         \
  V(UImm2, false, OperandSize::Short)      \
  V(UImm4, false, OperandSize::Int)        \
  V(JumpOffset, true, OperandSize::Short) \
  V(Local, false, OperandSize::Int)        \
  V(LocalCount, false, OperandSize::Short)

/**
 * This enumeration lists all possible types of operands to any bytecode
 */
enum class OperandType : u8 {
#define OP_TYPE(Name, ...) Name,
  OPERAND_TYPE_LIST(OP_TYPE)
#undef OP_TYPE
};

class OperandTypes {
 public:
  static bool IsSignedImmediate(OperandType operand_type) {
    return operand_type == OperandType::Imm1 ||
           operand_type == OperandType::Imm2 ||
           operand_type == OperandType::Imm4 ||
           operand_type == OperandType::Imm8;
  }

  static bool IsUnsignedImmediate(OperandType operand_type) {
    return operand_type == OperandType::UImm2 ||
           operand_type == OperandType::UImm4;
  }

  static bool IsLocal(OperandType operand_type) {
    return operand_type == OperandType::Local;
  }

  static bool IsLocalCount(OperandType operand_type) {
    return operand_type == OperandType::LocalCount;
  }
};

}  // namespace tpl::vm