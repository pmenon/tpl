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
#define OPERAND_TYPE_LIST(V)          \
  V(None, false, OperandSize::None)   \
  V(Imm1, true, OperandSize::Byte)    \
  V(Imm2, true, OperandSize::Short)   \
  V(Imm4, true, OperandSize::Int)     \
  V(Imm8, true, OperandSize::Long)    \
  V(UImm2, false, OperandSize::Short) \
  V(Reg, false, OperandSize::Short)   \
  V(RegCount, false, OperandSize::Short)

/**
 * This enumeration lists all possible types of operands to any bytecode
 */
enum class OperandType : u8 {
#define OP_TYPE(Name, ...) Name,
  OPERAND_TYPE_LIST(OP_TYPE)
#undef OP_TYPE
};

}  // namespace tpl::vm