#include "tpl_test.h"

#include "vm/bytecodes.h"

namespace tpl::vm::test {

class BytecodesTest : public TplTest {};

TEST_F(BytecodesTest, OperandCountTest) {
  // Non-exhaustive test of operand counts for various op codes

  // Imm loads
  EXPECT_EQ(2u, Bytecodes::NumOperands(Bytecode::AssignImm1));
  EXPECT_EQ(2u, Bytecodes::NumOperands(Bytecode::AssignImm2));
  EXPECT_EQ(2u, Bytecodes::NumOperands(Bytecode::AssignImm4));
  EXPECT_EQ(2u, Bytecodes::NumOperands(Bytecode::AssignImm8));

  // Jumps
  EXPECT_EQ(1u, Bytecodes::NumOperands(Bytecode::Jump));
  EXPECT_EQ(2u, Bytecodes::NumOperands(Bytecode::JumpIfTrue));
  EXPECT_EQ(2u, Bytecodes::NumOperands(Bytecode::JumpIfFalse));

  // Binary ops
  EXPECT_EQ(3u, Bytecodes::NumOperands(Bytecode::Add_i32));
  EXPECT_EQ(3u, Bytecodes::NumOperands(Bytecode::Mul_i32));
  EXPECT_EQ(3u, Bytecodes::NumOperands(Bytecode::Div_i32));
  EXPECT_EQ(3u, Bytecodes::NumOperands(Bytecode::Rem_i32));
  EXPECT_EQ(3u, Bytecodes::NumOperands(Bytecode::Sub_i32));

  // Return has no arguments
  EXPECT_EQ(0u, Bytecodes::NumOperands(Bytecode::Return));
}

TEST_F(BytecodesTest, OperandSizeTest) {
  // Non-exhaustive test of operand sizes for various op codes

  // Imm loads
  EXPECT_EQ(OperandSize::Int,
            Bytecodes::GetNthOperandSize(Bytecode::AssignImm1, 0));
  EXPECT_EQ(OperandSize::Byte,
            Bytecodes::GetNthOperandSize(Bytecode::AssignImm1, 1));

  // Jumps
  EXPECT_EQ(OperandSize::Int, Bytecodes::GetOperandSizes(Bytecode::Jump)[0]);

  // Conditional jumps have two operands: one four-byte operand for the local
  // holding value of the boolean condition, and one four-byte jump offset
  EXPECT_EQ(OperandSize::Int,
            Bytecodes::GetNthOperandSize(Bytecode::JumpIfTrue, 0));
  EXPECT_EQ(OperandSize::Int,
            Bytecodes::GetNthOperandSize(Bytecode::JumpIfTrue, 1));

  EXPECT_EQ(OperandSize::Int,
            Bytecodes::GetNthOperandSize(Bytecode::JumpIfFalse, 0));
  EXPECT_EQ(OperandSize::Int,
            Bytecodes::GetNthOperandSize(Bytecode::JumpIfFalse, 1));

  // Binary ops usually have three operands, all 4-byte register IDs
  EXPECT_EQ(OperandSize::Int,
            Bytecodes::GetNthOperandSize(Bytecode::Add_i32, 0));
  EXPECT_EQ(OperandSize::Int,
            Bytecodes::GetNthOperandSize(Bytecode::Add_i32, 1));
  EXPECT_EQ(OperandSize::Int,
            Bytecodes::GetNthOperandSize(Bytecode::Add_i32, 2));
}

TEST_F(BytecodesTest, OperandOffsetTest) {
  // Non-exhaustive test of operand sizes for various op codes

  // Imm loads
  EXPECT_EQ(4u, Bytecodes::GetNthOperandOffset(Bytecode::AssignImm1, 0));

  // Jumps
  EXPECT_EQ(4u, Bytecodes::GetNthOperandOffset(Bytecode::Jump, 0));

  // Conditional jumps have always have a 4-byte condition and 4-byte offset
  EXPECT_EQ(4u, Bytecodes::GetNthOperandOffset(Bytecode::JumpIfTrue, 0));
  EXPECT_EQ(8u, Bytecodes::GetNthOperandOffset(Bytecode::JumpIfTrue, 1));

  EXPECT_EQ(4u, Bytecodes::GetNthOperandOffset(Bytecode::JumpIfFalse, 0));
  EXPECT_EQ(8u, Bytecodes::GetNthOperandOffset(Bytecode::JumpIfFalse, 1));

  // Binary ops usually have three operands, all 4-byte register IDs
  EXPECT_EQ(4u, Bytecodes::GetNthOperandOffset(Bytecode::Add_i32, 0));
  EXPECT_EQ(8u, Bytecodes::GetNthOperandOffset(Bytecode::Add_i32, 1));
  EXPECT_EQ(12u, Bytecodes::GetNthOperandOffset(Bytecode::Add_i32, 2));
}

TEST_F(BytecodesTest, OperandTypesTest) {
  // Non-exhaustive test of operand types for various op codes

  // Imm loads
  EXPECT_EQ(OperandType::Local,
            Bytecodes::GetNthOperandType(Bytecode::AssignImm1, 0));
  EXPECT_EQ(OperandType::Imm1,
            Bytecodes::GetNthOperandType(Bytecode::AssignImm1, 1));

  // Jumps
  EXPECT_EQ(OperandType::JumpOffset,
            Bytecodes::GetNthOperandType(Bytecode::Jump, 0));

  // Conditional jumps have a 2-byte register ID and a 2-byte unsigned jump
  // offset
  EXPECT_EQ(OperandType::Local,
            Bytecodes::GetNthOperandType(Bytecode::JumpIfTrue, 0));
  EXPECT_EQ(OperandType::JumpOffset,
            Bytecodes::GetNthOperandType(Bytecode::JumpIfTrue, 1));

  // Binary ops usually have three operands, all 2-byte register IDs
  EXPECT_EQ(OperandType::Local,
            Bytecodes::GetNthOperandType(Bytecode::Add_i32, 0));
  EXPECT_EQ(OperandType::Local,
            Bytecodes::GetNthOperandType(Bytecode::Add_i32, 1));
  EXPECT_EQ(OperandType::Local,
            Bytecodes::GetNthOperandType(Bytecode::Add_i32, 2));
}

}  // namespace tpl::vm::test
