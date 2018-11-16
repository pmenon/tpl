#include "gtest/gtest.h"

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
  EXPECT_EQ(OperandSize::Short, Bytecodes::GetOperandSizes(Bytecode::Jump)[0]);

  // Conditional jumps have two operands: one four-byte operand for the register
  // holding value of the boolean condition, and one two-byte jump offset
  EXPECT_EQ(OperandSize::Int,
            Bytecodes::GetNthOperandSize(Bytecode::JumpIfTrue, 0));
  EXPECT_EQ(OperandSize::Short,
            Bytecodes::GetNthOperandSize(Bytecode::JumpIfTrue, 1));

  // Binary ops usually have three operands, all 4-byte register IDs
  EXPECT_EQ(OperandSize::Int,
            Bytecodes::GetNthOperandSize(Bytecode::Add_i32, 0));
  EXPECT_EQ(OperandSize::Int,
            Bytecodes::GetNthOperandSize(Bytecode::Add_i32, 1));
  EXPECT_EQ(OperandSize::Int,
            Bytecodes::GetNthOperandSize(Bytecode::Add_i32, 2));
}

TEST_F(BytecodesTest, OperandTypesTest) {
  // Non-exhaustive test of operand types for various op codes

  // Imm loads
  EXPECT_EQ(OperandType::Local,
            Bytecodes::GetNthOperandType(Bytecode::AssignImm1, 0));
  EXPECT_EQ(OperandType::Imm1,
            Bytecodes::GetNthOperandType(Bytecode::AssignImm1, 1));

  // Jumps
  EXPECT_EQ(OperandType::UImm2,
            Bytecodes::GetNthOperandType(Bytecode::Jump, 0));

  // Conditional jumps have a 2-byte register ID and a 2-byte unsigned jump
  // offset
  EXPECT_EQ(OperandType::Local,
            Bytecodes::GetNthOperandType(Bytecode::JumpIfTrue, 0));
  EXPECT_EQ(OperandType::UImm2,
            Bytecodes::GetNthOperandType(Bytecode::JumpIfTrue, 1));

  // Binary ops usually have three operands, all 2-byte register IDs
  EXPECT_EQ(OperandType::Local,
            Bytecodes::GetNthOperandType(Bytecode::Add_i32, 0));
  EXPECT_EQ(OperandType::Local,
            Bytecodes::GetNthOperandType(Bytecode::Add_i32, 1));
  EXPECT_EQ(OperandType::Local,
            Bytecodes::GetNthOperandType(Bytecode::Add_i32, 2));
}

TEST_F(BytecodesTest, BytecodeSizeTest) {
  // Non-exhaustive test of operand types for various op codes

  // Imm loads
  EXPECT_EQ(9u, Bytecodes::Size(Bytecode::AssignImm1));
  EXPECT_EQ(10u, Bytecodes::Size(Bytecode::AssignImm2));
  EXPECT_EQ(12u, Bytecodes::Size(Bytecode::AssignImm4));
  EXPECT_EQ(16u, Bytecodes::Size(Bytecode::AssignImm8));

  // Jumps
  EXPECT_EQ(6u, Bytecodes::Size(Bytecode::Jump));

  // Conditional jumps have a 4-byte register ID and a 2-byte unsigned jump
  // offset
  EXPECT_EQ(10u, Bytecodes::Size(Bytecode::JumpIfTrue));
  EXPECT_EQ(10u, Bytecodes::Size(Bytecode::JumpIfFalse));

  // Binary ops usually have three operands, all 4-byte register IDs
  EXPECT_EQ(16u, Bytecodes::Size(Bytecode::Add_i32));
  EXPECT_EQ(16u, Bytecodes::Size(Bytecode::Sub_i8));
  EXPECT_EQ(16u, Bytecodes::Size(Bytecode::Mul_i8));
  EXPECT_EQ(16u, Bytecodes::Size(Bytecode::Div_i16));
  EXPECT_EQ(16u, Bytecodes::Size(Bytecode::Rem_i64));
}

}  // namespace tpl::vm::test