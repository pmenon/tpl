#include <vector>

#include "util/region.h"
#include "util/test_harness.h"
#include "vm/bytecode_emitter.h"
#include "vm/bytecode_iterator.h"
#include "vm/bytecode_label.h"
#include "vm/vm.h"

namespace tpl::vm {

class BytecodeIteratorTest : public TplTest {
 public:
  std::vector<uint8_t> *GetMutableCode() { return &code_; }
  const std::vector<uint8_t> &GetCode() const { return code_; }

 private:
  std::vector<uint8_t> code_;
};

TEST_F(BytecodeIteratorTest, SimpleIteratorTest) {
  BytecodeEmitter emitter(GetMutableCode());

  LocalVar v1(0, LocalVar::AddressMode::Address);
  LocalVar v2(8, LocalVar::AddressMode::Address);
  LocalVar v3(16, LocalVar::AddressMode::Address);

  emitter.Emit(Bytecode::BitNeg_int8_t, v2, v1);
  emitter.EmitBinaryOp(Bytecode::Add_int16_t, v3, v2, v1);
  emitter.Emit(Bytecode::BitAnd_int8_t, v1, v2, v3);
  emitter.EmitAssignImm1(v1, 44);
  emitter.EmitAssignImm2(v1, 4444);
  emitter.EmitAssignImm4(v1, -4444444);
  emitter.EmitAssignImm8(v1, -444444444);
  emitter.EmitAssignImm4F(v2, -123.45);
  emitter.EmitAssignImm8F(v2, -123456.89999);

  BytecodeIterator iter(GetCode());

  // 8-bit Negation.
  EXPECT_FALSE(iter.Done());
  EXPECT_EQ(Bytecode::BitNeg_int8_t, iter.CurrentBytecode());
  EXPECT_EQ(v2, iter.GetLocalOperand(0));
  EXPECT_EQ(v1, iter.GetLocalOperand(1));

  // 16-bit Addition.
  iter.Advance();
  EXPECT_FALSE(iter.Done());
  EXPECT_EQ(Bytecode::Add_int16_t, iter.CurrentBytecode());
  EXPECT_EQ(v3, iter.GetLocalOperand(0));
  EXPECT_EQ(v2, iter.GetLocalOperand(1));
  EXPECT_EQ(v1, iter.GetLocalOperand(2));

  // 8-bit Bit-AND.
  iter.Advance();
  EXPECT_FALSE(iter.Done());
  EXPECT_EQ(Bytecode::BitAnd_int8_t, iter.CurrentBytecode());
  EXPECT_EQ(v1, iter.GetLocalOperand(0));
  EXPECT_EQ(v2, iter.GetLocalOperand(1));
  EXPECT_EQ(v3, iter.GetLocalOperand(2));

  // 8-bit Immediate Assignment.
  iter.Advance();
  EXPECT_FALSE(iter.Done());
  EXPECT_EQ(Bytecode::AssignImm1, iter.CurrentBytecode());
  EXPECT_EQ(44, iter.GetImmediateIntegerOperand(1));

  // 16-bit Immediate Assignment.
  iter.Advance();
  EXPECT_FALSE(iter.Done());
  EXPECT_EQ(Bytecode::AssignImm2, iter.CurrentBytecode());
  EXPECT_EQ(4444, iter.GetImmediateIntegerOperand(1));

  // 32-bit Immediate Assignment.
  iter.Advance();
  EXPECT_FALSE(iter.Done());
  EXPECT_EQ(Bytecode::AssignImm4, iter.CurrentBytecode());
  EXPECT_EQ(-4444444, iter.GetImmediateIntegerOperand(1));

  // 64-bit Immediate Assignment.
  iter.Advance();
  EXPECT_FALSE(iter.Done());
  EXPECT_EQ(Bytecode::AssignImm8, iter.CurrentBytecode());
  EXPECT_EQ(-444444444, iter.GetImmediateIntegerOperand(1));

  // 32-bit Float Immediate Assignment.
  iter.Advance();
  EXPECT_FALSE(iter.Done());
  EXPECT_EQ(Bytecode::AssignImm4F, iter.CurrentBytecode());
  EXPECT_FLOAT_EQ(-123.45, iter.GetImmediateFloatOperand(1));

  // 64-bit Float Immediate Assignment.
  iter.Advance();
  EXPECT_FALSE(iter.Done());
  EXPECT_EQ(Bytecode::AssignImm8F, iter.CurrentBytecode());
  EXPECT_DOUBLE_EQ(-123456.89999, iter.GetImmediateFloatOperand(1));

  // Should be done.
  iter.Advance();
  EXPECT_TRUE(iter.Done());
}

TEST_F(BytecodeIteratorTest, StaticLocalTest) {
  BytecodeEmitter emitter(GetMutableCode());

  LocalVar v1(0, LocalVar::AddressMode::Address);
  LocalVar v2(8, LocalVar::AddressMode::Address);
  LocalVar v3(16, LocalVar::AddressMode::Address);

  emitter.EmitInitString(v1, v2, 10);
  emitter.EmitInitString(v3, v1, 20);

  BytecodeIterator iter(GetCode());

  // First string-init.
  EXPECT_FALSE(iter.Done());
  EXPECT_EQ(Bytecode::InitString, iter.CurrentBytecode());
  EXPECT_EQ(v1, iter.GetLocalOperand(0));
  EXPECT_EQ(v2, iter.GetStaticLocalOperand(1));
  EXPECT_EQ(10, iter.GetUnsignedImmediateIntegerOperand(2));

  // Second string-init.
  iter.Advance();
  EXPECT_FALSE(iter.Done());
  EXPECT_EQ(Bytecode::InitString, iter.CurrentBytecode());
  EXPECT_EQ(v3, iter.GetLocalOperand(0));
  EXPECT_EQ(v1, iter.GetStaticLocalOperand(1));
  EXPECT_EQ(20, iter.GetUnsignedImmediateIntegerOperand(2));

  // Should be done.
  iter.Advance();
  EXPECT_TRUE(iter.Done());
}

TEST_F(BytecodeIteratorTest, CallTest) {
  BytecodeEmitter emitter(GetMutableCode());

  LocalVar arg1(0, LocalVar::AddressMode::Address);
  LocalVar arg2(8, LocalVar::AddressMode::Address);
  LocalVar arg3(16, LocalVar::AddressMode::Address);

  FunctionId id1 = 1;
  FunctionId id2 = 1;

  emitter.EmitCall(id1, {arg1, arg2});
  emitter.EmitCall(id2, {arg1, arg2, arg1, arg2, arg1, arg2, arg1, arg2});

  BytecodeIterator iter(GetCode());

  std::vector<LocalVar> params;

  // First call: id1(arg1,arg2)
  EXPECT_FALSE(iter.Done());
  EXPECT_EQ(Bytecode::Call, iter.CurrentBytecode());
  EXPECT_EQ(id1, iter.GetFunctionIdOperand(0));
  EXPECT_EQ(2, iter.GetLocalCountOperand(1, &params));
  EXPECT_EQ(arg1, params[0]);
  EXPECT_EQ(arg2, params[1]);

  // Second call: id2(arg1, arg2, arg1, arg2, arg1, arg2, arg1, arg2)
  iter.Advance();
  EXPECT_FALSE(iter.Done());
  EXPECT_EQ(Bytecode::Call, iter.CurrentBytecode());
  EXPECT_EQ(id2, iter.GetFunctionIdOperand(0));
  EXPECT_EQ(8, iter.GetLocalCountOperand(1, &params));
  for (uint32_t i = 0; i < params.size(); i += 2) {
    EXPECT_EQ(arg1, params[i + 0]);
    EXPECT_EQ(arg2, params[i + 1]);
  }

  // Should be done.
  iter.Advance();
  EXPECT_TRUE(iter.Done());
}

TEST_F(BytecodeIteratorTest, JumpTest) {
  BytecodeEmitter emitter(GetMutableCode());

  LocalVar v1(0, LocalVar::AddressMode::Address);
  LocalVar v2(8, LocalVar::AddressMode::Address);
  LocalVar v3(16, LocalVar::AddressMode::Address);

  // We have a label that we bind to the start of the instruction stream. Thus,
  // a jump to the start would be a jump of -4 (to skip over the JUMP bytecode
  // instruction itself).
  BytecodeLabel label;
  emitter.Bind(&label);
  emitter.EmitJump(Bytecode::Jump, &label);
  emitter.EmitBinaryOp(Bytecode::Add_int16_t, v3, v2, v1);

  BytecodeIterator iter(GetCode());
  EXPECT_FALSE(iter.Done());
  EXPECT_EQ(Bytecode::Jump, iter.CurrentBytecode());
  EXPECT_EQ(-4, iter.GetJumpOffsetOperand(0));

  iter.Advance();
  EXPECT_FALSE(iter.Done());
  EXPECT_EQ(Bytecode::Add_int16_t, iter.CurrentBytecode());
  EXPECT_EQ(v3, iter.GetLocalOperand(0));
  EXPECT_EQ(v2, iter.GetLocalOperand(1));
  EXPECT_EQ(v1, iter.GetLocalOperand(2));
}

}  // namespace tpl::vm
