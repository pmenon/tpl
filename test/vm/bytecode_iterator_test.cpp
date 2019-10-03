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
  vm::BytecodeEmitter emitter(GetMutableCode());

  LocalVar v1(0, LocalVar::AddressMode::Address);
  LocalVar v2(8, LocalVar::AddressMode::Address);
  LocalVar v3(16, LocalVar::AddressMode::Address);

  emitter.Emit(Bytecode::BitNeg_int8_t, v2, v1);
  emitter.EmitBinaryOp(Bytecode::Add_int16_t, v3, v2, v1);
  emitter.Emit(Bytecode::BitAnd_int8_t, v1, v2, v3);

  vm::BytecodeIterator iter(GetCode(), 0, GetCode().size());
  EXPECT_FALSE(iter.Done());
  EXPECT_EQ(Bytecode::BitNeg_int8_t, iter.CurrentBytecode());
  EXPECT_EQ(v2, iter.GetLocalOperand(0));
  EXPECT_EQ(v1, iter.GetLocalOperand(1));

  iter.Advance();

  EXPECT_FALSE(iter.Done());
  EXPECT_EQ(Bytecode::Add_int16_t, iter.CurrentBytecode());
  EXPECT_EQ(v3, iter.GetLocalOperand(0));
  EXPECT_EQ(v2, iter.GetLocalOperand(1));
  EXPECT_EQ(v1, iter.GetLocalOperand(2));

  iter.Advance();

  EXPECT_FALSE(iter.Done());
  EXPECT_EQ(Bytecode::BitAnd_int8_t, iter.CurrentBytecode());
  EXPECT_EQ(v1, iter.GetLocalOperand(0));
  EXPECT_EQ(v2, iter.GetLocalOperand(1));
  EXPECT_EQ(v3, iter.GetLocalOperand(2));

  iter.Advance();

  EXPECT_TRUE(iter.Done());
}

TEST_F(BytecodeIteratorTest, JumpTest) {
  vm::BytecodeEmitter emitter(GetMutableCode());

  LocalVar v1(0, LocalVar::AddressMode::Address);
  LocalVar v2(8, LocalVar::AddressMode::Address);
  LocalVar v3(16, LocalVar::AddressMode::Address);

  // We have a label that we bind to the start of the instruction stream. Thus,
  // a jump to the start would be a jump of -4 (to skip over the JUMP bytecode
  // instruction itself).
  vm::BytecodeLabel label;
  emitter.Bind(&label);
  emitter.EmitJump(Bytecode::Jump, &label);
  emitter.EmitBinaryOp(Bytecode::Add_int16_t, v3, v2, v1);

  vm::BytecodeIterator iter(GetCode(), 0, GetCode().size());
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
