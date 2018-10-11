#pragma once

#include <cstdint>
#include <vector>

#include "util/common.h"
#include "vm/bytecode_register.h"
#include "vm/bytecodes.h"

namespace tpl::vm {

class BytecodeUnit;
class BytecodeLabel;

class BytecodeEmitter {
  static const u16 kJumpPlaceholder = std::numeric_limits<u16>::max() - 1u;
 public:
  BytecodeEmitter() = default;

  DISALLOW_COPY_AND_MOVE(BytecodeEmitter);

  std::size_t position() const { return bytecodes_.size(); }

  void EmitLoadImm1(RegisterId reg_id, i8 val);
  void EmitLoadImm2(RegisterId reg_id, i16 val);
  void EmitLoadImm4(RegisterId reg_id, i32 val);
  void EmitLoadImm8(RegisterId reg_id, i64 val);

  void EmitJump(Bytecode bytecode, BytecodeLabel *label);
  void EmitConditionalJump(Bytecode bytecode, RegisterId cond, BytecodeLabel *label);

  void EmitReturn();

  void Emit(Bytecode bytecode, RegisterId dest, RegisterId input);
  void Emit(Bytecode bytecode, RegisterId dest, RegisterId lhs, RegisterId rhs);

  void Bind(BytecodeLabel *label);

  const std::vector<u8> &Finish();

 private:
  void EmitOp(Bytecode bytecode) {
    auto code = Bytecodes::ToByte(bytecode);
    auto *raw_code = reinterpret_cast<const u8 *>(&code);
    bytecodes_.push_back(raw_code[0]);
    bytecodes_.push_back(raw_code[1]);
  }

  template <typename T,
            typename std::enable_if_t<std::is_integral_v<T>, int> = 0>
  void EmitImmediateValue(T val) {
    bytecodes_.insert(bytecodes_.end(), sizeof(T), 0);
    *reinterpret_cast<T *>(&*(bytecodes_.end() - sizeof(T))) = val;
  }

  void EmitRegister(RegisterId reg_id) { EmitImmediateValue(reg_id); }

  template <typename... Regs,
            typename std::enable_if_t<
                std::conjunction_v<std::is_same<Regs, RegisterId>...>, int> = 0>
  void EmitRegisters(Regs... regs) {
    (EmitRegister(regs), ...);
  }

  void EmitJump(BytecodeLabel *label);

 private:
  std::vector<u8> bytecodes_;
};

}  // namespace tpl::vm