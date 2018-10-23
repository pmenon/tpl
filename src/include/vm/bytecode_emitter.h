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

  void EmitLoadImm1(LocalVar dest, i8 val);
  void EmitLoadImm2(LocalVar dest, i16 val);
  void EmitLoadImm4(LocalVar dest, i32 val);
  void EmitLoadImm8(LocalVar dest, i64 val);

  void EmitJump(Bytecode bytecode, BytecodeLabel *label);
  void EmitConditionalJump(Bytecode bytecode, LocalVar cond,
                           BytecodeLabel *label);

  void EmitLea(LocalVar dest, LocalVar src, u32 offset);
  void EmitReturn();

  void EmitUnaryOp(Bytecode bytecode, LocalVar dest, LocalVar input);
  void EmitBinaryOp(Bytecode bytecode, LocalVar dest, LocalVar lhs,
                    LocalVar rhs);

  void Emit(Bytecode bytecode, LocalVar operand_1);
  void Emit(Bytecode bytecode, LocalVar operand_1, LocalVar operand_2);

  template <typename T,
            typename std::enable_if_t<std::is_integral_v<T>, u32> = 0>
  void Emit(Bytecode bytecode, LocalVar operand_1, T imm) {
    EmitOp(bytecode);
    EmitLocalVar(operand_1);
    EmitImmediateValue(imm);
  }

  void Bind(BytecodeLabel *label);

  const std::vector<u8> &Finish();

 private:
  template <typename T,
            typename std::enable_if_t<std::is_integral_v<T>, u32> = 0>
  void EmitImmediateValue(T val) {
    bytecodes_.insert(bytecodes_.end(), sizeof(T), 0);
    *reinterpret_cast<T *>(&*(bytecodes_.end() - sizeof(T))) = val;
  }

  void EmitOp(Bytecode bytecode) {
    EmitImmediateValue(Bytecodes::ToByte(bytecode));
  }

  void EmitLocalVar(LocalVar local) { EmitImmediateValue(local.Encode()); }

  template <typename... Locals,
            typename std::enable_if_t<
                std::conjunction_v<std::is_same<Locals, LocalVar>...>, u32> = 0>
  void EmitLocalVars(Locals... locals) {
    (EmitLocalVar(locals), ...);
  }

  void EmitJump(BytecodeLabel *label);

 private:
  std::vector<u8> bytecodes_;
};

}  // namespace tpl::vm
