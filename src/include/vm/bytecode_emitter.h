#pragma once

#include <cstdint>
#include <vector>

#include "util/common.h"
#include "vm/bytecode_register.h"
#include "vm/bytecodes.h"

namespace tpl::vm {

class BytecodeUnit;

class BytecodeEmitter {
 public:
  BytecodeEmitter() = default;

  DISALLOW_COPY_AND_MOVE(BytecodeEmitter);

  std::size_t position() const { return bytecodes_.size(); }

  void EmitLiteral1(RegisterId reg_id, i8 val);
  void EmitLiteral2(RegisterId reg_id, i16 val);
  void EmitLiteral4(RegisterId reg_id, i32 val);
  void EmitLiteral8(RegisterId reg_id, i64 val);

  void EmitReturn();

  void Emit(Bytecode bytecode, RegisterId dest, RegisterId input);
  void Emit(Bytecode bytecode, RegisterId dest, RegisterId lhs, RegisterId rhs);

  const std::vector<u8> &Finish();

 private:
  void EmitOp(Bytecode bytecode) {
    bytecodes_.push_back(Bytecodes::ToByte(bytecode));
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

 private:
  std::vector<u8> bytecodes_;
};

}  // namespace tpl::vm
