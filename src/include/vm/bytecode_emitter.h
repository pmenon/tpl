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

  void EmitLiteral1(RegisterId reg_id, i8 val);
  void EmitLiteral2(RegisterId reg_id, i16 val);
  void EmitLiteral4(RegisterId reg_id, i32 val);
  void EmitLiteral8(RegisterId reg_id, i64 val);

  void EmitReturn();

#define GEN_BIN_OPS(type)                                                   \
  void Emit##Add##_##type(RegisterId dest, RegisterId lhs, RegisterId rhs); \
  void Emit##Sub##_##type(RegisterId dest, RegisterId lhs, RegisterId rhs); \
  void Emit##Mul##_##type(RegisterId dest, RegisterId lhs, RegisterId rhs); \
  void Emit##Div##_##type(RegisterId dest, RegisterId lhs, RegisterId rhs); \
  void Emit##Rem##_##type(RegisterId dest, RegisterId lhs, RegisterId rhs);
#define GEN_COMPARISONS(type)                                                 \
  void Emit##GreaterThan##_##type(RegisterId dest, RegisterId lhs,            \
                                  RegisterId rhs);                            \
  void Emit##GreaterThanEqual##_##type(RegisterId dest, RegisterId lhs,       \
                                       RegisterId rhs);                       \
  void Emit##Equal##_##type(RegisterId dest, RegisterId lhs, RegisterId rhs); \
  void Emit##LessThan##_##type(RegisterId dest, RegisterId lhs,               \
                               RegisterId rhs);                               \
  void Emit##LessThanEqual##_##type(RegisterId dest, RegisterId lhs,          \
                                    RegisterId rhs);                          \
  void Emit##NotEqual##_##type(RegisterId dest, RegisterId lhs, RegisterId rhs);
  INT_TYPES(GEN_BIN_OPS)
  INT_TYPES(GEN_COMPARISONS)
#undef GEN_COMPARISONS
#undef GEN_BIN_OPS

  const std::vector<u8> &Finish();

 private:
  void EmitOp(Bytecode bytecode) {
    bytecodes_.push_back(Bytecodes::ToByte(bytecode));
  }

  void EmitRegister(RegisterId reg_id) {
    bytecodes_.push_back(static_cast<u8>(reg_id >> 8));
    bytecodes_.push_back(static_cast<u8>(reg_id));
  }

  void EmitRegisters(RegisterId regs...) { EmitRegister(regs); }

 private:
  std::vector<u8> bytecodes_;
};

}  // namespace tpl::vm
