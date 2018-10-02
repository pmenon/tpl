#include "vm/bytecode_emitter.h"

#include "logging/logger.h"
#include "vm/bytecode_unit.h"

namespace tpl::vm {

void BytecodeEmitter::EmitLiteral1(RegisterId dest, i8 val) {
  EmitOp(Bytecode::LoadConstant1);
  EmitRegister(dest);
  bytecodes_.push_back(static_cast<u8>(val));
}

void BytecodeEmitter::EmitLiteral2(RegisterId dest, i16 val) {
  EmitOp(Bytecode::LoadConstant2);
  EmitRegister(dest);
  bytecodes_.push_back(static_cast<u8>(val >> 8));
  bytecodes_.push_back(static_cast<u8>(val));
}

void BytecodeEmitter::EmitLiteral4(RegisterId dest, i32 val) {
  EmitOp(Bytecode::LoadConstant4);
  EmitRegister(dest);
  bytecodes_.push_back(static_cast<u8>(val >> 24));
  bytecodes_.push_back(static_cast<u8>(val >> 16));
  bytecodes_.push_back(static_cast<u8>(val >> 8));
  bytecodes_.push_back(static_cast<u8>(val));
}

void BytecodeEmitter::EmitLiteral8(RegisterId dest, i64 val) {
  EmitOp(Bytecode::LoadConstant8);
  EmitRegister(dest);
  bytecodes_.push_back(static_cast<u8>(val >> 56));
  bytecodes_.push_back(static_cast<u8>(val >> 48));
  bytecodes_.push_back(static_cast<u8>(val >> 40));
  bytecodes_.push_back(static_cast<u8>(val >> 32));
  bytecodes_.push_back(static_cast<u8>(val >> 24));
  bytecodes_.push_back(static_cast<u8>(val >> 16));
  bytecodes_.push_back(static_cast<u8>(val >> 8));
  bytecodes_.push_back(static_cast<u8>(val));
}

#define GEN_BIN_OPS(type)                                                   \
  void BytecodeEmitter::Emit##Add##_##type(RegisterId dest, RegisterId lhs, \
                                           RegisterId rhs) {                \
    EmitOp(Bytecode::Add##_##type);                                         \
    EmitRegisters(dest, lhs, rhs);                                          \
  }                                                                         \
  void BytecodeEmitter::Emit##Sub##_##type(RegisterId dest, RegisterId lhs, \
                                           RegisterId rhs) {                \
    EmitOp(Bytecode::Sub##_##type);                                         \
    EmitRegisters(dest, lhs, rhs);                                          \
  }                                                                         \
  void BytecodeEmitter::Emit##Mul##_##type(RegisterId dest, RegisterId lhs, \
                                           RegisterId rhs) {                \
    EmitOp(Bytecode::Mul##_##type);                                         \
    EmitRegisters(dest, lhs, rhs);                                          \
  }                                                                         \
  void BytecodeEmitter::Emit##Div##_##type(RegisterId dest, RegisterId lhs, \
                                           RegisterId rhs) {                \
    EmitOp(Bytecode::Div##_##type);                                         \
    EmitRegisters(dest, lhs, rhs);                                          \
  }                                                                         \
  void BytecodeEmitter::Emit##Rem##_##type(RegisterId dest, RegisterId lhs, \
                                           RegisterId rhs) {                \
    EmitOp(Bytecode::Rem##_##type);                                         \
    EmitRegisters(dest, lhs, rhs);                                          \
  }
#define GEN_COMPARISONS(type)                                                 \
  void BytecodeEmitter::Emit##GreaterThan##_##type(                           \
      RegisterId dest, RegisterId lhs, RegisterId rhs) {                      \
    EmitOp(Bytecode::GreaterThan##_##type);                                   \
    EmitRegisters(dest, lhs, rhs);                                            \
  }                                                                           \
  void BytecodeEmitter::Emit##GreaterThanEqual##_##type(                      \
      RegisterId dest, RegisterId lhs, RegisterId rhs) {                      \
    EmitOp(Bytecode::GreaterThanEqual##_##type);                              \
    EmitRegisters(dest, lhs, rhs);                                            \
  }                                                                           \
  void BytecodeEmitter::Emit##Equal##_##type(RegisterId dest, RegisterId lhs, \
                                             RegisterId rhs) {                \
    EmitOp(Bytecode::Equal##_##type);                                         \
    EmitRegisters(dest, lhs, rhs);                                            \
  }                                                                           \
  void BytecodeEmitter::Emit##LessThan##_##type(                              \
      RegisterId dest, RegisterId lhs, RegisterId rhs) {                      \
    EmitOp(Bytecode::LessThan##_##type);                                      \
    EmitRegisters(dest, lhs, rhs);                                            \
  }                                                                           \
  void BytecodeEmitter::Emit##LessThanEqual##_##type(                         \
      RegisterId dest, RegisterId lhs, RegisterId rhs) {                      \
    EmitOp(Bytecode::LessThanEqual##_##type);                                 \
    EmitRegisters(dest, lhs, rhs);                                            \
  }                                                                           \
  void BytecodeEmitter::Emit##NotEqual##_##type(                              \
      RegisterId dest, RegisterId lhs, RegisterId rhs) {                      \
    EmitOp(Bytecode::NotEqual##_##type);                                      \
    EmitRegisters(dest, lhs, rhs);                                            \
  }
INT_TYPES(GEN_BIN_OPS)
INT_TYPES(GEN_COMPARISONS)
#undef GEN_COMPARISONS
#undef GEN_BIN_OPS

void BytecodeEmitter::EmitReturn() { EmitOp(Bytecode::Return); }

const std::vector<u8> &BytecodeEmitter::Finish() { return bytecodes_; }

}  // namespace tpl::vm