#include "vm/bytecode_emitter.h"

#include "logging/logger.h"
#include "vm/bytecode_unit.h"

namespace tpl::vm {

void BytecodeEmitter::EmitLiteral1(RegisterId dest, i8 val) {
  EmitOp(Bytecode::LoadConstant1);
  EmitRegister(dest);
  EmitImmediateValue(val);
}

void BytecodeEmitter::EmitLiteral2(RegisterId dest, i16 val) {
  EmitOp(Bytecode::LoadConstant2);
  EmitRegister(dest);
  EmitImmediateValue(val);
}

void BytecodeEmitter::EmitLiteral4(RegisterId dest, i32 val) {
  EmitOp(Bytecode::LoadConstant4);
  EmitRegister(dest);
  EmitImmediateValue(val);
}

void BytecodeEmitter::EmitLiteral8(RegisterId dest, i64 val) {
  EmitOp(Bytecode::LoadConstant8);
  EmitRegister(dest);
  EmitImmediateValue(val);
}

void BytecodeEmitter::Emit(Bytecode bytecode, RegisterId dest,
                           RegisterId input) {
  EmitOp(bytecode);
  EmitRegisters(dest, input);
}

void BytecodeEmitter::Emit(Bytecode bytecode, RegisterId dest, RegisterId lhs,
                           RegisterId rhs) {
  EmitOp(bytecode);
  EmitRegisters(dest, lhs, rhs);
}

void BytecodeEmitter::EmitReturn() { EmitOp(Bytecode::Return); }

const std::vector<u8> &BytecodeEmitter::Finish() { return bytecodes_; }

}  // namespace tpl::vm