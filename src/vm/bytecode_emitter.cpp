#include "vm/bytecode_emitter.h"

#include "logging/logger.h"
#include "vm/bytecode_unit.h"

namespace tpl::vm {

void BytecodeEmitter::EmitLoadImm1(RegisterId dest, i8 val) {
  EmitOp(Bytecode::LoadImm1);
  EmitRegister(dest);
  EmitImmediateValue(val);
}

void BytecodeEmitter::EmitLoadImm2(RegisterId dest, i16 val) {
  EmitOp(Bytecode::LoadImm2);
  EmitRegister(dest);
  EmitImmediateValue(val);
}

void BytecodeEmitter::EmitLoadImm4(RegisterId dest, i32 val) {
  EmitOp(Bytecode::LoadImm4);
  EmitRegister(dest);
  EmitImmediateValue(val);
}

void BytecodeEmitter::EmitLoadImm8(RegisterId dest, i64 val) {
  EmitOp(Bytecode::LoadImm8);
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