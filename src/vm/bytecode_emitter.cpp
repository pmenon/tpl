#include "vm/bytecode_emitter.h"

#include "logging/logger.h"
#include "vm/bytecode_label.h"
#include "vm/bytecode_traits.h"
#include "vm/bytecode_unit.h"

namespace tpl::vm {

void BytecodeEmitter::EmitLoadImm1(LocalVar dest, i8 val) {
  EmitOp(Bytecode::LoadImm1);
  EmitLocalVar(dest);
  EmitImmediateValue(val);
}

void BytecodeEmitter::EmitLoadImm2(LocalVar dest, i16 val) {
  EmitOp(Bytecode::LoadImm2);
  EmitLocalVar(dest);
  EmitImmediateValue(val);
}

void BytecodeEmitter::EmitLoadImm4(LocalVar dest, i32 val) {
  EmitOp(Bytecode::LoadImm4);
  EmitLocalVar(dest);
  EmitImmediateValue(val);
}

void BytecodeEmitter::EmitLoadImm8(LocalVar dest, i64 val) {
  EmitOp(Bytecode::LoadImm8);
  EmitLocalVar(dest);
  EmitImmediateValue(val);
}

void BytecodeEmitter::EmitUnaryOp(Bytecode bytecode, LocalVar dest,
                                  LocalVar input) {
  EmitOp(bytecode);
  EmitLocalVars(dest, input);
}

void BytecodeEmitter::EmitBinaryOp(Bytecode bytecode, LocalVar dest,
                                   LocalVar lhs, LocalVar rhs) {
  EmitOp(bytecode);
  EmitLocalVars(dest, lhs, rhs);
}

void BytecodeEmitter::EmitJump(Bytecode bytecode, BytecodeLabel *label) {
  TPL_ASSERT(Bytecodes::IsJump(bytecode), "Provided bytecode is not a jump");
  TPL_ASSERT((!label->is_bound() && bytecode == Bytecode::Jump) ||
                 (label->is_bound() && bytecode == Bytecode::JumpLoop),
             "Jump should only be used for forward jumps and JumpLoop for "
             "backwards jumps");

  // Emit the jump opcode and condition
  EmitOp(bytecode);
  EmitJump(label);
}

void BytecodeEmitter::EmitConditionalJump(Bytecode bytecode, LocalVar cond,
                                          BytecodeLabel *label) {
  TPL_ASSERT(Bytecodes::IsJump(bytecode), "Provided bytecode is not a jump");

  // Emit the jump opcode and condition
  EmitOp(bytecode);
  EmitLocalVar(cond);
  EmitJump(label);
}

void BytecodeEmitter::EmitLea(LocalVar dest, LocalVar src, u32 offset) {
  EmitOp(Bytecode::Lea);
  EmitLocalVars(dest, src);
  EmitImmediateValue(offset);
}

void BytecodeEmitter::EmitReturn() { EmitOp(Bytecode::Return); }

void BytecodeEmitter::EmitJump(BytecodeLabel *label) {
  std::size_t curr_offset = position();

  if (label->is_bound()) {
    // The label is already bound so this must be a backwards jump. We just need
    // to emit the delta offset directly into the bytestream.
    TPL_ASSERT(
        label->offset() <= curr_offset,
        "Label for backwards jump cannot be beyond current bytecode position");
    std::size_t delta = curr_offset - label->offset();
    TPL_ASSERT(delta < std::numeric_limits<u16>::max(),
               "Jump delta exceeds 16-bit value for jump offsets!");

    // Immediately emit the delta
    EmitImmediateValue(static_cast<u16>(delta));
  } else {
    // The label is not bound yet so this must be a forward jump. We set the
    // reference position in the label and use a placeholder offset in the
    // byte stream for now. We'll update the placeholder when the label is bound
    label->set_referrer(curr_offset);
    EmitImmediateValue(kJumpPlaceholder);
  }
}

void BytecodeEmitter::Emit(Bytecode bytecode, LocalVar operand_1) {
  EmitOp(bytecode);
  EmitLocalVar(operand_1);
}

void BytecodeEmitter::Emit(Bytecode bytecode, LocalVar operand_1,
                           LocalVar operand_2) {
  EmitOp(bytecode);
  EmitLocalVars(operand_1, operand_2);
}

void BytecodeEmitter::Bind(BytecodeLabel *label) {
  TPL_ASSERT(!label->is_bound(), "Cannot rebind labels");

  std::size_t curr_offset = position();

  if (label->is_forward_target()) {
    // We need to patch all the forward jumps
    auto &jump_locations = label->referrer_offsets();

    for (const auto &jump_location : jump_locations) {
      TPL_ASSERT(
          (curr_offset - jump_location) < std::numeric_limits<u16>::max(),
          "Jump delta exceeds 16-bit value for jump offsets!");

      u16 delta = static_cast<u16>(curr_offset - jump_location);
      u8 *raw_delta = reinterpret_cast<u8 *>(&delta);
      bytecodes_[jump_location] = raw_delta[0];
      bytecodes_[jump_location + 1] = raw_delta[1];
    }
  }

  label->BindTo(curr_offset);
}

const std::vector<u8> &BytecodeEmitter::Finish() { return bytecodes_; }

}  // namespace tpl::vm