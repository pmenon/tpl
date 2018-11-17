#include "vm/bytecode_emitter.h"

#include "vm/bytecode_label.h"

namespace tpl::vm {

void BytecodeEmitter::EmitAssignImm1(LocalVar dest, i8 val) {
  EmitOp(Bytecode::AssignImm1);
  EmitLocalVar(dest);
  EmitImmediateValue(val);
}

void BytecodeEmitter::EmitAssignImm2(LocalVar dest, i16 val) {
  EmitOp(Bytecode::AssignImm2);
  EmitLocalVar(dest);
  EmitImmediateValue(val);
}

void BytecodeEmitter::EmitAssignImm4(LocalVar dest, i32 val) {
  EmitOp(Bytecode::AssignImm4);
  EmitLocalVar(dest);
  EmitImmediateValue(val);
}

void BytecodeEmitter::EmitAssignImm8(LocalVar dest, i64 val) {
  EmitOp(Bytecode::AssignImm8);
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

void BytecodeEmitter::EmitLeaScaled(LocalVar dest, LocalVar src, LocalVar index,
                                    u32 scale, u32 offset) {
  EmitOp(Bytecode::LeaScaled);
  EmitLocalVars(dest, src, index);
  EmitImmediateValue(scale);
  EmitImmediateValue(offset);
}

void BytecodeEmitter::EmitCall(FunctionId func_id,
                               const std::vector<LocalVar> &params) {
  TPL_ASSERT(
      Bytecodes::GetNthOperandSize(Bytecode::Call, 1) == OperandSize::Short,
      "Expected argument count to be 2-byte short");
  TPL_ASSERT(params.size() < std::numeric_limits<u16>::max(),
             "Too many parameters!");

  EmitOp(Bytecode::Call);
  EmitImmediateValue(static_cast<u16>(func_id));
  EmitImmediateValue(static_cast<u16>(params.size()));
  for (LocalVar local : params) {
    EmitLocalVar(local);
  }
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

  if (label->IsForwardTarget()) {
    // We need to patch all the forward jumps
    auto &jump_locations = label->referrer_offsets();

    for (const auto &jump_location : jump_locations) {
      TPL_ASSERT(
          (curr_offset - jump_location) < std::numeric_limits<u16>::max(),
          "Jump delta exceeds 16-bit value for jump offsets!");

      u16 delta = static_cast<u16>(curr_offset - jump_location);
      u8 *raw_delta = reinterpret_cast<u8 *>(&delta);
      bytecode_[jump_location] = raw_delta[0];
      bytecode_[jump_location + 1] = raw_delta[1];
    }
  }

  label->BindTo(curr_offset);
}

}  // namespace tpl::vm