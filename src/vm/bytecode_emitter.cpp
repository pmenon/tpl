#include "vm/bytecode_emitter.h"

#include "vm/bytecode_label.h"

namespace tpl::vm {

void BytecodeEmitter::EmitDeref(Bytecode bytecode, LocalVar dest,
                                LocalVar src) {
  TPL_ASSERT(bytecode == Bytecode::Deref1 || bytecode == Bytecode::Deref2 ||
                 bytecode == Bytecode::Deref4 || bytecode == Bytecode::Deref8,
             "Bytecode is not a Deref code");
  EmitAll(bytecode, dest, src);
}

void BytecodeEmitter::EmitDerefN(LocalVar dest, LocalVar src, u32 len) {
  EmitAll(Bytecode::DerefN, dest, src);
}

void BytecodeEmitter::EmitAssign(Bytecode bytecode, LocalVar dest,
                                 LocalVar src) {
  TPL_ASSERT(bytecode == Bytecode::Assign1 || bytecode == Bytecode::Assign2 ||
                 bytecode == Bytecode::Assign4 || bytecode == Bytecode::Assign8,
             "Bytecode is not an Assign code");
  EmitAll(bytecode, dest, src);
}

void BytecodeEmitter::EmitAssignImm1(LocalVar dest, i8 val) {
  EmitAll(Bytecode::AssignImm1, dest, val);
}

void BytecodeEmitter::EmitAssignImm2(LocalVar dest, i16 val) {
  EmitAll(Bytecode::AssignImm2, dest, val);
}

void BytecodeEmitter::EmitAssignImm4(LocalVar dest, i32 val) {
  EmitAll(Bytecode::AssignImm4, dest, val);
}

void BytecodeEmitter::EmitAssignImm8(LocalVar dest, i64 val) {
  EmitAll(Bytecode::AssignImm8, dest, val);
}

void BytecodeEmitter::EmitUnaryOp(Bytecode bytecode, LocalVar dest,
                                  LocalVar input) {
  EmitAll(bytecode, dest, input);
}

void BytecodeEmitter::EmitBinaryOp(Bytecode bytecode, LocalVar dest,
                                   LocalVar lhs, LocalVar rhs) {
  EmitAll(bytecode, dest, lhs, rhs);
}

void BytecodeEmitter::EmitLea(LocalVar dest, LocalVar src, u32 offset) {
  EmitAll(Bytecode::Lea, dest, src, offset);
}

void BytecodeEmitter::EmitLeaScaled(LocalVar dest, LocalVar src, LocalVar index,
                                    u32 scale, u32 offset) {
  EmitAll(Bytecode::LeaScaled, dest, src, index, scale, offset);
}

void BytecodeEmitter::EmitCall(FunctionId func_id,
                               const std::vector<LocalVar> &params) {
  TPL_ASSERT(
      Bytecodes::GetNthOperandSize(Bytecode::Call, 1) == OperandSize::Short,
      "Expected argument count to be 2-byte short");
  TPL_ASSERT(params.size() < std::numeric_limits<u16>::max(),
             "Too many parameters!");

  EmitAll(Bytecode::Call, static_cast<u16>(func_id),
          static_cast<u16>(params.size()));
  for (LocalVar local : params) {
    EmitImpl(local);
  }
}

void BytecodeEmitter::EmitReturn() { EmitImpl(Bytecode::Return); }

void BytecodeEmitter::Bind(BytecodeLabel *label) {
  TPL_ASSERT(!label->is_bound(), "Cannot rebind labels");

  std::size_t curr_offset = position();

  if (label->IsForwardTarget()) {
    // We need to patch all locations in the bytecode that forward jump to the
    // given bytecode label. Each referrer is stored in the bytecode label ...
    auto &jump_locations = label->referrer_offsets();

    for (const auto &jump_location : jump_locations) {
      TPL_ASSERT(
          (curr_offset - jump_location) < std::numeric_limits<i32>::max(),
          "Jump delta exceeds 32-bit value for jump offsets!");

      i32 delta = static_cast<i32>(curr_offset - jump_location);
      u8 *raw_delta = reinterpret_cast<u8 *>(&delta);
      bytecode_[jump_location] = raw_delta[0];
      bytecode_[jump_location + 1] = raw_delta[1];
      bytecode_[jump_location + 2] = raw_delta[2];
      bytecode_[jump_location + 3] = raw_delta[3];
    }
  }

  label->BindTo(curr_offset);
}

void BytecodeEmitter::EmitJump(BytecodeLabel *label) {
  static const i32 kJumpPlaceholder = std::numeric_limits<i32>::max() - 1;

  std::size_t curr_offset = position();

  if (label->is_bound()) {
    // The label is already bound so this must be a backwards jump. We just need
    // to emit the delta offset directly into the bytestream.
    TPL_ASSERT(
        label->offset() <= curr_offset,
        "Label for backwards jump cannot be beyond current bytecode position");
    std::size_t delta = curr_offset - label->offset();
    TPL_ASSERT(delta < std::numeric_limits<i32>::max(),
               "Jump delta exceeds 32-bit value for jump offsets!");

    // Immediately emit the delta
    EmitScalarValue(-static_cast<i32>(delta));
  } else {
    // The label is not bound yet so this must be a forward jump. We set the
    // reference position in the label and use a placeholder offset in the
    // byte stream for now. We'll update the placeholder when the label is bound
    label->set_referrer(curr_offset);
    EmitScalarValue(kJumpPlaceholder);
  }
}

void BytecodeEmitter::EmitJump(Bytecode bytecode, BytecodeLabel *label) {
  TPL_ASSERT(Bytecodes::IsJump(bytecode), "Provided bytecode is not a jump");
  EmitAll(bytecode);
  EmitJump(label);
}

void BytecodeEmitter::EmitConditionalJump(Bytecode bytecode, LocalVar cond,
                                          BytecodeLabel *label) {
  TPL_ASSERT(Bytecodes::IsJump(bytecode), "Provided bytecode is not a jump");
  EmitAll(bytecode, cond);
  EmitJump(label);
}

void BytecodeEmitter::Emit(Bytecode bytecode, LocalVar operand_1) {
  EmitAll(bytecode, operand_1);
}

void BytecodeEmitter::Emit(Bytecode bytecode, LocalVar operand_1,
                           LocalVar operand_2) {
  EmitAll(bytecode, operand_1, operand_2);
}

void BytecodeEmitter::Emit(Bytecode bytecode, LocalVar operand_1,
                           LocalVar operand_2, LocalVar operand_3) {
  EmitAll(bytecode, operand_1, operand_2, operand_3);
}

void BytecodeEmitter::Emit(Bytecode bytecode, LocalVar operand_1,
                           LocalVar operand_2, LocalVar operand_3,
                           LocalVar operand_4) {
  EmitAll(bytecode, operand_1, operand_2, operand_3, operand_4);
}

void BytecodeEmitter::Emit(Bytecode bytecode, LocalVar operand_1,
                           LocalVar operand_2, LocalVar operand_3,
                           LocalVar operand_4, LocalVar operand_5) {
  EmitAll(bytecode, operand_1, operand_2, operand_3, operand_4, operand_5);
}

void BytecodeEmitter::Emit(Bytecode bytecode, LocalVar operand_1,
                           LocalVar operand_2, LocalVar operand_3,
                           LocalVar operand_4, LocalVar operand_5,
                           LocalVar operand_6) {
  EmitAll(bytecode, operand_1, operand_2, operand_3, operand_4, operand_5,
          operand_6);
}

void BytecodeEmitter::Emit(Bytecode bytecode, LocalVar operand_1,
                           LocalVar operand_2, LocalVar operand_3,
                           LocalVar operand_4, LocalVar operand_5,
                           LocalVar operand_6, LocalVar operand_7) {
  EmitAll(bytecode, operand_1, operand_2, operand_3, operand_4, operand_5,
          operand_6, operand_7);
}

void BytecodeEmitter::Emit(Bytecode bytecode, LocalVar operand_1,
                           LocalVar operand_2, LocalVar operand_3,
                           LocalVar operand_4, LocalVar operand_5,
                           LocalVar operand_6, LocalVar operand_7,
                           LocalVar operand_8) {
  EmitAll(bytecode, operand_1, operand_2, operand_3, operand_4, operand_5,
          operand_6, operand_7, operand_8);
}

void BytecodeEmitter::EmitTableIteratorInit(Bytecode bytecode, LocalVar iter,
                                            u16 table_id) {
  EmitAll(bytecode, iter, table_id);
}

void BytecodeEmitter::EmitVPIGet(Bytecode bytecode, LocalVar out, LocalVar vpi,
                                 u32 col_idx) {
  EmitAll(bytecode, out, vpi, col_idx);
}

void BytecodeEmitter::EmitVPIVectorFilter(Bytecode bytecode, LocalVar selected,
                                          LocalVar iter, u32 col_idx, i64 val) {
  EmitAll(bytecode, selected, iter, col_idx, val);
}

}  // namespace tpl::vm