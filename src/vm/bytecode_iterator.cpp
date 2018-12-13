#include "vm/bytecode_iterator.h"

#include "vm/bytecode_function_info.h"
#include "vm/bytecode_traits.h"

namespace tpl::vm {

BytecodeIterator::BytecodeIterator(const util::RegionVector<u8> &bytecode,
                                   std::size_t start, std::size_t end)
    : bytecodes_(bytecode),
      start_offset_(start),
      end_offset_(end),
      curr_offset_(start) {}

Bytecode BytecodeIterator::CurrentBytecode() const {
  auto raw_code = *reinterpret_cast<const std::underlying_type_t<Bytecode> *>(
      &bytecodes_[current_offset()]);
  return Bytecodes::FromByte(raw_code);
}

void BytecodeIterator::Advance() {
  SetOffset(current_offset() + CurrentBytecodeSize());
}

bool BytecodeIterator::Done() const { return current_offset() >= end_offset(); }

i64 BytecodeIterator::GetImmediateOperand(u32 operand_index) const {
  OperandType operand_type =
      Bytecodes::GetNthOperandType(CurrentBytecode(), operand_index);
  TPL_ASSERT(OperandTypes::IsSignedImmediate(operand_type),
             "Operand type is not a signed immediate");

  const u8 *operand_address =
      GetFirstBytecodeAddress() + current_offset() +
      Bytecodes::GetNthOperandOffset(CurrentBytecode(), operand_index);

  switch (operand_type) {
    case OperandType::Imm1: {
      return *reinterpret_cast<const i8 *>(operand_address);
    }
    case OperandType::Imm2: {
      return *reinterpret_cast<const i16 *>(operand_address);
    }
    case OperandType::Imm4: {
      return *reinterpret_cast<const i32 *>(operand_address);
    }
    case OperandType::Imm8: {
      return *reinterpret_cast<const i64 *>(operand_address);
    }
    default: { UNREACHABLE("Impossible!"); }
  }
}

u64 BytecodeIterator::GetUnsignedImmediateOperand(u32 operand_index) const {
  OperandType operand_type =
      Bytecodes::GetNthOperandType(CurrentBytecode(), operand_index);
  TPL_ASSERT(OperandTypes::IsUnsignedImmediate(operand_type),
             "Operand type is not a signed immediate");

  const u8 *operand_address =
      GetFirstBytecodeAddress() + current_offset() +
      Bytecodes::GetNthOperandOffset(CurrentBytecode(), operand_index);

  switch (operand_type) {
    case OperandType::UImm2: {
      return *reinterpret_cast<const u16 *>(operand_address);
    }
    case OperandType::UImm4: {
      return *reinterpret_cast<const u32 *>(operand_address);
    }
    default: { break; }
  }

  UNREACHABLE("Impossible!");
}

i32 BytecodeIterator::GetJumpOffsetOperand(u32 operand_index) const {
  TPL_ASSERT(Bytecodes::GetNthOperandType(CurrentBytecode(), operand_index) ==
                 OperandType::JumpOffset,
             "Operand isn't a jump offset");
  const u8 *operand_address =
      GetFirstBytecodeAddress() + current_offset() +
      Bytecodes::GetNthOperandOffset(CurrentBytecode(), operand_index);
  return *reinterpret_cast<const i32 *>(operand_address);
}

LocalVar BytecodeIterator::GetLocalOperand(u32 operand_index) const {
  TPL_ASSERT(OperandTypes::IsLocal(Bytecodes::GetNthOperandType(
                 CurrentBytecode(), operand_index)),
             "Operand type is not a local variable reference");
  const u8 *operand_address =
      GetFirstBytecodeAddress() + current_offset() +
      Bytecodes::GetNthOperandOffset(CurrentBytecode(), operand_index);
  auto encoded_val = *reinterpret_cast<const u32 *>(operand_address);
  return LocalVar::Decode(encoded_val);
}

u16 BytecodeIterator::GetLocalCountOperand(u32 operand_index) const {
  TPL_ASSERT(OperandTypes::IsLocalCount(Bytecodes::GetNthOperandType(
                 CurrentBytecode(), operand_index)),
             "Operand type is not a local variable count");

  const u8 *operand_address =
      GetFirstBytecodeAddress() + current_offset() +
      Bytecodes::GetNthOperandOffset(CurrentBytecode(), operand_index);

  return *reinterpret_cast<const u16 *>(operand_address);
}

u32 BytecodeIterator::CurrentBytecodeSize() const {
  Bytecode bytecode = CurrentBytecode();
  u32 size = sizeof(std::underlying_type_t<Bytecode>);
  for (u32 i = 0; i < Bytecodes::NumOperands(bytecode); i++) {
    size += static_cast<u32>(Bytecodes::GetNthOperandSize(bytecode, i));
    if (Bytecodes::GetNthOperandType(bytecode, i) == OperandType::LocalCount) {
      auto num_locals = GetLocalCountOperand(i);
      size += (num_locals + OperandTypeTraits<OperandType::Local>::kSize);
    }
  }
  return size;
}

}  // namespace tpl::vm