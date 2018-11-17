#include "vm/bytecode_iterator.h"

#include "vm/bytecode_traits.h"

namespace tpl::vm {

BytecodeIterator::BytecodeIterator(const util::RegionVector<u8> &bytecode)
    : BytecodeIterator(bytecode, 0, bytecode.size()) {}

BytecodeIterator::BytecodeIterator(const util::RegionVector<u8> &bytecode,
                                   std::size_t start, std::size_t end)
    : bytecodes_(bytecode),
      start_offset_(start),
      end_offset_(end),
      curr_offset_(start) {}

Bytecode BytecodeIterator::current_bytecode() const {
  auto raw_code = *reinterpret_cast<const std::underlying_type_t<Bytecode> *>(
      &bytecodes_[current_offset()]);
  return Bytecodes::FromByte(raw_code);
}

void BytecodeIterator::Advance() {
  if (Done()) {
    return;
  }

  auto bytecode = current_bytecode();

  u32 offset = 0;
  if (Bytecodes::IsCall(bytecode)) {
    // The parameter count is the second argument to the call instruction
    auto num_param_offset = Bytecodes::GetNthOperandOffset(bytecode, 1);
    auto num_params = *reinterpret_cast<const u16 *>(
        &bytecodes_[current_offset() + num_param_offset]);
    offset = (num_params * OperandTypeTraits<OperandType::Local>::kSize);
  }

  curr_offset_ += Bytecodes::Size(bytecode) + offset;
}

bool BytecodeIterator::Done() const { return current_offset() >= end_offset(); }

}  // namespace tpl::vm