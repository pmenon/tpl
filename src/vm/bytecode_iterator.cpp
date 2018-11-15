#include "vm/bytecode_iterator.h"

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
  if (!Done()) {
    curr_offset_ += Bytecodes::Size(current_bytecode());
  }
}

bool BytecodeIterator::Done() const { return current_offset() >= end_offset(); }

}  // namespace tpl::vm