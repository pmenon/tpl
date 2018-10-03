#include "vm/bytecode_iterator.h"

namespace tpl::vm {

BytecodeIterator::BytecodeIterator(const std::vector<u8> &bytecode)
    : BytecodeIterator(bytecode, 0, bytecode.size()) {}

BytecodeIterator::BytecodeIterator(const std::vector<u8> &bytecode,
                                   std::size_t start, std::size_t end)
    : bytecodes_(bytecode),
      start_offset_(start),
      end_offset_(end),
      curr_offset_(start) {}

Bytecode BytecodeIterator::current_bytecode() const {
  return Bytecodes::FromByte(bytecodes_[current_offset()]);
}

void BytecodeIterator::Advance() { curr_offset_ += 2; }

bool BytecodeIterator::Done() const { return current_offset() > end_offset(); }

}  // namespace tpl::vm