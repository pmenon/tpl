#pragma once

#include <vector>

#include "util/common.h"
#include "vm/bytecodes.h"

namespace tpl::vm {

class BytecodeIterator {
 public:
  explicit BytecodeIterator(const std::vector<u8> &bytecode);
  BytecodeIterator(const std::vector<u8> &bytecode, std::size_t start,
                   std::size_t end);

  Bytecode current_bytecode() const;
  std::size_t current_offset() const { return bytecode_offset_; }
  const std::vector<u8> &bytecodes() const { return bytecodes_; }

  void Advance();
  bool Done() const;

 private:
  const std::vector<u8> &bytecodes_;
  std::size_t start_offset_;
  std::size_t end_offset_;
  std::size_t bytecode_offset_;
};

}  // namespace tpl::vm