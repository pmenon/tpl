#pragma once

#include "util/common.h"
#include "util/region_containers.h"
#include "vm/bytecodes.h"

namespace tpl::vm {

class BytecodeIterator {
 public:
  explicit BytecodeIterator(const util::RegionVector<u8> &bytecode);
  BytecodeIterator(const util::RegionVector<u8> &bytecode, std::size_t start,
                   std::size_t end);

  Bytecode current_bytecode() const;
  std::size_t start_offset() const { return start_offset_; }
  std::size_t current_offset() const { return curr_offset_; }
  std::size_t end_offset() const { return end_offset_; }
  const util::RegionVector<u8> &bytecodes() const { return bytecodes_; }

  void Advance();
  bool Done() const;

 private:
  const util::RegionVector<u8> &bytecodes_;
  std::size_t start_offset_;
  std::size_t end_offset_;
  std::size_t curr_offset_;
};

}  // namespace tpl::vm