#pragma once

#include <iosfwd>
#include <memory>
#include <vector>

#include "vm/bytecode_iterator.h"
#include "vm/bytecode_register.h"

namespace tpl::vm {

class BytecodeUnit {
 public:
  static std::unique_ptr<BytecodeUnit> Create(
      const std::vector<u8> &code, const std::vector<FunctionInfo> &functions) {
    // Can't use std::make_unique() because the constructor is private
    return std::unique_ptr<BytecodeUnit>(new BytecodeUnit(code, functions));
  }

  std::size_t instruction_size() const { return code_.size(); }

  std::size_t num_functions() const { return functions_.size(); }

  const FunctionInfo &GetFunction(FunctionId func_id) const {
    TPL_ASSERT(func_id < num_functions(), "Invalid function ID");
    return functions_[func_id];
  }

  BytecodeIterator BytecodeForFunction(FunctionId func_id) const {
    const auto &func = GetFunction(func_id);
    return BytecodeIterator(code_, func.bytecode_start_offset(),
                            func.bytecode_end_offset());
  }

  void PrettyPrint(std::ostream &os);

 private:
  BytecodeUnit(std::vector<u8> code, std::vector<FunctionInfo> functions)
      : code_(std::move(code)), functions_(std::move(functions)) {}

 private:
  std::vector<u8> code_;
  std::vector<FunctionInfo> functions_;
};

}  // namespace tpl::vm