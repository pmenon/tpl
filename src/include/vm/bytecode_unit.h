#pragma once

#include <memory>
#include <vector>

#include "vm/bytecode_register.h"

namespace tpl::vm {

class BytecodeUnit {
 public:
  static std::unique_ptr<BytecodeUnit> Create(
      const std::vector<u8> &code,
      const std::vector<FunctionInfo> &functions) {
    // Can't use std::make_unique() because the constructor is private
    return std::unique_ptr<BytecodeUnit>(new BytecodeUnit(code, functions));
  }

 private:
  BytecodeUnit(std::vector<u8> code, std::vector<FunctionInfo> functions)
      : code_(std::move(code)), functions_(std::move(functions)) {}

 private:
  std::vector<u8> code_;
  std::vector<FunctionInfo> functions_;
};

}  // namespace tpl::vm