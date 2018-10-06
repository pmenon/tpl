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

  const FunctionInfo *GetFunctionById(FunctionId func_id) const {
    for (const auto &func : functions_) {
      if (func.id() == func_id) return &func;
    }
    return nullptr;
  }

  const FunctionInfo *GetFunctionByName(const std::string &name) const {
    for (const auto &func : functions_) {
      if (func.name() == name) return &func;
    }
    return nullptr;
  }

  BytecodeIterator BytecodeForFunction(const FunctionInfo &func) const {
    TPL_ASSERT(IsFunctionDefinedInUnit(func), "Function not defined in unit!");
    return BytecodeIterator(code_, func.bytecode_start_offset(),
                            func.bytecode_end_offset());
  }

  void PrettyPrint(std::ostream &os);

 public:
  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Accessors
  ///
  //////////////////////////////////////////////////////////////////////////////

  std::size_t instruction_count() const { return code_.size(); }

  std::size_t num_functions() const { return functions_.size(); }

  const std::vector<FunctionInfo> &functions() const { return functions_; }

 private:
  friend class VM;
  const u8 *GetBytecodeForFunction(const FunctionInfo &func) const {
    TPL_ASSERT(IsFunctionDefinedInUnit(func), "Function not defined in unit!");
    return &code_[func.bytecode_start_offset()];
  }

  bool IsFunctionDefinedInUnit(const FunctionInfo &func) const {
    return GetFunctionById(func.id()) != nullptr;
  }

 private:
  BytecodeUnit(std::vector<u8> code, std::vector<FunctionInfo> functions)
      : code_(std::move(code)), functions_(std::move(functions)) {}

 private:
  std::vector<u8> code_;
  std::vector<FunctionInfo> functions_;
};

}  // namespace tpl::vm