#pragma once

#include <iosfwd>
#include <string>
#include <vector>

#include "vm/bytecode_function_info.h"
#include "vm/bytecode_iterator.h"
#include "vm/vm.h"

namespace tpl::vm {

/**
 * A bytecode module is a container for all the TPL bytecode (TBC) for a TPL source file. Bytecode
 * modules directly contain a list of all the physical bytecode that make up the program, and a list
 * of functions that store information about the functions in the TPL program.
 */
class BytecodeModule {
 public:
  /**
   * Construct a new bytecode module. After construction, all available bytecode functions are
   * available for execution.
   * @param name The name of the module
   * @param code The bytecode that makes up the module
   * @param functions The functions within the module
   */
  BytecodeModule(std::string name, std::vector<uint8_t> &&code,
                 std::vector<FunctionInfo> &&functions);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(BytecodeModule);

  /**
   * Look up a TPL function in this module by its ID.
   * @return A pointer to the function's info if it exists; null otherwise.
   */
  const FunctionInfo *GetFuncInfoById(const FunctionId func_id) const {
    TPL_ASSERT(func_id < GetFunctionCount(), "Invalid function");
    return &functions_[func_id];
  }

  /**
   * Look up a TPL function in this module by its name
   * @param name The name of the function to lookup
   * @return A pointer to the function's info if it exists; null otherwise
   */
  const FunctionInfo *GetFuncInfoByName(const std::string &name) const {
    for (const auto &func : functions_) {
      if (func.GetName() == name) {
        return &func;
      }
    }
    return nullptr;
  }

  /**
   * @return An iterator over the bytecode for the function @em func.
   */
  BytecodeIterator BytecodeForFunction(const FunctionInfo &func) const {
    auto [start, end] = func.GetBytecodeRange();
    return BytecodeIterator(code_, start, end);
  }

  /**
   * @return The number of bytecode instructions in this module.
   */
  std::size_t GetInstructionCount() const;

  /**
   * Pretty print all the module's contents into the provided output stream
   * @param os The stream into which we dump the module's contents
   */
  void Dump(std::ostream &os) const;

  /**
   * @return The name of the module.
   */
  const std::string &GetName() const { return name_; }

  /**
   * @return A const-view of the metadata for all functions in this module.
   */
  const std::vector<FunctionInfo> &GetFunctions() const { return functions_; }

  /**
   * @return The number of functions defined in this module.
   */
  std::size_t GetFunctionCount() const { return functions_.size(); }

 private:
  friend class VM;

  const uint8_t *GetBytecodeForFunction(const FunctionInfo &func) const {
    auto [start, _] = func.GetBytecodeRange();
    (void)_;
    return &code_[start];
  }

 private:
  const std::string name_;
  const std::vector<uint8_t> code_;
  const std::vector<FunctionInfo> functions_;
};

}  // namespace tpl::vm
