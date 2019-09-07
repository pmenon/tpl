#pragma once

#include <iosfwd>
#include <string>
#include <vector>

#include "vm/bytecode_function_info.h"
#include "vm/bytecode_iterator.h"
#include "vm/vm.h"

namespace tpl::vm {

/**
 * A bytecode module is a container for all the TPL bytecode (TBC) for a TPL
 * source file. Bytecode modules directly contain a list of all the physical
 * bytecode that make up the program, and a list of functions that store
 * information about the functions in the TPL program.
 */
class BytecodeModule {
 public:
  /**
   * Construct a new bytecode module. After construction, all available bytecode
   * functions are available for execution.
   * @param name The name of the module
   * @param code The bytecode that makes up the module
   * @param functions The functions within the module
   */
  BytecodeModule(std::string name, std::vector<uint8_t> &&code,
                 std::vector<FunctionInfo> &&functions);

  /**
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(BytecodeModule);

  /**
   * Look up a TPL function in this module by its ID
   * @return A pointer to the function's info if it exists; null otherwise
   */
  const FunctionInfo *GetFuncInfoById(const FunctionId func_id) const {
    TPL_ASSERT(func_id < num_functions(), "Invalid function");
    return &functions_[func_id];
  }

  /**
   * Look up a TPL function in this module by its name
   * @param name The name of the function to lookup
   * @return A pointer to the function's info if it exists; null otherwise
   */
  const FunctionInfo *GetFuncInfoByName(const std::string &name) const {
    for (const auto &func : functions_) {
      if (func.name() == name) {
        return &func;
      }
    }
    return nullptr;
  }

  /**
   * Retrieve an iterator over the bytecode for the given function \a func
   * @return A pointer to the function's info if it exists; null otherwise
   */
  BytecodeIterator BytecodeForFunction(const FunctionInfo &func) const {
    auto [start, end] = func.bytecode_range();
    return BytecodeIterator(code_, start, end);
  }

  /**
   * Return the number of bytecode instructions in this module.
   */
  std::size_t GetInstructionCount() const {
    std::size_t count = 0;
    for (BytecodeIterator iter(code_); !iter.Done(); iter.Advance()) {
      count++;
    }
    return count;
  }

  /**
   * Pretty print all the module's contents into the provided output stream
   * @param os The stream into which we dump the module's contents
   */
  void PrettyPrint(std::ostream &os) const;

  /**
   * Return the name of the module
   */
  const std::string &name() const { return name_; }

  /**
   * Return a constant view of all functions
   */
  const std::vector<FunctionInfo> &functions() const { return functions_; }

  /**
   * Return the number of functions defined in this module
   */
  std::size_t num_functions() const { return functions_.size(); }

 private:
  friend class VM;

  const uint8_t *GetBytecodeForFunction(const FunctionInfo &func) const {
    auto [start, _] = func.bytecode_range();
    (void)_;
    return &code_[start];
  }

 private:
  const std::string name_;
  const std::vector<uint8_t> code_;
  const std::vector<FunctionInfo> functions_;
};

}  // namespace tpl::vm
