#pragma once

#include <algorithm>
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
   * @param code The code section containing all bytecode instructions.
   * @param data The data section containing static data.
   * @param functions The functions within the module
   * @param static_locals All statically allocated variables in the data section.
   */
  BytecodeModule(std::string name, std::vector<uint8_t> &&code, std::vector<uint8_t> &&data,
                 std::vector<FunctionInfo> &&functions, std::vector<LocalInfo> &&static_locals);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(BytecodeModule);

  /**
   * Look up a TPL function in this module by its ID.
   * @return A pointer to the function's info if it exists; null otherwise.
   */
  const FunctionInfo *GetFuncInfoById(const FunctionId func_id) const {
    // Function IDs are dense, so the given ID must be in the range [0, # functions)
    TPL_ASSERT(func_id < GetFunctionCount(), "Invalid function");
    return &functions_[func_id];
  }

  /**
   * Lookup and retrieve the metadata for a TPL function whose name is @em name.
   * @param name The name of the function to lookup.
   * @return A pointer to the function's info if it exists; NULL otherwise.
   */
  const FunctionInfo *LookupFuncInfoByName(const std::string &name) const {
    const auto iter =
        std::find_if(functions_.begin(), functions_.end(),
                     [&](const FunctionInfo &info) { return info.GetName() == name; });
    return iter == functions_.end() ? nullptr : &*iter;
  }

  /**
   * Lookup and retrieve the metadata for a static local whose offset is @em offset into the data
   * section of this module.
   * @param offset The offset of the static local in bytes in the data section.
   * @return The metadata for the static if one exists at the offset; NULL otherwise.
   */
  const LocalInfo *LookupStaticInfoByOffset(const uint32_t offset) const {
    const auto iter =
        std::find_if(static_locals_.begin(), static_locals_.end(),
                     [&](const LocalInfo &info) { return info.GetOffset() == offset; });
    return iter == static_locals_.end() ? nullptr : &*iter;
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
   * @return The name of the module.
   */
  const std::string &GetName() const { return name_; }

  /**
   * @return A const-view of the metadata for all functions in this module.
   */
  const std::vector<FunctionInfo> &GetFunctions() const { return functions_; }

  /**
   * @return A const-view of the metadata for all static-locals in this module.
   */
  const std::vector<LocalInfo> &GetStaticLocals() const noexcept { return static_locals_; }

  /**
   * @return The number of functions defined in this module.
   */
  std::size_t GetFunctionCount() const { return functions_.size(); }

  /**
   * @return The number of static locals.
   */
  uint32_t GetStaticLocalsCount() const noexcept { return static_locals_.size(); }

  /**
   * Pretty print all the module's contents into the provided output stream.
   * @param os The stream into which we dump the module's contents.
   */
  void Dump(std::ostream &os) const;

 private:
  friend class VM;
  friend class LLVMEngine;

  const uint8_t *AccessBytecodeForFunctionRaw(const FunctionInfo &func) const {
    TPL_ASSERT(GetFuncInfoById(func.GetId()) == &func, "Function not in module!");
    auto [start, _] = func.GetBytecodeRange();
    (void)_;
    return &code_[start];
  }

  // Access a const-view of some static-local's data by its offset
  const uint8_t *AccessStaticLocalDataRaw(const uint32_t offset) const {
#ifndef NDEBUG
    TPL_ASSERT(offset < data_.size(), "Invalid local offset");
    UNUSED auto iter =
        std::find_if(static_locals_.begin(), static_locals_.end(),
                     [&](const LocalInfo &info) { return info.GetOffset() == offset; });
    TPL_ASSERT(iter != static_locals_.end(), "No local at given offset");
#endif
    return &data_[offset];
  }

  // Access a const-view of a static-local's data
  const uint8_t *AccessStaticLocalDataRaw(const LocalVar local) const {
    return AccessStaticLocalDataRaw(local.GetOffset());
  }

 private:
  const std::string name_;
  const std::vector<uint8_t> code_;
  const std::vector<uint8_t> data_;
  const std::vector<FunctionInfo> functions_;
  const std::vector<LocalInfo> static_locals_;
};

}  // namespace tpl::vm
