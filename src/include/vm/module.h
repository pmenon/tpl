#pragma once

#include <functional>
#include <iosfwd>
#include <memory>

#include "logging/logger.h"
#include "util/region_containers.h"
#include "vm/bytecode_iterator.h"
#include "vm/function_info.h"
#include "vm/vm.h"

namespace tpl::vm {

enum class InterpreterMode : u8 {
  Interpret = 0,
  InterpretOpt = 1,
  Jit = 2,
  JitOpt = 3
};

class Module {
 public:
  Module(util::RegionVector<u8> code,
         util::RegionVector<FunctionInfo> functions)
      : code_(std::move(code)), functions_(std::move(functions)) {}

  DISALLOW_COPY_AND_MOVE(Module);

  /**
   * Retrieve a function's information struct by the ID of the function
   *
   * @param func_id The ID of the function
   * @return The FunctionInfo struct, or NULL if the function does not exist
   */
  const FunctionInfo *GetFuncInfoById(FunctionId func_id) const {
    for (const auto &func : functions_) {
      if (func.id() == func_id) return &func;
    }
    return nullptr;
  }

  /**
   * Retrieve a function's information struct by the function's name
   *
   * @param name The name of the function
   * @return The FunctionInfo struct, or NULL if the function does not exist
   */
  const FunctionInfo *GetFuncInfoByName(const std::string &name) const {
    for (const auto &func : functions_) {
      if (func.name() == name) return &func;
    }
    return nullptr;
  }

  /**
   * Retrieve an iterator over the bytecode for a given function
   *
   * @param func The function whose bytecode to retrieve
   * @return An iterator over the bytecode
   */
  BytecodeIterator BytecodeForFunction(const FunctionInfo &func) const {
    TPL_ASSERT(GetFuncInfoById(func.id()) != nullptr,
               "Function not defined in unit!");
    return BytecodeIterator(code_, func.bytecode_start_offset(),
                            func.bytecode_end_offset());
  }

  /**
   * Get a TPL function as a C++ function.
   *
   * @tparam Ret
   * @tparam ArgTypes
   * @param name
   * @param interp_mode
   * @param func
   */
  template <typename Ret, typename... ArgTypes>
  void GetFunction(const std::string &name, InterpreterMode interp_mode,
                   std::function<Ret(ArgTypes...)> &func) {
    const FunctionInfo *func_info = GetFuncInfoByName(name);
    if (func_info == nullptr) {
      return;
    }

    switch (interp_mode) {
      case InterpreterMode::Interpret: {
        /*
         * In normal interpreter mode, just copy arguments and fire off
         */
        func = [=](ArgTypes...) {
          util::Region region(name + "-exec-region");
          VM::Execute(&region, *this, name);
        };

        break;
      }
      default: {
        /*
         * No other mode is supported
         */
        LOG_ERROR("Non-basic-interpreter-mode not supported yet");
      }
    }
  }

  /**
   * Pretty print all the module's contents into the provided output stream
   *
   * @param os The stream where the module's contents are printed to
   */
  void PrettyPrint(std::ostream &os);

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Accessors
  ///
  //////////////////////////////////////////////////////////////////////////////

  std::size_t instruction_count() const { return code_.size(); }

  std::size_t num_functions() const { return functions_.size(); }

 private:
  friend class VM;

  const util::RegionVector<FunctionInfo> &functions() const {
    return functions_;
  }

  const u8 *GetBytecodeForFunction(const FunctionInfo &func) const {
    TPL_ASSERT(GetFuncInfoById(func.id()) != nullptr,
               "Function not defined in unit!");
    return &code_[func.bytecode_start_offset()];
  }

 private:
  // Private constructor to force users to use Create


 private:
  util::RegionVector<u8> code_;
  util::RegionVector<FunctionInfo> functions_;
};

}  // namespace tpl::vm