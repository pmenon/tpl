#pragma once

#include <functional>
#include <iosfwd>

#include "logging/logger.h"
#include "util/region_containers.h"
#include "vm/bytecode_function_info.h"
#include "vm/bytecode_iterator.h"
#include "vm/llvm_engine.h"
#include "vm/vm.h"

namespace tpl::vm {

/// An enumeration capturing different execution methods and optimization levels
enum class ExecutionMode : u8 {
  Interpret = 0,
  InterpretOpt = 1,
  Jit = 2,
  JitOpt = 3
};

/// A module represents all code in a single TPL source file
class BytecodeModule {
 public:
  BytecodeModule(std::string name, util::RegionVector<u8> &&code,
                 util::RegionVector<FunctionInfo> &&functions)
      : name_(std::move(name)),
        code_(std::move(code)),
        functions_(std::move(functions)) {}

  /// This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(BytecodeModule);

  /// Look up a TPL function in this module by its ID
  /// \param func_id The ID of the function
  /// \return The TPL function if one exists with the ID; otherwise return null
  const FunctionInfo *GetFuncInfoById(FunctionId func_id) const {
    TPL_ASSERT(func_id < NumFunctions(), "Invalid function");
    return &functions_[func_id];
  }

  /// Look up a TPL function in this module by its name
  /// \param name The name of the function
  /// \return The TPL function if it exists; otherwise return null
  const FunctionInfo *GetFuncInfoByName(const std::string &name) const {
    for (const auto &func : functions_) {
      if (func.name() == name) return &func;
    }
    return nullptr;
  }

  /// Retrieve an iterator over the bytecode for a given function
  /// \param func The function whose bytecode to retrieve
  /// \return An iterator over the bytecode
  BytecodeIterator BytecodeForFunction(const FunctionInfo &func) const {
    TPL_ASSERT(GetFuncInfoById(func.id()) != nullptr,
               "Function not defined in unit!");
    auto [start, end] = func.bytecode_range();
    return BytecodeIterator(code_, start, end);
  }

  /// Retrieve and wrap a TPL function inside a C++ function object, thus making
  /// the TPL function callable as a C++ function. Callers can request different
  /// versions of the TPL code including an interpreted version and a compiled
  /// version.
  /// \tparam Ret Ret The C/C++ return type of the function
  /// \tparam ArgTypes ArgTypes The C/C++ argument types to the function
  /// \param name The name of the function the caller wants
  /// \param exec_mode The interpretation mode the caller desires
  /// \param func[output] The function wrapper we use to wrap the TPL function
  /// \return True if the function was found and the output parameter was set
  template <typename Ret, typename... ArgTypes>
  bool GetFunction(const std::string &name, ExecutionMode exec_mode,
                   std::function<Ret(ArgTypes...)> &func) const;

  /// Pretty print all the module's contents into the provided output stream
  /// \param os The stream into which we dump the module's contents
  void PrettyPrint(std::ostream &os);

  /// How many instructions are in this module?
  /// \return The total number of bytecode instructions in the module
  std::size_t InstructionCount() const { return code_.size(); }

  /// How many functions are defined in this module?
  /// \return The number of functions in the module
  std::size_t NumFunctions() const { return functions_.size(); }

 private:
  friend class VM;
  friend class LLVMEngine;

  const u8 *GetBytecodeForFunction(const FunctionInfo &func) const {
    auto [start, _] = func.bytecode_range();
    return &code_[start];
  }

  // -------------------------------------------------------
  // Accessors
  // -------------------------------------------------------

  const std::string &name() const { return name_; }

  const util::RegionVector<u8> code() const { return code_; }

  const util::RegionVector<FunctionInfo> &functions() const {
    return functions_;
  }

 private:
  const std::string name_;
  util::RegionVector<u8> code_;
  util::RegionVector<FunctionInfo> functions_;
};

//----------------------------------------------------------
// Implementation below
//----------------------------------------------------------

template <typename RetT, typename... ArgTypes>
inline bool BytecodeModule::GetFunction(
    const std::string &name, ExecutionMode exec_mode,
    std::function<RetT(ArgTypes...)> &func) const {
  const FunctionInfo *func_info = GetFuncInfoByName(name);

  // Check valid function
  if (func_info == nullptr) {
    return false;
  }

  // Verify argument counts
  constexpr u32 num_params = sizeof...(ArgTypes) + !std::is_void_v<RetT>;
  if (num_params != func_info->num_params()) {
    return false;
  }

  switch (exec_mode) {
    case ExecutionMode::Interpret: {
      func = [this, func_info](ArgTypes... args) {
        // Create allocator for execution
        util::Region region(func_info->name() + "-exec-region");

        // The virtual machine
        VM vm(&region, *this);

        // Let's go!
        const u8 *ip = GetBytecodeForFunction(*func_info);
        if constexpr (std::is_void_v<RetT>) {
          vm.Execute(*func_info, ip, args...);
          return;
        } else {
          RetT rv{};
          vm.Execute(*func_info, ip, &rv, args...);
          return rv;
        }
      };
      return true;
    }
    case ExecutionMode::Jit: {
      func = [this, func_info](ArgTypes... args) {
        // JIT the module
        auto compiled = LLVMEngine::Compile(*this);

        void *raw_f = compiled->GetFunctionPointer(func_info->name());
        TPL_ASSERT(raw_f != nullptr, "No function");

        // Let's go!
        if constexpr (std::is_void_v<RetT>) {
          auto *jit_f = reinterpret_cast<void (*)(ArgTypes...)>(raw_f);
          jit_f(args...);
          return;
        } else {
          auto *jit_f = reinterpret_cast<void (*)(RetT *, ArgTypes...)>(raw_f);
          RetT rv{};
          jit_f(&rv, args...);
          return rv;
        }
      };
      return true;
    }
    default: {
      LOG_ERROR("Non-basic-interpreter-mode not supported yet");
      return false;
    }
  }
}

}  // namespace tpl::vm