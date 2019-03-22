#pragma once

#include <functional>
#include <iosfwd>
#include <vector>

#include "llvm/Support/Memory.h"

#include "logging/logger.h"
#include "util/memory.h"
#include "vm/bytecode_function_info.h"
#include "vm/bytecode_iterator.h"
#include "vm/llvm_engine.h"
#include "vm/vm.h"

namespace tpl::vm {

/// An enumeration capturing different execution methods and optimization levels
enum class ExecutionMode : u8 { Interpret, Jit };

/// A module represents all code in a single TPL source file
class BytecodeModule {
 public:
  /// Construct
  BytecodeModule(std::string name, std::vector<u8> &&code,
                 std::vector<FunctionInfo> &&functions);

  /// This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(BytecodeModule);

  /// Look up a TPL function in this module by its ID
  /// \return A pointer to the function's info if it exists; null otherwise
  const FunctionInfo *GetFuncInfoById(FunctionId func_id) const;

  /// Look up a TPL function in this module by its name
  /// \return A pointer to the function's info if it exists; null otherwise
  const FunctionInfo *GetFuncInfoByName(const std::string &name) const;

  /// Retrieve an iterator over the bytecode for the given function \a func
  /// \return A pointer to the function's info if it exists; null otherwise
  BytecodeIterator BytecodeForFunction(const FunctionInfo &func) const;

  /// Get the trampoline for the bytecode function with id \a func_id
  /// \return An opaque function pointer to the bytecode function
  void *GetFuncTrampoline(FunctionId func_id) const;

  /// Retrieve and wrap a TPL function inside a C++ function object, thus making
  /// the TPL function callable as a C++ function. Callers can request different
  /// versions of the TPL code including an interpreted version and a compiled
  /// version.
  /// \tparam Ret Ret The C/C++ return type of the function
  /// \tparam ArgTypes ArgTypes The C/C++ argument types to the function
  /// \param name The name of the function the caller wants
  /// \param exec_mode The interpretation mode the caller desires
  /// \param[out] func The function wrapper we use to wrap the TPL function
  /// \return True if the function was found and the output parameter was set
  template <typename Ret, typename... ArgTypes>
  bool GetFunction(const std::string &name, ExecutionMode exec_mode,
                   std::function<Ret(ArgTypes...)> &func) const;

  /// Pretty print all the module's contents into the provided output stream
  /// \param os The stream into which we dump the module's contents
  void PrettyPrint(std::ostream &os);

  // -------------------------------------------------------
  // Accessors
  // -------------------------------------------------------

  /// Return the name of the module
  const std::string &name() const noexcept { return name_; }

  /// Return a constant view of all functions
  const std::vector<FunctionInfo> &functions() const { return functions_; }

  /// Return the number of bytecode instructions in this module
  std::size_t instruction_count() const { return code_.size(); }

  /// Return the number of functions defined in this module
  std::size_t num_functions() const { return functions_.size(); }

 private:
  friend class VM;
  friend class LLVMEngine;

  const u8 *GetBytecodeForFunction(const FunctionInfo &func) const {
    auto [start, _] = func.bytecode_range();
    (void)_;
    return &code_[start];
  }

  /// Create a trampoline function for the function with id \a func_id
  class Trampoline;
  void CreateFunctionTrampoline(FunctionId func_id);
  void CreateFunctionTrampoline(const FunctionInfo &func,
                                Trampoline &trampoline);

 private:
  /// A trampoline is a stub function that all calls into TPL code go through
  /// to set up call arguments.
  class Trampoline {
   public:
    /// Create an empty/uninitialized trampoline
    Trampoline() noexcept : mem_() {}

    /// Create a trampoline over the given memory block
    explicit Trampoline(llvm::sys::OwningMemoryBlock &&mem) noexcept
        : mem_(std::move(mem)) {}

    /// Move assignment
    Trampoline &operator=(Trampoline &&other) noexcept {
      mem_ = std::move(other.mem_);
      return *this;
    }

    /// Access the trampoline code
    void *GetCode() const { return mem_.base(); }

   private:
    // Memory region where the trampoline's code is
    llvm::sys::OwningMemoryBlock mem_;
  };

 private:
  const std::string name_;
  const std::vector<u8> code_;
  const std::vector<FunctionInfo> functions_;
  std::vector<Trampoline> trampolines_;
};

//----------------------------------------------------------
// Implementation below
//----------------------------------------------------------

inline const FunctionInfo *BytecodeModule::GetFuncInfoById(
    const FunctionId func_id) const {
  TPL_ASSERT(func_id < num_functions(), "Invalid function");
  return &functions_[func_id];
}

inline const FunctionInfo *BytecodeModule::GetFuncInfoByName(
    const std::string &name) const {
  for (const auto &func : functions_) {
    if (func.name() == name) {
      return &func;
    }
  }
  return nullptr;
}

inline BytecodeIterator BytecodeModule::BytecodeForFunction(
    const FunctionInfo &func) const {
  TPL_ASSERT(GetFuncInfoById(func.id()) != nullptr,
             "Function not defined in unit!");
  auto [start, end] = func.bytecode_range();
  return BytecodeIterator(code_, start, end);
}

inline void *BytecodeModule::GetFuncTrampoline(const FunctionId func_id) const {
  return trampolines_[func_id].GetCode();
}

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
      func = [this, func_info](ArgTypes... args) -> RetT {
        // The virtual machine
        VM vm(*this);
        // Let's go
        if constexpr (std::is_void_v<RetT>) {
          // The buffer we copy the arguments into
          u8 arg_buffer[(0ul + ... + sizeof(args))];
          util::CopyAll(arg_buffer, std::forward(args)...);
          // Invoke the function
          vm.InvokeFunction(func_info->id(), arg_buffer);
          // Finish
          return;
        } else {
          // The buffer we copy the arguments into, including the return value
          u8 arg_buffer[sizeof(RetT *) + (0ul + ... + sizeof(args))];
          // The return value
          RetT rv{};
          // Copy the function arguments
          util::CopyAll(arg_buffer, &rv, std::forward(args)...);
          // Invoke the function
          vm.InvokeFunction(func_info->id(), arg_buffer);
          // Finish
          return rv;
        }
      };
      return true;
    }
    case ExecutionMode::Jit: {
      func = [this, func_info](ArgTypes... args) -> RetT {
        // JIT the module
        auto compiled = LLVMEngine::Compile(*this);
        // Pull out the function
        void *raw_fn = compiled->GetFunctionPointer(func_info->name());
        TPL_ASSERT(raw_fn != nullptr, "No function");
        // Let's go!
        if constexpr (std::is_void_v<RetT>) {
          auto *jit_f = reinterpret_cast<void (*)(ArgTypes...)>(raw_fn);
          jit_f(args...);
          return;
        } else {
          auto *jit_f = reinterpret_cast<void (*)(RetT *, ArgTypes...)>(raw_fn);
          RetT rv{};
          jit_f(&rv, args...);
          return rv;
        }
      };
      return true;
    }
  }

  UNREACHABLE("Impossible");
  return false;
}

}  // namespace tpl::vm
