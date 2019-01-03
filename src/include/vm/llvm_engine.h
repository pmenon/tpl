#pragma once

#include <memory>

#include "llvm/Support/MemoryBuffer.h"

#include "util/common.h"
#include "util/macros.h"
#include "vm/bytecodes.h"

namespace tpl::ast {
class Type;
}  // namespace tpl::ast

namespace tpl::vm {

class BytecodeModule;
class FunctionInfo;
class LocalVar;

/// The interface to LLVM to JIT compile TPL bytecode
class LLVMEngine {
 public:
  // -------------------------------------------------------
  // Helper classes
  // -------------------------------------------------------

  class TPLMemoryManager;
  class TypeMap;
  class FunctionHelper;
  class CompilerOptions;
  class CompiledModule;
  class CompiledModuleBuilder;

  // -------------------------------------------------------
  // Public API
  // -------------------------------------------------------

  /// Initialize the whole LLVM subsystem
  static void Initialize();

  /// Shutdown the whole LLVM subsystem
  static void Shutdown();

  /// JIT compile a TPL bytecode module to native code
  /// \param module The module to compile
  /// \return The JIT compiled module
  static std::unique_ptr<CompiledModule> Compile(
      const vm::BytecodeModule &module, const CompilerOptions &options = {});

  // -------------------------------------------------------
  // Compiler Options
  // -------------------------------------------------------

  /// Options to provide when compiling
  class CompilerOptions {
   public:
    CompilerOptions()
        : debug_(false), write_obj_file_(false), output_file_name_() {}

    CompilerOptions &SetDebug(bool debug) {
      debug_ = debug;
      return *this;
    }

    bool IsDebug() const { return debug_; }

    CompilerOptions &SetPersistObjectFile(bool write_obj_file) {
      write_obj_file_ = write_obj_file;
      return *this;
    }

    bool ShouldPersistObjectFile() const { return write_obj_file_; }

    CompilerOptions &SetOutputObjectFileName(const std::string &name) {
      output_file_name_ = name;
      return *this;
    }

    const std::string &GetOutputObjectFileName() const {
      return output_file_name_;
    }

    std::string GetBytecodeHandlersBcPath() const {
      return "./lib/bytecode_handlers_ir.bc";
    }

   private:
    bool debug_;
    bool write_obj_file_;
    std::string output_file_name_;
  };

  // -------------------------------------------------------
  // Compiled Module
  // -------------------------------------------------------

  /// A compiled module corresponds to a single TPL bytecode module that has
  /// been JIT compiled into native code.
  class CompiledModule {
   public:
    explicit CompiledModule(std::unique_ptr<llvm::MemoryBuffer> object_code);

    /// No copying or moving this class
    DISALLOW_COPY_AND_MOVE(CompiledModule);

    ~CompiledModule();

    /// Obtain a raw function pointer to a JITted function in this module
    /// \param name The name of the function
    /// \return A raw function pointer if a function with the name exists. If no
    /// function with the provided name exists, this will return null
    void *GetFunctionPointer(const std::string &name) const;

    /// Load this module into memory
    /// \param module
    void Load(const BytecodeModule &module);

   private:
    bool loaded() const { return loaded_; }
    void set_loaded(bool loaded) { loaded_ = loaded; }

    llvm::MemoryBuffer *object_code() { return object_code_.get(); }

    TPLMemoryManager *memory_manager() { return memory_manager_.get(); }

   private:
    bool loaded_;
    std::unique_ptr<llvm::MemoryBuffer> object_code_;
    std::unique_ptr<TPLMemoryManager> memory_manager_;
    std::unordered_map<std::string, void *> functions_;
  };
};

}  // namespace tpl::vm