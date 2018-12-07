#pragma once

#include <memory>

#include "llvm/ADT/DenseMap.h"

namespace llvm {
class LLVMContext;
class Module;
class Type;
}  // namespace llvm

namespace tpl::ast {
class Type;
}  // namespace tpl::ast

namespace tpl::vm {

class BytecodeModule;

/// The interface to LLVM to perform JIT of TPL bytecode
class LLVMEngine {
 public:
  /// Initialize the whole LLVM subsystem
  static void Initialize();

  /// Shutdown the whole LLVM subsystem
  static void Shutdown();

  class CompileOptions {
   public:
    std::string GetBytecodeHandlersBcPath() const;
  };

  /// A compilation unit is another word for a translation unit in C/C++
  /// parlance. It represents all the code in a single source file. Compilation
  /// units
  class CompilationUnit {
   public:
    explicit CompilationUnit(llvm::Module *module);

    void *GetFunctionPointer(const std::string &name) const;

   private:
    llvm::Module *module() { return module_; }

   private:
    llvm::Module *module_;
  };

  /// A builder to create a compilation unit. We need this because a compilation
  /// unit is immutable after creation.
  class CompilationUnitBuilder {
   public:
    explicit CompilationUnitBuilder(const CompileOptions &options,
                                    vm::BytecodeModule *tpl_module);

    /// Create function declarations for all functions declared in the TPL
    /// bytecode module
    /// \param bytecode_module The bytecode module
    void DeclareFunctions();

    /// Generate LLVM function implementions for all functions defined in the
    /// TPL bytecode module
    /// \param bytecode_module The bytecode module
    void DefineFunctions();

    /// Verify that all generated code is good
    void Verify();

    /// Clean up the code
    void Clean();

    /// Optimize the generate code
    void Optimize();

    /// Perform finalization logic create a compilation unit
    /// \return A compilation unit housing all LLVM bitcode for the module
    std::unique_ptr<CompilationUnit> Finalize();

    /// Print the contents of the module to a string and return it
    /// \return Stringidifed module contents
    std::string PrettyPrintLLVMModule() const;

   private:
    llvm::Type *GetLLVMType(const ast::Type *type);

    // -----------------------------------------------------
    // Accessors
    // -----------------------------------------------------

    vm::BytecodeModule *tpl_module() { return tpl_module_; }

    llvm::LLVMContext &context() { return *context_; }

    llvm::Module *module() { return llvm_module_.get(); }
    const llvm::Module *module() const { return llvm_module_.get(); }

   private:
    vm::BytecodeModule *tpl_module_;
    std::unique_ptr<llvm::LLVMContext> context_;
    std::unique_ptr<llvm::Module> llvm_module_;
    llvm::DenseMap<const ast::Type *, llvm::Type *> type_map_;
  };

  /// Compile a TPL bytecode module into an LLVM compilation unit
  /// \param module The module to compile
  /// \return The JIT compiled module
  std::unique_ptr<CompilationUnit> Compile(vm::BytecodeModule *module);
};

}  // namespace tpl::vm