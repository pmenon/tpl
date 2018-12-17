#pragma once

#include <memory>

#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/StringMap.h"

#include "util/common.h"
#include "util/macros.h"
#include "vm/bytecodes.h"

namespace llvm {
class BasicBlock;
class ConstantFolder;
class Function;
template <typename, typename>
class IRBuilder;
class IRBuilderDefaultInserter;
class LLVMContext;
class Module;
class Type;
class Value;
}  // namespace llvm

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
  /// Initialize the whole LLVM subsystem
  static void Initialize();

  /// Shutdown the whole LLVM subsystem
  static void Shutdown();

  class CompilationUnit;

  /// Compile a TPL bytecode module into an LLVM compilation unit
  /// \param module The module to compile
  /// \return The JIT compiled module
  static std::unique_ptr<CompilationUnit> Compile(
      const vm::BytecodeModule &module);

  // -------------------------------------------------------
  // Helper classes below
  // -------------------------------------------------------

  /// A class tracking options used to control compilation
  class CompileOptions {
   public:
    CompileOptions() = default;

    /// No copying or moving this class
    DISALLOW_COPY_AND_MOVE(CompileOptions);

    std::string GetBytecodeHandlersBcPath() const;
  };

  /// A compilation unit is another word for a translation unit in C/C++
  /// parlance. It represents all the code in a single source file. Compilation
  /// units
  class CompilationUnit {
   public:
    explicit CompilationUnit(llvm::Module *module);

    /// No copying or moving this class
    DISALLOW_COPY_AND_MOVE(CompilationUnit);

    void *GetFunctionPointer(const std::string &name) const;

   private:
    llvm::Module *module() { return module_; }

   private:
    llvm::Module *module_;
  };

  /// A handy class that maps TPL types to LLVM types
  class TPLTypeToLLVMTypeMap {
   public:
    explicit TPLTypeToLLVMTypeMap(llvm::Module *module);

    /// No copying or moving this class
    DISALLOW_COPY_AND_MOVE(TPLTypeToLLVMTypeMap);

    llvm::Type *VoidType() { return type_map_["nil"]; }
    llvm::Type *BoolType() { return type_map_["bool"]; }
    llvm::Type *Int8Type() { return type_map_["int8"]; };
    llvm::Type *Int16Type() { return type_map_["int16"]; };
    llvm::Type *Int32Type() { return type_map_["int32"]; };
    llvm::Type *Int64Type() { return type_map_["int64"]; };
    llvm::Type *UInt8Type() { return type_map_["uint8"]; };
    llvm::Type *UInt16Type() { return type_map_["uint16"]; };
    llvm::Type *UInt32Type() { return type_map_["uint32"]; };
    llvm::Type *UInt64Type() { return type_map_["uint64"]; };

    llvm::Type *GetLLVMType(const ast::Type *type);

   private:
    llvm::Module *module() { return module_; }

   private:
    llvm::Module *module_;
    llvm::StringMap<llvm::Type *> type_map_;
  };

  /// A builder to create a compilation unit. We need this because a compilation
  /// unit is immutable after creation.
  class CompilationUnitBuilder {
   public:
    CompilationUnitBuilder(const CompileOptions &options,
                           const vm::BytecodeModule &tpl_module);

    /// No copying or moving this class
    DISALLOW_COPY_AND_MOVE(CompilationUnitBuilder);

    /// Create function declarations for all functions declared in the TPL
    /// bytecode module
    /// \param bytecode_module The bytecode module
    void DeclareFunctions();

    /// Generate LLVM function implementations for all functions defined in the
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
    /// Define the body of the function \ref func_info
    /// \param func_info The function to define
    /// \param ir_builder LLVM's IR builder
    void DefineFunction(
        const FunctionInfo &func_info,
        llvm::IRBuilder<llvm::ConstantFolder, llvm::IRBuilderDefaultInserter>
            &ir_builder);

    /// Lookup the handler function for the given bytecode
    /// \param bytecode The bytecode whose handler to lookup
    /// \return
    llvm::Function *LookupBytecodeHandler(Bytecode bytecode) const;

    // -----------------------------------------------------
    // Accessors
    // -----------------------------------------------------

    const vm::BytecodeModule &tpl_module() const { return tpl_module_; }

    llvm::LLVMContext &context() { return *context_; }

    llvm::Module *module() { return llvm_module_.get(); }

    const llvm::Module *module() const { return llvm_module_.get(); }

    TPLTypeToLLVMTypeMap *type_map() { return type_map_.get(); }

   private:
    const vm::BytecodeModule &tpl_module_;
    std::unique_ptr<llvm::LLVMContext> context_;
    std::unique_ptr<llvm::Module> llvm_module_;
    std::unique_ptr<TPLTypeToLLVMTypeMap> type_map_;
  };

  /// Class that helps construction of an LLVM function from a TPL function
  class LLVMFunctionHelper {
   public:
    LLVMFunctionHelper(
        const FunctionInfo &func_info, llvm::Function *func,
        TPLTypeToLLVMTypeMap *type_map,
        llvm::IRBuilder<llvm::ConstantFolder, llvm::IRBuilderDefaultInserter>
            &ir_builder);

    llvm::Value *GetArgumentById(LocalVar var);

   private:
    llvm::IRBuilder<llvm::ConstantFolder, llvm::IRBuilderDefaultInserter>
        &ir_builder_;
    llvm::DenseMap<u32, llvm::Value *> params_;
    llvm::DenseMap<u32, llvm::Value *> locals_;
  };
};

}  // namespace tpl::vm