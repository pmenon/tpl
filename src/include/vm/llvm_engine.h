#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "llvm/Support/MemoryBuffer.h"

#include "common/common.h"
#include "common/macros.h"
#include "vm/bytecodes.h"

namespace tpl::ast {
class Type;
}  // namespace tpl::ast

namespace tpl::vm {

class BytecodeModule;
class FunctionInfo;
class LocalVar;

/**
 * The interface to LLVM to JIT compile TPL bytecode.
 */
class LLVMEngine {
 public:
  // -------------------------------------------------------
  // Helper classes
  // -------------------------------------------------------

  class MCMemoryManager;
  class TypeMap;
  class FunctionLocalsMap;
  class CompilerOptions;
  class CompiledModule;
  class CompiledModuleBuilder;

  // -------------------------------------------------------
  // Public API
  // -------------------------------------------------------

  /**
   * Initialize the whole LLVM subsystem.
   */
  static void Initialize();

  /**
   * Shutdown the whole LLVM subsystem.
   */
  static void Shutdown();

  /**
   * JIT compile a TPL bytecode module to native code.
   *
   * @param module The module to compile.
   * @return The JIT compiled module.
   */
  static std::unique_ptr<CompiledModule> Compile(const BytecodeModule &module,
                                                 const CompilerOptions &options = {});

  // -------------------------------------------------------
  // Compiler Options
  // -------------------------------------------------------

  /**
   * Options to provide when compiling.
   */
  class CompilerOptions {
   public:
    /**
     * Create compiler options with default values.
     */
    CompilerOptions() : debug_(false), write_obj_file_(false), output_file_name_() {}

    /**
     * Set the debug option to the provided value. If debug is true, JIT code will contain debug
     * symbols amenable for use within GDB. This increases binary size and slows compilation, but
     * helps with debugging TPL code. Should not be used in release mode.
     *
     * @param debug Flag indicating whether to add debug symbols.
     * @return The current compiler options.
     */
    CompilerOptions &SetDebug(bool debug) {
      debug_ = debug;
      return *this;
    }

    /**
     * @return True if debug symbols will be generated during compilation; false otherwise.
     */
    bool IsDebug() const { return debug_; }

    /**
     * Set the flag indicating whether the compiled binary should be persisted as a shared object.
     * Shared objects can be linked in at a later time, and is portable across machines. However,
     * this may impact compilation times since the module must be synced to disk before completion.
     *
     * @param write_obj_file Flag indicating whether the compiled code should be persisted to disk.
     * @return The current compiler options.
     */
    CompilerOptions &SetPersistObjectFile(bool write_obj_file) {
      write_obj_file_ = write_obj_file;
      return *this;
    }

    /**
     * @return True if the module will be persisted to disk as a shared object file; false
     * otherwise.
     */
    bool ShouldPersistObjectFile() const { return write_obj_file_; }

    /**
     * If the compiled module will be persisted to disk, use the provided name as the file. If no
     * name is provided, a default name will be used.
     *
     * @param name The name of the object file persist.
     * @return The current compiler options.
     */
    CompilerOptions &SetOutputObjectFileName(const std::string &name) {
      output_file_name_ = name;
      return *this;
    }

    /**
     * @return The name that will be used if the compiled module is persisted as a shared object.
     */
    const std::string &GetOutputObjectFileName() const { return output_file_name_; }

    /**
     * @return The path where the required bytecode handlers is found.
     */
    std::string GetBytecodeHandlersBcPath() const { return "./bytecode_handlers_ir.bc"; }

   private:
    bool debug_;
    bool write_obj_file_;
    std::string output_file_name_;
  };

  // -------------------------------------------------------
  // Compiled Module
  // -------------------------------------------------------

  /**
   * A compiled module corresponds to a single TPL bytecode module that has been JIT compiled into
   * native code.
   */
  class CompiledModule {
   public:
    /**
     * Construct a module without an in-memory module. Users must call Load() to load in a
     * pre-compiled shared object library for this compiled module before this module's functions
     * can be invoked.
     */
    CompiledModule() : CompiledModule(nullptr) {}

    /**
     * Construct a compiled module using the provided shared object file.
     * @param object_code The object file containing code for this module.
     */
    explicit CompiledModule(std::unique_ptr<llvm::MemoryBuffer> object_code);

    /**
     * This class cannot be copied or moved.
     */
    DISALLOW_COPY_AND_MOVE(CompiledModule);

    /**
     * Destructor.
     */
    ~CompiledModule();

    /**
     * @return A pointer to the JIT-ed function in this module with name @em name. If no such
     *         function exists, returns null.
     */
    void *GetFunctionPointer(const std::string &name) const;

    /**
     * @return The size of the module's object code in-memory in bytes.
     */
    std::size_t GetModuleObjectCodeSizeInBytes() const { return object_code_->getBufferSize(); }

    /**
     * Load the given module into memory. If this compiled module has already been loaded, it will
     * not be reloaded.
     */
    void Load(const BytecodeModule &module);

    /**
     * @return True if this module has been loaded an linked into the process; false otherwise.
     */
    bool IsLoaded() const noexcept { return loaded_; }

   private:
    // Flag indicating if the module is loaded into memory and executable.
    bool loaded_;
    // The object code.
    std::unique_ptr<llvm::MemoryBuffer> object_code_;
    // The memory manager.
    std::unique_ptr<MCMemoryManager> memory_manager_;
    // Function cache.
    std::unordered_map<std::string, void *> functions_;
  };
};

}  // namespace tpl::vm
