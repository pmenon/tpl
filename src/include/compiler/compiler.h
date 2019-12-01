#pragma once

#include <memory>
#include <string>

#include "util/region.h"
#include "util/timer.h"

namespace tpl {

namespace ast {
class AstNode;
class Context;
}  // namespace ast

namespace sema {
class ErrorReporter;
}  // namespace sema

namespace vm {
class BytecodeModule;
class Module;
}  // namespace vm

namespace compiler {

/**
 * Primary interface to drive compilation of TPL programs into TPL modules. TPL modules support
 * hybrid interpreted and JIT execution modes.
 */
class Compiler {
 public:
  enum class Response : uint8_t { Continue, Abort };

  /**
   * Input into a compilation job.
   */
  class Input {
    friend class Compiler;

   public:
    /**
     * Construct an input from a TPL source program.
     * @param name The name to assign the input.
     * @param context The TPL context to use.
     * @param source The TPL source code.
     */
    Input(const std::string &name, ast::Context *context, const std::string &source);

    /**
     * Construct input from a pre-generated TPL AST. The region that created the AST must also be
     * moved into this input, which will take ownership of it.
     * @param name The name to assign the input.
     * @param context The TPL context the AST belongs to.
     * @param root The root of the AST.
     */
    Input(const std::string &name, ast::Context *context, ast::AstNode *root);

    /**
     * @return The name of the input.
     */
    const std::string &GetName() const noexcept { return name_; }

    /**
     * @return True if the input is raw TPL source code; false otherwise.
     */
    bool IsFromSource() const noexcept { return source_ != nullptr; }

    /**
     * @return True if the input is a pre-generated TPL AST; false otherwise.
     */
    bool IsFromAST() const noexcept { return root_ != nullptr; }

    /**
     * @return The TPL context.
     */
    ast::Context *GetContext() const noexcept { return context_; }

   private:
    // The name to assign the input
    const std::string &name_;
    // The context that created the AST
    ast::Context *context_;
    // The root of the AST, if any
    ast::AstNode *root_;
    // The TPL source, if any
    const std::string *source_;
  };

  /**
   * Callback interface used to notify users of the various stages of compilation.
   */
  class Callbacks {
   public:
    virtual ~Callbacks() = default;
    virtual Response OnBeginParse(Compiler *compiler) { return Response::Continue; }
    virtual Response OnEndParse(Compiler *compiler) { return Response::Continue; }
    virtual Response OnBeginSemanticAnalysis(Compiler *compiler) { return Response::Continue; }
    virtual Response OnEndSemanticAnalysis(Compiler *compiler) { return Response::Continue; }
    virtual Response OnBeginBytecodeGeneration(Compiler *compiler) { return Response::Continue; }
    virtual Response OnEndBytecodeGeneration(Compiler *compiler) { return Response::Continue; }
    virtual Response OnBeginModuleGeneration(Compiler *compiler) { return Response::Continue; }
    virtual Response OnEndModuleGeneration(Compiler *compiler) { return Response::Continue; }
    virtual void OnError(Compiler *compiler) {}
    virtual void TakeOwnership(std::unique_ptr<vm::Module> module) = 0;
  };

  /**
   * Main entry point into the TPL compilation pipeline. Accepts an input program (in the form of
   * raw TPL source or a pre-generated AST), passes it through all required compilation passes and
   * produces a TPL module. The TPL module is provided to the callback.
   * @param input The input into the compiler.
   * @param callbacks The callbacks.
   */
  static void RunCompilation(const Input &input, Callbacks *callbacks);

  /**
   * @return The TPL context used during compilation.
   */
  ast::Context *GetContext() const { return input_.GetContext(); }

  /**
   * @return The diagnostic error reporter used during compilation.
   */
  sema::ErrorReporter *GetErrorReporter() const;

  /**
   * @return The AST for the compiled program. If parsing is not complete, or if there was an error
   *         during compilation, will be null.
   */
  ast::AstNode *GetAST() const { return root_; }

  /**
   * @return The generated bytecode module. If code-generation has not begun, or if there was an
   *         error during compilation, will be null.
   */
  vm::BytecodeModule *GetBytecodeModule() const { return bytecode_module_; }

 private:
  // Create a compiler instance
  explicit Compiler(const Compiler::Input &input);

  // This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(Compiler);

  // Destructor
  ~Compiler();

  // Driver
  void Run(Compiler::Callbacks *callbacks);

  bool Parse(Compiler::Callbacks *callbacks);

  bool SemanticAnalysis(Compiler::Callbacks *callbacks);

  std::unique_ptr<vm::BytecodeModule> GenerateBytecode(Compiler::Callbacks *callbacks);

  void GenerateModule(std::unique_ptr<vm::BytecodeModule>, Compiler::Callbacks *callbacks);

 private:
  // The input to compilation
  const Compiler::Input &input_;
  // The parsed AST
  ast::AstNode *root_;
  // The generated bytecode module
  vm::BytecodeModule *bytecode_module_;
};

class TimePasses : public Compiler::Callbacks {
 public:
  explicit TimePasses();

  Compiler::Response OnBeginParse(Compiler *compiler) override;
  Compiler::Response OnEndParse(Compiler *compiler) override;
  Compiler::Response OnBeginSemanticAnalysis(Compiler *compiler) override;
  Compiler::Response OnEndSemanticAnalysis(Compiler *compiler) override;
  Compiler::Response OnBeginBytecodeGeneration(Compiler *compiler) override;
  Compiler::Response OnEndBytecodeGeneration(Compiler *compiler) override;
  Compiler::Response OnBeginModuleGeneration(Compiler *compiler) override;
  Compiler::Response OnEndModuleGeneration(Compiler *compiler) override;

  double GetParseTimeMs() const noexcept { return parse_ms_; }
  double GetSemaTimeMs() const noexcept { return sema_ms_; }
  double GetTBCGenTimeMs() const noexcept { return tbc_gen_ms_; }
  double GetModuleGenTimeMs() const noexcept { return module_gen_ms_; }

 private:
  util::Timer<std::milli> timer_;
  // Time taken to parse program
  double parse_ms_;
  // Time taken to type-check program
  double sema_ms_;
  // Time taken to code-gen program
  double tbc_gen_ms_;
  // Time taken to construct module
  double module_gen_ms_;
};

}  // namespace compiler
}  // namespace tpl
