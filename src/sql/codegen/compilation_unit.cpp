#include "sql/codegen/compilation_unit.h"

#include <iostream>
#include <sstream>

#include "ast/ast_node_factory.h"
#include "ast/ast_pretty_print.h"
#include "ast/context.h"
#include "compiler/compiler.h"
#include "logging/logger.h"
#include "sema/error_reporter.h"
#include "vm/module.h"

namespace tpl::sql::codegen {

CompilationUnit::CompilationUnit(ast::Context *ctx, std::string name) : ctx_(ctx), name_(name) {}

void CompilationUnit::CopyDeclarations(const CompilationUnit &other) {
  TPL_ASSERT(ctx_ == other.ctx_,
             "Mismatched AST context objects. When adding all declarations from one container into "
             "the other, they must both be using the same AST context.");
  // First, copy the structs.
  structs_.reserve(structs_.size() + other.structs_.size());
  structs_.insert(structs_.end(), other.structs_.begin(), other.structs_.end());
  // Then the functions.
  functions_.reserve(functions_.size() + other.functions_.size());
  functions_.insert(functions_.end(), other.functions_.begin(), other.functions_.end());
}

namespace {

class Callbacks : public compiler::Compiler::Callbacks {
 public:
  Callbacks() : module_(nullptr) {}

  void OnError(compiler::Compiler::Phase phase, compiler::Compiler *compiler) override {
    std::string generated_code, errors;
    // Serialize the errors.
    {
      std::stringstream ss;
      compiler->GetErrorReporter()->PrintErrors(ss);
      errors = ss.str();
    }
    // Serialize the generated code.
    {
      std::stringstream ss;
      ast::AstPrettyPrint::Dump(ss, compiler->GetAST());
      generated_code = ss.str();
    }
    LOG_ERROR("======================== COMPILER ERROR START ========================");
    LOG_ERROR("Generated Code: {}", generated_code);
    LOG_ERROR("Errors: {}", errors);
    LOG_ERROR("======================== COMPILER ERROR END ==========================");
  }

  void TakeOwnership(std::unique_ptr<vm::Module> module) override { module_ = std::move(module); }

  std::unique_ptr<vm::Module> ReleaseModule() { return std::move(module_); }

 private:
  std::unique_ptr<vm::Module> module_;
};

}  // namespace

std::unique_ptr<vm::Module> CompilationUnit::Compile() {
  // Build up the declaration list for the file. This is just the concatenation
  // of all the structure and function definitions, in that order.
  util::RegionVector<ast::Decl *> declarations(ctx_->GetRegion());
  declarations.reserve(structs_.size() + functions_.size());
  declarations.insert(declarations.end(), structs_.begin(), structs_.end());
  declarations.insert(declarations.end(), functions_.begin(), functions_.end());

  // Create the file we're to compile.
  ast::File *generated_file = ctx_->GetNodeFactory()->NewFile({0, 0}, std::move(declarations));

  ast::AstPrettyPrint::Dump(std::cout, generated_file);

  // Compile it!
  compiler::Compiler::Input input(name_, ctx_, generated_file);
  Callbacks callbacks;
  compiler::TimePasses timer(&callbacks);
  compiler::Compiler::RunCompilation(input, &timer);
  std::unique_ptr<vm::Module> module = callbacks.ReleaseModule();

  LOG_DEBUG("Type-check: {:.2f} ms, Bytecode Gen: {:.2f} ms, Module Gen: {:.2f} ms",
            timer.GetSemaTimeMs(), timer.GetBytecodeGenTimeMs(), timer.GetModuleGenTimeMs());

  // Done.
  return module;
}

}  // namespace tpl::sql::codegen
