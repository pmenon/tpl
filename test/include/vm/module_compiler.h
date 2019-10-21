#pragma once

#include <memory>
#include <string>

#include "ast/ast.h"
#include "ast/context.h"
#include "parsing/parser.h"
#include "parsing/scanner.h"
#include "sema/sema.h"
#include "vm/bytecode_generator.h"
#include "vm/bytecode_module.h"
#include "vm/module.h"

namespace tpl::vm {

class ModuleCompiler {
 public:
  ModuleCompiler() : errors_(), ctx_(&errors_) {}

  ast::AstNode *CompileToAst(const std::string &source) {
    parsing::Scanner scanner(source);
    parsing::Parser parser(&scanner, &ctx_);

    auto *ast = parser.Parse();

    sema::Sema type_check(&ctx_);
    type_check.Run(ast);

    if (errors_.HasErrors()) {
      errors_.PrintErrors();
    }

    return ast;
  }

  std::unique_ptr<Module> CompileToModule(const std::string &source) {
    auto *ast = CompileToAst(source);
    if (HasErrors()) return nullptr;
    return std::make_unique<Module>(vm::BytecodeGenerator::Compile(ast, "test"));
  }

  // Does the error reporter have any errors?
  bool HasErrors() const { return errors_.HasErrors(); }

  // Reset any previous errors
  void ClearErrors() { errors_.Reset(); }

 private:
  sema::ErrorReporter errors_;
  ast::Context ctx_;
};

}  // namespace tpl::vm
