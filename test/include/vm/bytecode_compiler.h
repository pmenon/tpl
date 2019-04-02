#pragma once

#include "ast/ast.h"
#include "ast/ast_context.h"
#include "parsing/parser.h"
#include "parsing/scanner.h"
#include "sema/sema.h"
#include "vm/bytecode_generator.h"
#include "vm/bytecode_module.h"

namespace tpl::vm::test {

class BytecodeCompiler {
 public:
  BytecodeCompiler()
      : region_("temp"), errors_(&region_), ctx_(&region_, errors_) {}

  ast::AstNode *CompileToAst(const std::string &source) {
    parsing::Scanner scanner(source);
    parsing::Parser parser(scanner, ctx_);

    auto *ast = parser.Parse();

    sema::Sema type_check(ctx_);
    type_check.Run(ast);

    if (errors_.HasErrors()) {
      errors_.PrintErrors();
    }

    return ast;
  }

  std::unique_ptr<BytecodeModule> CompileToModule(const std::string &source) {
    auto *ast = CompileToAst(source);
    if (HasErrors()) return nullptr;
    return vm::BytecodeGenerator::Compile(ast, "test");
  }

  // Does the error reporter have any errors?
  bool HasErrors() const { return errors_.HasErrors(); }

  // Reset any previous errors
  void ClearErrors() { errors_.Reset(); }

 private:
  util::Region region_;
  sema::ErrorReporter errors_;
  ast::AstContext ctx_;
};

}  // namespace tpl::vm::test