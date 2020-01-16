#include "sql/codegen/code_container.h"

#include "ast/ast_node_factory.h"
#include "ast/context.h"
#include "compiler/compiler.h"
#include "sema/error_reporter.h"
#include "vm/module.h"

#include <iostream>
#include "ast/ast_pretty_print.h"

namespace tpl::sql::codegen {

CodeContainer::CodeContainer(ast::Context *ctx)
    : ctx_(ctx), generated_file_(nullptr), module_(nullptr) {
  util::RegionVector<ast::Decl *> empty(ctx_->GetRegion());
  generated_file_ = ctx_->GetNodeFactory()->NewFile({0, 0}, std::move(empty));
}

// Needed because we forward-declare classes used as template types to std::unique_ptr<>
CodeContainer::~CodeContainer() = default;

void CodeContainer::RegisterStruct(ast::StructDecl *decl) {
  generated_file_->MutableDeclarations()->push_back(decl);
}

void CodeContainer::RegisterFunction(ast::FunctionDecl *decl) {
  generated_file_->MutableDeclarations()->push_back(decl);
}

namespace {

class ContainerCompileCallback : public compiler::Compiler::Callbacks {
 public:
  ContainerCompileCallback() : module_(nullptr) {}

  void OnError(compiler::Compiler::Phase phase, compiler::Compiler *compiler) override {
    compiler->GetErrorReporter()->PrintErrors(std::cout);
  }

  void TakeOwnership(std::unique_ptr<vm::Module> module) override { module_ = std::move(module); }

  std::unique_ptr<vm::Module> TakeModule() { return std::move(module_); }

 private:
  std::unique_ptr<vm::Module> module_;
};

}  // namespace

bool CodeContainer::Compile() {
  // If the container's code has already been compiled, we're done.
  if (IsCompiled()) {
    return true;
  }

  // Compile it.
  auto input = compiler::Compiler::Input("", ctx_, generated_file_);
  auto callbacks = ContainerCompileCallback();
  auto timer = compiler::TimePasses(&callbacks);
  compiler::Compiler::RunCompilation(input, &timer);
  module_ = callbacks.TakeModule();

  // If compilation succeeded, a valid module should have been created.
  // If no module was created, we conclude there was some error that'd
  // been reported to the error reporter.
  return module_ != nullptr;
}

void CodeContainer::Dump() { ast::AstPrettyPrint::Dump(std::cout, generated_file_); }

}  // namespace tpl::sql::codegen
