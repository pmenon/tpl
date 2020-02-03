#include "sql/codegen/executable_query_builder.h"

#include <iostream>

#include "ast/ast_node_factory.h"
#include "ast/context.h"
#include "compiler/compiler.h"
#include "sema/error_reporter.h"
#include "vm/module.h"

namespace tpl::sql::codegen {

ExecutableQueryFragmentBuilder::ExecutableQueryFragmentBuilder(ast::Context *ctx) : ctx_(ctx) {}

void ExecutableQueryFragmentBuilder::RegisterStep(ast::FunctionDecl *decl) {
  functions_.push_back(decl);
  step_functions_.push_back(decl->Name().GetString());
}

namespace {

class Callbacks : public compiler::Compiler::Callbacks {
 public:
  Callbacks() : module_(nullptr) {}

  void OnError(compiler::Compiler::Phase phase, compiler::Compiler *compiler) override {
    compiler->GetErrorReporter()->PrintErrors(std::cout);
  }

  void TakeOwnership(std::unique_ptr<vm::Module> module) override { module_ = std::move(module); }

  std::unique_ptr<vm::Module> ReleaseModule() { return std::move(module_); }

 private:
  std::unique_ptr<vm::Module> module_;
};

}  // namespace

std::unique_ptr<ExecutableQuery::Fragment> ExecutableQueryFragmentBuilder::Compile() {
  // Build up the declaration list for the file.
  util::RegionVector<ast::Decl *> decls(ctx_->GetRegion());
  decls.reserve(structs_.size() + functions_.size());
  decls.insert(decls.end(), structs_.begin(), structs_.end());
  decls.insert(decls.end(), functions_.begin(), functions_.end());

  // The file we'll compile.
  ast::File *generated_file = ctx_->GetNodeFactory()->NewFile({0, 0}, std::move(decls));

  // Compile it!
  compiler::Compiler::Input input("", ctx_, generated_file);
  Callbacks callbacks;
  compiler::TimePasses timer(&callbacks);
  compiler::Compiler::RunCompilation(input, &timer);
  std::unique_ptr<vm::Module> module = callbacks.ReleaseModule();

  // Create the fragment.
  return std::make_unique<ExecutableQuery::Fragment>(std::move(step_functions_), std::move(module));
}

}  // namespace tpl::sql::codegen
