#include "compiler/compiler.h"
#include "ast/ast.h"
#include "ast/context.h"
#include "sema/error_reporter.h"
#include "util/test_harness.h"
#include "vm/module.h"

namespace tpl::compiler {

class CompilerTest : public TplTest {
 public:
  CompilerTest() : context_(&error_reporter_) {}

  ast::Context *Context() { return &context_; }

 private:
  sema::ErrorReporter error_reporter_;
  ast::Context context_;
};

TEST_F(CompilerTest, CompileFromSource) {
  // Should be able to compile multiple TPL programs from source, including
  // functions that potentially collide in name.

  for (uint32_t i = 1; i < 4; i++) {
    auto src = std::string("fun test() -> int32 { return " + std::to_string(i * 10) + " }");
    auto input = Compiler::Input("Simple Test", Context(), &src);
    auto module = Compiler::RunCompilationSimple(input);

    // The module should be valid since the input source is valid
    EXPECT_FALSE(module == nullptr);

    // The function should exist
    std::function<int32_t()> test_fn;
    EXPECT_TRUE(module->GetFunction("test", vm::ExecutionMode::Interpret, test_fn));

    // And should return what we expect
    EXPECT_EQ(i * 10, test_fn());
  }
}

TEST_F(CompilerTest, CompileToAst) {
  // Check compilation only to AST.
  struct CompileToAstCallback : public Compiler::Callbacks {
    ast::AstNode *root;
    bool BeginPhase(Compiler::Phase phase, Compiler *compiler) override {
      return phase == Compiler::Phase::Parsing;
    }
    void EndPhase(Compiler::Phase phase, Compiler *compiler) override {
      if (phase == Compiler::Phase::Parsing) {
        root = compiler->GetAST();
      }
    }
    void OnError(Compiler::Phase phase, Compiler *compiler) override { FAIL(); }
    void TakeOwnership(std::unique_ptr<vm::Module> module) override { FAIL(); }
  };

  auto src = std::string("fun BLAH() -> int32 { return 10 }");
  auto input = Compiler::Input("Simple Test", Context(), &src);
  auto callback = CompileToAstCallback();
  Compiler::RunCompilation(input, &callback);

  EXPECT_NE(nullptr, callback.root);
  EXPECT_TRUE(callback.root->IsFile());
  EXPECT_TRUE(callback.root->As<ast::File>()->GetDeclarations()[0]->IsFunctionDecl());
  auto *decl = callback.root->As<ast::File>()->GetDeclarations()[0]->As<ast::FunctionDecl>();

  // Check name
  EXPECT_EQ(Context()->GetIdentifier("BLAH"), decl->name());

  // Check type isn't set, since we didn't do type checking
  EXPECT_EQ(nullptr, decl->function()->type_repr()->return_type()->type());
}

}  // namespace tpl::compiler
