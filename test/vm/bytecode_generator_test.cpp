#include "gtest/gtest.h"

#include "tpl_test.h"

#include "ast/ast_context.h"
#include "ast/ast_node_factory.h"
#include "ast/type.h"
#include "parsing/parser.h"
#include "parsing/scanner.h"
#include "sema/error_reporter.h"
#include "sema/sema.h"
#include "util/region.h"
#include "vm/bytecode_generator.h"
#include "vm/module.h"
#include "vm/vm.h"

namespace tpl::vm::test {

class BytecodeGeneratorTest : public TplTest {
 public:
  BytecodeGeneratorTest() : region_("test") {}

  util::Region *region() { return &region_; }

 private:
  util::Region region_;
};

class BytecodeExpectations {
 public:
  explicit BytecodeExpectations(util::Region *region)
      : errors_(region), ctx_(region, errors_) {}

  ast::AstNode *Compile(const std::string &source) {
    parsing::Scanner scanner(source);
    parsing::Parser parser(scanner, ctx_);

    auto *ast = parser.Parse();

    sema::Sema type_check(ctx_);
    type_check.Run(ast);

    return ast;
  }

 private:
  sema::ErrorReporter errors_;
  ast::AstContext ctx_;
};

TEST_F(BytecodeGeneratorTest, LoadConstantTest) {
  auto src = R"(
    fun test(x: uint32) -> uint32 {
      var y : uint32 = 20
      return x * y
    })";
  BytecodeExpectations expectations(region());
  auto *ast = expectations.Compile(src);

  // Try generating bytecode for this declaration
  auto module = BytecodeGenerator::Compile(region(), ast);

  module->PrettyPrint(std::cout);

  VM::Execute(region(), *module, "test");
}

TEST_F(BytecodeGeneratorTest, BooleanEvaluationTest) {
  auto src = R"(
    fun test() -> bool {
      var x : int32 = 4
      var t : int32 = 8
      var f : int32 = 10
      return (f > 1 and x < 2) and (t < 100 or x < 3)
    })";
  BytecodeExpectations expectations(region());
  auto *ast = expectations.Compile(src);

  // Try generating bytecode for this declaration
  auto module = BytecodeGenerator::Compile(region(), ast);

  module->PrettyPrint(std::cout);

  VM::Execute(region(), *module, "test");
}

}  // namespace tpl::vm::test