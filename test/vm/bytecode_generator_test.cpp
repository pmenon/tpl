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
#include "vm/bytecode_unit.h"
#include "vm/vm.h"

namespace tpl::vm::test {

class BytecodeGeneratorTest : public TplTest {};

class BytecodeExpectations {
 public:
  BytecodeExpectations() : tmp_("test"), errors_(tmp_), ctx_(tmp_, errors_) {}

  ast::AstNode *Compile(const std::string &source) {
    parsing::Scanner scanner(source);
    parsing::Parser parser(scanner, ctx_);

    auto *ast = parser.Parse();

    sema::Sema type_check(ctx_);
    type_check.Run(ast);

    return ast;
  }

 private:
  util::Region tmp_;
  sema::ErrorReporter errors_;
  ast::AstContext ctx_;
};

TEST_F(BytecodeGeneratorTest, LoadConstantTest) {
  auto src = R"(
    fun test(x: uint32) -> uint32 {
      var y : uint32 = 20
      return x * y
    })";
  BytecodeExpectations expectations;
  auto *ast = expectations.Compile(src);

  // Try generating bytecode for this declaration
  auto unit = BytecodeGenerator::Compile(ast);

  unit->PrettyPrint(std::cout);

  VM::Execute(*unit, "test");
}

}  // namespace tpl::vm::test