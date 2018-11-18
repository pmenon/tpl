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
#include "vm/bytecode_module.h"
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

TEST_F(BytecodeGeneratorTest, SimpleTest) {
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

  std::function<u32(u32)> f;
  EXPECT_TRUE(module->GetFunction("test", ExecutionMode::Interpret, f))
      << "Function 'test' not found in module";

  EXPECT_EQ(20u, f(1));
  EXPECT_EQ(40u, f(2));
  EXPECT_EQ(60u, f(3));
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

  std::function<bool()> f;
  EXPECT_TRUE(module->GetFunction("test", ExecutionMode::Interpret, f))
            << "Function 'test' not found in module";
  EXPECT_FALSE(f());
}

TEST_F(BytecodeGeneratorTest, ParameterPassingTest) {
  auto src = R"(
    struct S {
      a: int
      b: int
    }
    fun test(s: *S) -> bool {
      s.a = 10
      s.b = s.a * 2
      return true
    })";
  BytecodeExpectations expectations(region());
  auto *ast = expectations.Compile(src);

  // Try generating bytecode for this declaration
  auto module = BytecodeGenerator::Compile(region(), ast);

  module->PrettyPrint(std::cout);

  struct S {
    int a;
    int b;
  };

  std::function<bool(S *s)> f;
  EXPECT_TRUE(module->GetFunction("test", ExecutionMode::Interpret, f))
            << "Function 'test' not found in module";

  S s{.a = 0, .b = 0};
  EXPECT_TRUE(f(&s));
  EXPECT_EQ(10, s.a);
  EXPECT_EQ(20, s.b);
}

TEST_F(BytecodeGeneratorTest, FunctionTest) {
  auto src = R"(
    struct S {
      a: int
      b: int
    }
    fun f(s: *S) -> bool {
      s.b = s.a * 2
      return true
    }
    fun test(s: *S) -> bool {
      s.a = 10
      f(s)
      return true
    })";
  BytecodeExpectations expectations(region());
  auto *ast = expectations.Compile(src);

  // Try generating bytecode for this declaration
  auto module = BytecodeGenerator::Compile(region(), ast);

  module->PrettyPrint(std::cout);

  struct S {
    int a;
    int b;
  };

  std::function<bool(S *s)> f;
  EXPECT_TRUE(module->GetFunction("test", ExecutionMode::Interpret, f))
            << "Function 'test' not found in module";

  S s{.a = 0, .b = 0};
  EXPECT_TRUE(f(&s));
  EXPECT_EQ(10, s.a);
  EXPECT_EQ(20, s.b);
}

}  // namespace tpl::vm::test