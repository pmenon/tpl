#include <string>

#include "tpl_test.h"  // NOLINT

#include "llvm/Support/FormatVariadic.h"

// From test
#include "vm/bytecode_compiler.h"

namespace tpl::vm::test {

class BytecodeGeneratorTest : public TplTest {
 public:
  BytecodeGeneratorTest() : region_("test") {}

  util::Region *region() { return &region_; }

 private:
  util::Region region_;
};

TEST_F(BytecodeGeneratorTest, SimpleTest) {
  auto src = R"(
    fun test(x: uint32) -> uint32 {
      var y : uint32 = 20
      return x * y
    })";
  BytecodeCompiler compiler;
  auto *ast = compiler.CompileToAst(src);

  // Try generating bytecode for this declaration
  auto module = BytecodeGenerator::Compile(ast, "test");

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
  BytecodeCompiler compiler;
  auto *ast = compiler.CompileToAst(src);

  // Try generating bytecode for this declaration
  auto module = BytecodeGenerator::Compile(ast, "test");

  module->PrettyPrint(std::cout);

  std::function<bool()> f;
  EXPECT_TRUE(module->GetFunction("test", ExecutionMode::Interpret, f))
      << "Function 'test' not found in module";
  EXPECT_FALSE(f());
}

TEST_F(BytecodeGeneratorTest, SimpleTypesTest) {
  auto fn = [](auto type, auto arg) {
    using ArgType = decltype(arg);

    auto src = llvm::formatv(R"(
      fun test(a: *{0}) -> void {{
        *a = 10
        return
      })",
                             type);

    BytecodeCompiler compiler;
    auto *ast = compiler.CompileToAst(src);
    ASSERT_FALSE(compiler.HasErrors());

    auto module = BytecodeGenerator::Compile(ast, "test");

    module->PrettyPrint(std::cout);

    std::function<void(ArgType *)> f;
    ASSERT_TRUE(module->GetFunction("test", ExecutionMode::Interpret, f))
        << "Function 'test' not found in module";

    ArgType a = 0;

    f(&a);
    EXPECT_EQ((ArgType)10, a);
  };

  fn("int8", static_cast<i8>(0));
  fn("int16", static_cast<i16>(0));
  fn("int32", static_cast<i32>(0));
  fn("int64", static_cast<i64>(0));
  fn("uint8", static_cast<u8>(0));
  fn("uint16", static_cast<u16>(0));
  fn("uint32", static_cast<u32>(0));
  fn("uint64", static_cast<u64>(0));
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
  BytecodeCompiler compiler;
  auto *ast = compiler.CompileToAst(src);

  // Try generating bytecode for this declaration
  auto module = BytecodeGenerator::Compile(ast, "test");

  module->PrettyPrint(std::cout);

  struct S {
    int a;
    int b;
  };

  std::function<bool(S *)> f;
  EXPECT_TRUE(module->GetFunction("test", ExecutionMode::Interpret, f))
      << "Function 'test' not found in module";

  S s{.a = 0, .b = 0};
  EXPECT_TRUE(f(&s));
  EXPECT_EQ(10, s.a);
  EXPECT_EQ(20, s.b);
}

TEST_F(BytecodeGeneratorTest, FunctionTypeCheckTest) {
  /*
   * Function with nil return type cannot return expression
   */

  {
    auto src = R"(
    fun test() -> nil {
      var a = 10
      return a
    })";

    BytecodeCompiler compiler;
    compiler.CompileToAst(src);
    EXPECT_TRUE(compiler.HasErrors());
  }

  /*
   * Function with non-nil return type must have expression
   */

  {
    auto src = R"(
    fun test() -> int32 {
      return
    })";

    BytecodeCompiler compiler;
    compiler.CompileToAst(src);
    EXPECT_TRUE(compiler.HasErrors());
  }

  {
    auto src = R"(
    fun test() -> int32 {
      return 10
    })";

    BytecodeCompiler compiler;
    auto *ast = compiler.CompileToAst(src);
    EXPECT_FALSE(compiler.HasErrors());

    // Try generating bytecode for this declaration
    auto module = BytecodeGenerator::Compile(ast, "test");

    module->PrettyPrint(std::cout);

    std::function<i32()> f;
    EXPECT_TRUE(module->GetFunction("test", ExecutionMode::Interpret, f))
        << "Function 'test' not found in module";

    EXPECT_EQ(10, f());
  }

  {
    auto src = R"(
    fun test() -> int16 {
      var a: int16 = 20
      var b: int16 = 40
      return a * b
    })";

    BytecodeCompiler compiler;
    auto *ast = compiler.CompileToAst(src);
    EXPECT_FALSE(compiler.HasErrors());

    // Try generating bytecode for this declaration
    auto module = BytecodeGenerator::Compile(ast, "test");

    module->PrettyPrint(std::cout);

    std::function<i32()> f;
    EXPECT_TRUE(module->GetFunction("test", ExecutionMode::Interpret, f))
        << "Function 'test' not found in module";

    EXPECT_EQ(800, f());
  }
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
  BytecodeCompiler compiler;
  auto *ast = compiler.CompileToAst(src);

  // Try generating bytecode for this declaration
  auto module = BytecodeGenerator::Compile(ast, "test");

  module->PrettyPrint(std::cout);

  struct S {
    int a;
    int b;
  };

  std::function<bool(S *)> f;
  EXPECT_TRUE(module->GetFunction("test", ExecutionMode::Interpret, f))
      << "Function 'test' not found in module";

  S s{.a = 0, .b = 0};
  EXPECT_TRUE(f(&s));
  EXPECT_EQ(10, s.a);
  EXPECT_EQ(20, s.b);
}

}  // namespace tpl::vm::test
