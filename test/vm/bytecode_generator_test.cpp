#include "tpl_test.h"

// From test
#include "vm/bytecode_compiler.h"

#include "logging/logger.h"

namespace tpl::vm::test {

class BytecodeGeneratorTest : public TplTest {
 public:
  BytecodeGeneratorTest() : region_("test") {}

  util::Region *region() { return &region_; }

 private:
  util::Region region_;
};

TEST_F(BytecodeGeneratorTest, SimpleTest) {
  //
  // Create a function that multiples an input unsigned 32-bit integer by 20
  //

  auto src = R"(
    fun mul20(x: uint32) -> uint32 {
      var y: uint32 = 20
      return x * y
    })";
  BytecodeCompiler compiler;
  auto *ast = compiler.CompileToAst(src);

  // Try generating bytecode for this declaration
  auto module = BytecodeGenerator::Compile(ast, "mul20");

  std::function<u32(u32)> mul_20;
  EXPECT_TRUE(module->GetFunction("mul20", ExecutionMode::Interpret, mul_20))
      << "Function 'mul20' not found in module";

  EXPECT_EQ(20u, mul_20(1));
  EXPECT_EQ(40u, mul_20(2));
  EXPECT_EQ(60u, mul_20(3));
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

TEST_F(BytecodeGeneratorTest, SimpleArithmeticTest) {
  const auto gen_compare_func = [](auto arg_type_name, auto dummy_arg, auto op,
                                   auto cb) {
    using Type = decltype(dummy_arg);
    auto src = fmt::format(R"(
      fun test(a: {0}, b: {0}) -> {0} {{
        return a {1} b
      }})",
                           arg_type_name, op);

    BytecodeCompiler compiler;
    auto *ast = compiler.CompileToAst(src);
    ASSERT_FALSE(compiler.HasErrors());

    auto module = BytecodeGenerator::Compile(ast, "test");

    std::function<Type(Type, Type)> fn;
    ASSERT_TRUE(module->GetFunction("test", ExecutionMode::Interpret, fn))
        << "Function 'test' not found in module";

    // Test the function
    cb(fn);
  };

#define CMP_TEST(cpptype, tpltype, op)                                 \
  gen_compare_func(tpltype, cpptype{0}, #op, [](auto fn) {             \
    EXPECT_EQ(cpptype{1} op cpptype{1}, fn(cpptype{1}, cpptype{1}));   \
    EXPECT_EQ(cpptype{-1} op cpptype{1}, fn(cpptype{-1}, cpptype{1})); \
    EXPECT_EQ(cpptype{2} op cpptype{1}, fn(cpptype{2}, cpptype{1}));   \
  });

#define TEST_ALL_CMP(cpptype, tpltype) \
  CMP_TEST(cpptype, tpltype, +)        \
  CMP_TEST(cpptype, tpltype, -)        \
  CMP_TEST(cpptype, tpltype, *)        \
  CMP_TEST(cpptype, tpltype, /)        \
  CMP_TEST(cpptype, tpltype, %)

  TEST_ALL_CMP(i8, "int8")
  TEST_ALL_CMP(i16, "int16")
  TEST_ALL_CMP(i32, "int32")
  TEST_ALL_CMP(i64, "int64")

#undef TEST_ALL_CMP
#undef CMP_TEST
}

TEST_F(BytecodeGeneratorTest, ComparisonTest) {
  const auto gen_compare_func = [](auto arg_type_name, auto dummy_arg, auto op,
                                   auto cb) {
    using Type = decltype(dummy_arg);
    auto src = fmt::format(R"(
      fun test(a: {0}, b: {0}) -> bool {{
        return a {1} b
      }})",
                           arg_type_name, op);

    BytecodeCompiler compiler;
    auto *ast = compiler.CompileToAst(src);
    ASSERT_FALSE(compiler.HasErrors());

    auto module = BytecodeGenerator::Compile(ast, "test");

    std::function<bool(Type, Type)> fn;
    ASSERT_TRUE(module->GetFunction("test", ExecutionMode::Interpret, fn))
        << "Function 'test' not found in module";

    // Test the function
    cb(fn);
  };

#define CMP_TEST(cpptype, tpltype, op)                                 \
  gen_compare_func(tpltype, cpptype{0}, #op, [](auto fn) {             \
    EXPECT_EQ(cpptype{1} op cpptype{1}, fn(cpptype{1}, cpptype{1}));   \
    EXPECT_EQ(cpptype{-1} op cpptype{1}, fn(cpptype{-1}, cpptype{1})); \
    EXPECT_EQ(cpptype{2} op cpptype{1}, fn(cpptype{2}, cpptype{1}));   \
  });

#define TEST_ALL_CMP(cpptype, tpltype) \
  CMP_TEST(cpptype, tpltype, <)        \
  CMP_TEST(cpptype, tpltype, <=)       \
  CMP_TEST(cpptype, tpltype, ==)       \
  CMP_TEST(cpptype, tpltype, >)        \
  CMP_TEST(cpptype, tpltype, >=)       \
  CMP_TEST(cpptype, tpltype, !=)

  TEST_ALL_CMP(i8, "int8")
  TEST_ALL_CMP(i16, "int16")
  TEST_ALL_CMP(i32, "int32")
  TEST_ALL_CMP(i64, "int64")

#undef TEST_ALL_CMP
#undef CMP_TEST
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
