#include "tpl_test.h"

#include <random>

#include "vm/bytecode_compiler.h"

namespace tpl::vm::test {

//
// These tests use the trampoline to call into bytecode functions.
// TODO(pmenon): We need way more fucking tests for this ...
//

class BytecodeTrampolineTest : public TplTest {};

TEST_F(BytecodeTrampolineTest, VoidFunctionTest) {
  BytecodeCompiler compiler;

  auto src = "fun test() -> nil { }";
  auto module = compiler.CompileToModule(src);

  EXPECT_FALSE(compiler.HasErrors());

  auto fn = reinterpret_cast<void (*)()>(
      module->GetFuncTrampoline(module->GetFuncInfoByName("test")->id()));

  fn();
}

TEST_F(BytecodeTrampolineTest, BooleanFunctionTest) {
  BytecodeCompiler compiler;

  auto src = "fun lt(a: int32, b: int32) -> bool { return a < b }";
  auto module = compiler.CompileToModule(src);
  module->PrettyPrint(std::cout);

  EXPECT_FALSE(compiler.HasErrors());

  auto less_than = reinterpret_cast<bool (*)(i32, i32)>(
      module->GetFuncTrampoline(module->GetFuncInfoByName("lt")->id()));

  EXPECT_EQ(true, less_than(1, 2));
  EXPECT_EQ(false, less_than(2, 1));
}

TEST_F(BytecodeTrampolineTest, IntFunctionTest) {
  {
    BytecodeCompiler compiler;

    auto src = "fun test() -> int32 { return 10 }";
    auto module = compiler.CompileToModule(src);
    module->PrettyPrint(std::cout);

    EXPECT_FALSE(compiler.HasErrors());

    auto fn = reinterpret_cast<i32 (*)()>(
        module->GetFuncTrampoline(module->GetFuncInfoByName("test")->id()));

    EXPECT_EQ(10, fn());
  }

  // Add function
  {
    BytecodeCompiler compiler;

    auto src = "fun add2(a: int32, b: int32) -> int32 { return a + b }";
    auto module = compiler.CompileToModule(src);
    module->PrettyPrint(std::cout);

    EXPECT_FALSE(compiler.HasErrors());

    auto fn = reinterpret_cast<i32 (*)(i32, i32)>(
        module->GetFuncTrampoline(module->GetFuncInfoByName("add2")->id()));

    EXPECT_EQ(20, fn(10, 10));
    EXPECT_EQ(10, fn(0, 10));
    EXPECT_EQ(10, fn(10, 0));
    EXPECT_EQ(0, fn(0, 0));
  }

  // Sub function
  {
    BytecodeCompiler compiler;

    auto src =
        "fun sub3(a: int32, b: int32, c: int32) -> int32 { return a - b - c }";
    auto module = compiler.CompileToModule(src);
    module->PrettyPrint(std::cout);

    EXPECT_FALSE(compiler.HasErrors());

    auto fn = reinterpret_cast<i32 (*)(i32, i32, i32)>(
        module->GetFuncTrampoline(module->GetFuncInfoByName("sub3")->id()));

    EXPECT_EQ(-10, fn(10, 10, 10));
    EXPECT_EQ(10, fn(30, 10, 10));
    EXPECT_EQ(0, fn(0, 0, 0));
  }
}

TEST_F(BytecodeTrampolineTest, BigIntFunctionTest) {
  {
    BytecodeCompiler compiler;

    auto src = R"(
    fun test() -> int64 {
      var x : int64 = 10
      return x
    })";
    auto module = compiler.CompileToModule(src);
    module->PrettyPrint(std::cout);

    EXPECT_FALSE(compiler.HasErrors());

    auto fn = reinterpret_cast<i64 (*)()>(
        module->GetFuncTrampoline(module->GetFuncInfoByName("test")->id()));

    EXPECT_EQ(10, fn());
  }

  {
    BytecodeCompiler compiler;

    auto src = R"(
    fun mul3(a: int64, b: int64, c: int64) -> int64 {
      return a * b * c
    })";
    auto module = compiler.CompileToModule(src);
    module->PrettyPrint(std::cout);

    EXPECT_FALSE(compiler.HasErrors());

    auto fn = reinterpret_cast<i64 (*)(i64, i64, i64)>(
        module->GetFuncTrampoline(module->GetFuncInfoByName("mul3")->id()));

    EXPECT_EQ(6, fn(1, 2, 3));
    EXPECT_EQ(-6, fn(-1, 2, 3));
    EXPECT_EQ(0, fn(0, 2, 3));
  }
}

TEST_F(BytecodeTrampolineTest, GeneratedSorterTest) {
  // Generate a few random numbers
  const u32 nelems = 10;
  std::vector<i32> numbers(nelems);
  std::random_device random;
  std::generate(numbers.begin(), numbers.end(),
                [&random]() { return random() % 100; });

  // Generate the comparison function that sorts ascending
  BytecodeCompiler compiler;
  auto src = "fun compare(a: *int32, b: *int32) -> bool { return *a < *b }";
  auto module = compiler.CompileToModule(src);
  EXPECT_FALSE(compiler.HasErrors());
  auto compare = reinterpret_cast<bool (*)(const i32 *, const i32 *)>(
      module->GetFuncTrampoline(module->GetFuncInfoByName("compare")->id()));
  EXPECT_TRUE(compare != nullptr);

  // Try to sort using the generated comparison function
  std::sort(
      numbers.begin(), numbers.end(),
      [compare](const auto &a, const auto &b) { return compare(&a, &b); });

  // Verify
  EXPECT_TRUE(std::is_sorted(numbers.begin(), numbers.end()));
}

}  // namespace tpl::vm::test