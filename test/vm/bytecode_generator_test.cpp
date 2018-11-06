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

TEST_F(BytecodeGeneratorTest, ShortCircuitEvaluationTest_SimpleExpression) {
  auto src = R"(
    fun test() -> bool {
      var x : int32 = 4
      var t : int32 = 8
      var f : int32 = 10
      return (f > 1 and x > 2) and (t < 100 or x < 3)
    })";
  BytecodeExpectations expectations;
  auto *ast = expectations.Compile(src);

  // Try generating bytecode for this declaration
  auto unit = BytecodeGenerator::Compile(ast);

  unit->PrettyPrint(std::cout);

  VM::Execute(*unit, "test");
}

TEST_F(BytecodeGeneratorTest, ShortCircuitEvaluationTest_IfStatement) {
  {
    auto src = R"(
      fun test() -> int32 {
        var x : int32 = 4
        var t : int32 = 8
        var f : int32 = 10
        var temp: int32
        if ((f > 1 and x > 2) and (t < 100 or x < 3)) {
          temp = 1
        } else {
          temp = 100
        }
        return temp
      })";
    BytecodeExpectations expectations;
    auto *ast = expectations.Compile(src);

    // Try generating bytecode for this declaration
    auto unit = BytecodeGenerator::Compile(ast);

    unit->PrettyPrint(std::cout);

    VM::Execute(*unit, "test");    
  }

  {
    auto src = R"(
      fun test() -> int32 {
        var x : int32 = 4
        var t : int32 = 8
        var f : int32 = 10
        var temp: int32
        if ((f < 1 and x > 2) and (t < 100 or x < 3)) {
          temp = 1
        } else {
          temp = 100
        }
        return temp
      })";
    BytecodeExpectations expectations;
    auto *ast = expectations.Compile(src);

    // Try generating bytecode for this declaration
    auto unit = BytecodeGenerator::Compile(ast);

    unit->PrettyPrint(std::cout);

    VM::Execute(*unit, "test");
  }
}

TEST_F(BytecodeGeneratorTest, ForLoopTest) {
  auto src = R"(
    fun test() -> int32 {
      var i : int32
      var n : int32 = 10
      var count : int32 = 0

      for (i = 0; i < n; i = i + 1) {
        count = count + 2
      }

      return count
    })";
  BytecodeExpectations expectations;
  auto *ast = expectations.Compile(src);

  // Try generating bytecode for this declaration
  auto unit = BytecodeGenerator::Compile(ast);

  unit->PrettyPrint(std::cout);

  VM::Execute(*unit, "test");

  // Expected count = 20
}

TEST_F(BytecodeGeneratorTest, JumpToJumpTest) {
  auto src = R"(
    fun test() -> int32 {
      var i : int32
      var j : int32
      var n : int32 = 10
      var count : int32 = 0

      for (i = 0; i < n; i = i + 1) {
        for (j = 0; j < n; j = j + 1) {
          if ((i < 5 and j < 5) or (i >= 5 and j >= 5)) {
            if ((i < 2 and j < 2) or (i > 7 and j > 7)) {
              count = count + 1
            } else {
              count = count + 2
            }
          } else {
            if ((i < 2 and j > 7) or (i > 7 and j < 2)) {
              count = count + 3
            } else {
              count = count + 4
            }
          }
        }
      }

      return count
    })";
  BytecodeExpectations expectations;
  auto *ast = expectations.Compile(src);

  // Try generating bytecode for this declaration
  auto unit = BytecodeGenerator::Compile(ast);

  unit->PrettyPrint(std::cout);

  VM::Execute(*unit, "test");

  // Expected total = 8 * 1 + 2 * 42 + 8 * 3 + 4 * 42 = 284
}

TEST_F(BytecodeGeneratorTest, JumpToJumpTest_IfWithoutElse) {
  auto src = R"(
    fun test() -> int32 {
      var i : int32
      var j : int32
      var n : int32 = 10
      var count : int32 = 0

      for (i = 0; i < n; i = i + 1) {
        for (j = 0; j < n; j = j + 1) {
          if ((i < 5 and j < 5) or (i >= 5 and j >= 5)) {
            if ((i < 2 and j < 2) or (i > 7 and j > 7)) {
              count = count + 1
            }
          } else {
            if ((i < 2 and j > 7) or (i > 7 and j < 2)) {
              count = count + 3
            }
          }
        }
      }

      return count
    })";
  BytecodeExpectations expectations;
  auto *ast = expectations.Compile(src);

  // Try generating bytecode for this declaration
  auto unit = BytecodeGenerator::Compile(ast);

  unit->PrettyPrint(std::cout);

  VM::Execute(*unit, "test");

  // Expected total = 8 * 1 + 8 * 3 = 32
}

TEST_F(BytecodeGeneratorTest, JumpToJumpTest_WhileLoop) {
  auto src = R"(
    fun test() -> int32 {
      var i : int32 = -1
      var j : int32
      var n : int32 = 10
      var count : int32 = 0

      for (i < (n - 1)) {
        i = i + 1
        j = -1
        for (j < (n - 1)) {
          j = j + 1
          if ((i < 5 and j < 5) or (i >= 5 and j >= 5)) {
            if ((i < 2 and j < 2) or (i > 7 and j > 7)) {
              count = count + 1
            }
          } else {
            if ((i < 2 and j > 7) or (i > 7 and j < 2)) {
              count = count + 3
            }
          }
        }
      }

      return count
    })";
  BytecodeExpectations expectations;
  auto *ast = expectations.Compile(src);

  // Try generating bytecode for this declaration
  auto unit = BytecodeGenerator::Compile(ast);

  unit->PrettyPrint(std::cout);

  VM::Execute(*unit, "test");

  // Expected total = 8 * 1 + 8 * 3 = 32
}

TEST_F(BytecodeGeneratorTest, ContinueTest) {
  {
    auto src = R"(
      fun test() -> int32 {
        var i : int32 = -1
        var j : int32
        var n : int32 = 10
        var count : int32 = 0

        for (i < (n - 1)) {
          i = i + 1
          j = -1
          for (j < (n - 1)) {
            j = j + 1
            if ((i < 5 and j < 5) or (i >= 5 and j >= 5)) {
              if ((i < 2 and j < 2) or (i > 7 and j > 7)) {
                continue
              }
              count = count + 2
            } else {
              if ((i < 2 and j > 7) or (i > 7 and j < 2)) {
                continue
              }
              count = count + 4
            }
          }
        }

        return count
      })";
    BytecodeExpectations expectations;
    auto *ast = expectations.Compile(src);

    // Try generating bytecode for this declaration
    auto unit = BytecodeGenerator::Compile(ast);

    unit->PrettyPrint(std::cout);

    VM::Execute(*unit, "test");

    // Expected total = 42 * 2 + 42 * 4 = 252
  }

  {
    auto src = R"(
      fun test() -> int32 {
        var i : int32
        var j : int32
        var n : int32 = 10
        var count : int32 = 0

        for (i = 0; i < n; i = i + 1) {
          for (j = 0; j < n; j = j + 1) {
            if ((i < 5 and j < 5) or (i >= 5 and j >= 5)) {
              if ((i < 2 and j < 2) or (i > 7 and j > 7)) {
                continue
              }
              count = count + 2
            } else {
              if ((i < 2 and j > 7) or (i > 7 and j < 2)) {
                continue
              }
              count = count + 4
            }
          }
        }

        return count
      })";
    BytecodeExpectations expectations;
    auto *ast = expectations.Compile(src);

    // Try generating bytecode for this declaration
    auto unit = BytecodeGenerator::Compile(ast);

    unit->PrettyPrint(std::cout);

    VM::Execute(*unit, "test");

    // Expected total = 42 * 2 + 42 * 4 = 252
  }
}

TEST_F(BytecodeGeneratorTest, BreakTest) {
  {
    auto src = R"(
      fun test() -> int32 {
        var i : int32 = 0
        var j : int32
        var n : int32 = 10
        var inf : int32 = 10000
        var count : int32 = 0

        for (i < inf) {
          i = i + 1
          if (i > n) {
            break
          }
          j = 0
          for (j < inf) {
            j = j + 1
            if (j > n) {
              break
            }
            count = count + 1
          }
        }

        return count
      })";
    BytecodeExpectations expectations;
    auto *ast = expectations.Compile(src);

    // Try generating bytecode for this declaration
    auto unit = BytecodeGenerator::Compile(ast);

    unit->PrettyPrint(std::cout);

    VM::Execute(*unit, "test");

    // Expected total = 10 * 10 = 100
  }

  {
    auto src = R"(
      fun test() -> int32 {
        var i : int32
        var j : int32
        var n : int32 = 10
        var inf : int32 = 10000
        var count : int32 = 0

        for (i = 0; i < inf; i = i + 1) {
          if (i == n) {
            break
          }
          for (j = 0; j < inf; j = j + 1) {
            if (j == n) {
              break
            }
            count = count + 1
          }
        }

        return count
      })";
    BytecodeExpectations expectations;
    auto *ast = expectations.Compile(src);

    // Try generating bytecode for this declaration
    auto unit = BytecodeGenerator::Compile(ast);

    unit->PrettyPrint(std::cout);

    VM::Execute(*unit, "test");

    // Expected total = 10 * 10 = 100
  }
}

}  // namespace tpl::vm::test