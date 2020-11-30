#include <utility>

#include <algorithm>
#include <functional>
#include <string>
#include <vector>

#include "parsing/parser.h"
#include "parsing/scanner.h"
#include "util/test_harness.h"

namespace tpl::parsing {

class ParserTest : public TplTest {
 public:
  ParserTest() : reporter_(), ctx_(&reporter_) {}

  ast::Context *context() { return &ctx_; }
  sema::ErrorReporter *reporter() { return &reporter_; }

 private:
  sema::ErrorReporter reporter_;
  ast::Context ctx_;
};

TEST_F(ParserTest, RegularForStmtTest) {
  const std::string source = R"(
    fun main() -> nil { for (var idx = 0; idx < 10; idx = idx + 1) { } }
  )";
  Scanner scanner(source);
  Parser parser(&scanner, context());

  // Attempt parse
  auto *ast = parser.Parse();
  ASSERT_NE(nullptr, ast);
  ASSERT_FALSE(reporter()->HasErrors());

  // No errors, move down AST
  ASSERT_TRUE(ast->IsFile());
  ASSERT_EQ(std::size_t{1}, ast->As<ast::File>()->GetDeclarations().size());

  // Only one function decl
  auto *decl = ast->As<ast::File>()->GetDeclarations()[0];
  ASSERT_TRUE(decl->IsFunctionDecl());

  auto *func_decl = decl->As<ast::FunctionDecl>();
  ASSERT_NE(nullptr, func_decl->GetFunctionLiteral());
  ASSERT_NE(nullptr, func_decl->GetFunctionLiteral()->GetBody());
  ASSERT_EQ(std::size_t{1}, func_decl->GetFunctionLiteral()->GetBody()->GetStatements().size());

  // Only one for statement, all elements are non-null
  auto *for_stmt =
      func_decl->GetFunctionLiteral()->GetBody()->GetStatements()[0]->SafeAs<ast::ForStmt>();
  ASSERT_NE(nullptr, for_stmt);
  ASSERT_NE(nullptr, for_stmt->GetInit());
  ASSERT_TRUE(for_stmt->GetInit()->IsDeclStmt());
  ASSERT_TRUE(for_stmt->GetInit()->As<ast::DeclStmt>()->GetDeclaration()->IsVariableDecl());
  ASSERT_NE(nullptr, for_stmt->GetCondition());
  ASSERT_NE(nullptr, for_stmt->GetNext());
}

TEST_F(ParserTest, ExhaustiveForStmtTest) {
  struct Test {
    const std::string source;
    bool init_null, cond_null, next_null;
    Test(std::string source, bool init_null, bool cond_null, bool next_null)
        : source(std::move(source)),
          init_null(init_null),
          cond_null(cond_null),
          next_null(next_null) {}
  };

  // All possible permutations of init, condition, and next statements in loops
  // clang-format off
  const Test tests[] = {
      {"fun main() -> nil { for (var idx = 0; idx < 10; idx = idx + 1) { } }", false, false, false},
      {"fun main() -> nil { for (var idx = 0; idx < 10; ) { } }",              false, false, true},
      {"fun main() -> nil { for (var idx = 0; ; idx = idx + 1) { } }",         false, true, false},
      {"fun main() -> nil { for (var idx = 0; ; ) { } }",                      false, true, true},
      {"fun main() -> nil { for (; idx < 10; idx = idx + 1) { } }",            true, false, false},
      {"fun main() -> nil { for (; idx < 10; ) { } }",                         true, false, true},
      {"fun main() -> nil { for (; ; idx = idx + 1) { } }",                    true, true, false},
      {"fun main() -> nil { for (; ; ) { } }",                                 true, true, true},
  };
  // clang-format on

  for (const auto &test : tests) {
    Scanner scanner(test.source);
    Parser parser(&scanner, context());

    // Attempt parse
    auto *ast = parser.Parse();
    ASSERT_NE(nullptr, ast);
    ASSERT_FALSE(reporter()->HasErrors());

    // No errors, move down AST
    ASSERT_TRUE(ast->IsFile());
    ASSERT_EQ(std::size_t{1}, ast->As<ast::File>()->GetDeclarations().size());

    // Only one function decl
    auto *decl = ast->As<ast::File>()->GetDeclarations()[0];
    ASSERT_TRUE(decl->IsFunctionDecl());

    auto *func_decl = decl->As<ast::FunctionDecl>();
    ASSERT_NE(nullptr, func_decl->GetFunctionLiteral());
    ASSERT_NE(nullptr, func_decl->GetFunctionLiteral()->GetBody());
    ASSERT_EQ(std::size_t{1}, func_decl->GetFunctionLiteral()->GetBody()->GetStatements().size());

    // Only one for statement, all elements are non-null
    auto *for_stmt =
        func_decl->GetFunctionLiteral()->GetBody()->GetStatements()[0]->SafeAs<ast::ForStmt>();
    ASSERT_NE(nullptr, for_stmt);
    ASSERT_EQ(test.init_null, for_stmt->GetInit() == nullptr);
    ASSERT_EQ(test.cond_null, for_stmt->GetCondition() == nullptr);
    ASSERT_EQ(test.next_null, for_stmt->GetNext() == nullptr);
  }
}

TEST_F(ParserTest, RegularForStmt_NoInitTest) {
  const std::string source = R"(
    fun main() -> nil {
      var idx = 0
      for (; idx < 10; idx = idx + 1) { }
    }
  )";
  Scanner scanner(source);
  Parser parser(&scanner, context());

  // Attempt parse
  auto *ast = parser.Parse();
  ASSERT_NE(nullptr, ast);
  ASSERT_FALSE(reporter()->HasErrors());

  // No errors, move down AST
  ASSERT_TRUE(ast->IsFile());
  ASSERT_EQ(std::size_t{1}, ast->As<ast::File>()->GetDeclarations().size());

  // Only one function decl
  auto *decl = ast->As<ast::File>()->GetDeclarations()[0];
  ASSERT_TRUE(decl->IsFunctionDecl());

  auto *func_decl = decl->As<ast::FunctionDecl>();
  ASSERT_NE(nullptr, func_decl->GetFunctionLiteral());
  ASSERT_NE(nullptr, func_decl->GetFunctionLiteral()->GetBody());
  ASSERT_EQ(std::size_t{2}, func_decl->GetFunctionLiteral()->GetBody()->GetStatements().size());

  // Two statements in function

  // First is the variable declaration
  auto &block = func_decl->GetFunctionLiteral()->GetBody()->GetStatements();
  ASSERT_TRUE(block[0]->IsDeclStmt());
  ASSERT_TRUE(block[0]->As<ast::DeclStmt>()->GetDeclaration()->IsVariableDecl());

  // Next is the for statement
  auto *for_stmt = block[1]->SafeAs<ast::ForStmt>();
  ASSERT_NE(nullptr, for_stmt);
  ASSERT_EQ(nullptr, for_stmt->GetInit());
  ASSERT_NE(nullptr, for_stmt->GetCondition());
  ASSERT_NE(nullptr, for_stmt->GetNext());
}

TEST_F(ParserTest, RegularForStmt_WhileTest) {
  const std::string for_while_sources[] = {
      R"(
      fun main() -> nil {
        var idx = 0
        for (idx < 10) { idx = idx + 1 }
      }
      )",
      R"(
      fun main() -> nil {
        var idx = 0
        for (; idx < 10; ) { idx = idx + 1 }
      }
      )",
  };

  for (const auto &source : for_while_sources) {
    Scanner scanner(source);
    Parser parser(&scanner, context());

    // Attempt parse
    auto *ast = parser.Parse();
    ASSERT_NE(nullptr, ast);
    ASSERT_FALSE(reporter()->HasErrors());

    // No errors, move down AST
    ASSERT_TRUE(ast->IsFile());
    ASSERT_EQ(std::size_t{1}, ast->As<ast::File>()->GetDeclarations().size());

    // Only one function decl
    auto *decl = ast->As<ast::File>()->GetDeclarations()[0];
    ASSERT_TRUE(decl->IsFunctionDecl());

    auto *func_decl = decl->As<ast::FunctionDecl>();
    ASSERT_NE(nullptr, func_decl->GetFunctionLiteral());
    ASSERT_NE(nullptr, func_decl->GetFunctionLiteral()->GetBody());
    ASSERT_EQ(std::size_t{2}, func_decl->GetFunctionLiteral()->GetBody()->GetStatements().size());

    // Two statements in function

    // First is the variable declaration
    auto &block = func_decl->GetFunctionLiteral()->GetBody()->GetStatements();
    ASSERT_TRUE(block[0]->IsDeclStmt());
    ASSERT_TRUE(block[0]->As<ast::DeclStmt>()->GetDeclaration()->IsVariableDecl());

    // Next is the for statement
    auto *for_stmt = block[1]->SafeAs<ast::ForStmt>();
    ASSERT_NE(nullptr, for_stmt);
    ASSERT_EQ(nullptr, for_stmt->GetInit());
    ASSERT_NE(nullptr, for_stmt->GetCondition());
    ASSERT_EQ(nullptr, for_stmt->GetNext());
  }
}

TEST_F(ParserTest, RegularForInStmtTest) {
  const std::string source = R"(
    fun main() -> nil {
      for (idx in range()) { }
    }
  )";
  Scanner scanner(source);
  Parser parser(&scanner, context());

  // Attempt parse
  auto *ast = parser.Parse();
  ASSERT_NE(nullptr, ast);
  ASSERT_FALSE(reporter()->HasErrors());

  // No errors, move down AST
  ASSERT_TRUE(ast->IsFile());
  ASSERT_EQ(std::size_t{1}, ast->As<ast::File>()->GetDeclarations().size());

  // Only one function decl
  auto *decl = ast->As<ast::File>()->GetDeclarations()[0];
  ASSERT_TRUE(decl->IsFunctionDecl());

  auto *func_decl = decl->As<ast::FunctionDecl>();
  ASSERT_NE(nullptr, func_decl->GetFunctionLiteral());
  ASSERT_NE(nullptr, func_decl->GetFunctionLiteral()->GetBody());
  ASSERT_EQ(std::size_t{1}, func_decl->GetFunctionLiteral()->GetBody()->GetStatements().size());

  // Only statement is the for-in statement
  auto &block = func_decl->GetFunctionLiteral()->GetBody()->GetStatements();
  auto *for_in_stmt = block[0]->SafeAs<ast::ForInStmt>();
  ASSERT_NE(nullptr, for_in_stmt);
  ASSERT_NE(nullptr, for_in_stmt->Target());
  ASSERT_NE(nullptr, for_in_stmt->Iterable());
}

TEST_F(ParserTest, ArrayTypeTest) {
  struct TestCase {
    std::string source;
    bool valid;
  };

  TestCase tests[] = {
      // Array with unknown length = valid
      {"fun main(arr: [*]int32) -> nil { }", true},
      // Array with known length = valid
      {"fun main() -> nil { var arr: [10]int32 }", true},
      // Array with missing length field = invalid
      {"fun main(arr: []int32) -> nil { }", false},
  };

  for (const auto &test_case : tests) {
    Scanner scanner(test_case.source);
    Parser parser(&scanner, context());

    // Attempt parse
    auto *ast = parser.Parse();
    ASSERT_NE(nullptr, ast);
    EXPECT_EQ(test_case.valid, !reporter()->HasErrors());
    reporter()->Reset();
  }
}

}  // namespace tpl::parsing
