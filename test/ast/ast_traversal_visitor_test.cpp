#include <string>

#include "ast/ast.h"
#include "ast/ast_node_factory.h"
#include "ast/ast_traversal_visitor.h"
#include "parsing/parser.h"
#include "parsing/scanner.h"
#include "sema/sema.h"
#include "util/test_harness.h"

namespace tpl::ast {

class AstTraversalVisitorTest : public TplTest {
 public:
  AstTraversalVisitorTest() : error_reporter_(), context_(&error_reporter_) {}

  AstNode *GenerateAst(const std::string &src) {
    parsing::Scanner scanner(src);
    parsing::Parser parser(&scanner, Ctx());

    if (ErrorReporter()->HasErrors()) {
      ErrorReporter()->PrintErrors(std::cerr);
      return nullptr;
    }

    auto *root = parser.Parse();

    sema::Sema sema(Ctx());
    auto check = sema.Run(root);

    EXPECT_FALSE(check);

    return root;
  }

 private:
  sema::ErrorReporter *ErrorReporter() { return &error_reporter_; }

  ast::Context *Ctx() { return &context_; }

 private:
  sema::ErrorReporter error_reporter_;
  ast::Context context_;
};

namespace {

// Visit to find FOR loops only
template <bool FindInfinite = false>
class ForFinder : public AstTraversalVisitor<ForFinder<FindInfinite>> {
  using SelfT = ForFinder<FindInfinite>;

 public:
  explicit ForFinder(ast::AstNode *root) : AstTraversalVisitor<SelfT>(root), num_fors_(0) {}

  void VisitForStatement(ast::ForStatement *stmt) {
    if constexpr (FindInfinite) {
      bool is_finite_for = (stmt->GetCondition() == nullptr);
      num_fors_ += static_cast<uint32_t>(is_finite_for);
    } else {  // NOLINT
      num_fors_++;
    }
    AstTraversalVisitor<SelfT>::VisitForStatement(stmt);
  }

  uint32_t num_fors() const { return num_fors_; }

 private:
  uint32_t num_fors_;
};

}  // namespace

TEST_F(AstTraversalVisitorTest, CountForLoopsTest) {
  // No for-loops
  {
    const auto src = R"(
    fun test(x: uint32) -> uint32 {
      var y : uint32 = 20
      return x * y
    })";

    auto *root = GenerateAst(src);

    ForFinder finder(root);
    finder.Run();

    EXPECT_EQ(0u, finder.num_fors());
  }

  // 1 for-loop
  {
    const auto src = R"(
    fun test(x: int) -> int {
      for (x < 10) { }
      return 0
    })";

    auto *root = GenerateAst(src);

    ForFinder finder(root);
    finder.Run();

    EXPECT_EQ(1u, finder.num_fors());
  }

  // 4 nested for-loops
  {
    const auto src = R"(
    fun test(x: int) -> int {
      for (x < 10) {
        for {
          for {
            for { }
          }
        }
      }
      return 0
    })";

    auto *root = GenerateAst(src);

    ForFinder<false> finder(root);
    ForFinder<true> inf_finder(root);

    finder.Run();
    inf_finder.Run();

    EXPECT_EQ(4u, finder.num_fors());
    EXPECT_EQ(3u, inf_finder.num_fors());
  }

  // 4 sequential for-loops
  {
    const auto src = R"(
    fun test(x: int) -> int {
      for {}
      for {}
      for {}
      for {}
      return 0
    })";

    auto *root = GenerateAst(src);

    ForFinder finder(root);
    finder.Run();

    EXPECT_EQ(4u, finder.num_fors());
  }
}

namespace {

// Visitor to find function declarations or, optionally, all function literals
// (i.e., lambdas) as well.
template <bool CountLiterals = false>
class FunctionFinder : public AstTraversalVisitor<FunctionFinder<CountLiterals>> {
  using SelfT = FunctionFinder<CountLiterals>;

 public:
  explicit FunctionFinder(ast::AstNode *root) : AstTraversalVisitor<SelfT>(root), num_funcs_(0) {}

  void VisitFunctionDeclaration(ast::FunctionDeclaration *decl) {
    if constexpr (!CountLiterals) {
      num_funcs_++;
    }
    AstTraversalVisitor<SelfT>::VisitFunctionDeclaration(decl);
  }

  void VisitFunctionLiteralExpr(ast::FunctionLiteralExpr *expr) {
    if constexpr (CountLiterals) {
      num_funcs_++;
    }
    AstTraversalVisitor<SelfT>::VisitFunctionLiteralExpr(expr);
  }

  uint32_t NumFunctions() const { return num_funcs_; }

 private:
  uint32_t num_funcs_;
};

}  // namespace

TEST_F(AstTraversalVisitorTest, CountFunctionsTest) {
  // Function declarations only
  {
    const auto src = R"(
      fun f1(x: int) -> void { }
      fun f2(x: int) -> void { }
    )";

    auto *root = GenerateAst(src);

    FunctionFinder<false> find_func_decls(root);
    FunctionFinder<true> find_all_funcs(root);

    find_func_decls.Run();
    find_all_funcs.Run();

    EXPECT_EQ(2u, find_func_decls.NumFunctions());
    EXPECT_EQ(2u, find_all_funcs.NumFunctions());
  }

  // Function declarations and literals
  {
    const auto src = R"(
      fun f1(x: int) -> void { }
      fun f2(x: int) -> void {
        var x = fun(xx:int) -> int { return xx * 2 }
      }
    )";

    auto *root = GenerateAst(src);

    FunctionFinder<false> find_func_decls(root);
    FunctionFinder<true> find_all_funcs(root);

    find_func_decls.Run();
    find_all_funcs.Run();

    EXPECT_EQ(2u, find_func_decls.NumFunctions());
    EXPECT_EQ(3u, find_all_funcs.NumFunctions());
  }
}

namespace {

class IfFinder : public AstTraversalVisitor<IfFinder> {
 public:
  explicit IfFinder(ast::AstNode *root) : AstTraversalVisitor(root), num_ifs_(0) {}

  void VisitIfStatement(ast::IfStatement *stmt) {
    num_ifs_++;
    AstTraversalVisitor<IfFinder>::VisitIfStatement(stmt);
  }

  uint32_t NumIfs() const { return num_ifs_; }

 private:
  uint32_t num_ifs_;
};

}  // namespace

TEST_F(AstTraversalVisitorTest, CountIfTest) {
  // Nestes Ifs
  {
    const auto src = R"(
      fun f1(x: int) -> void {
        if (x < 10) {
          if (x < 5) {
            if (x < 2) { }
            else {}
          }
        } else if (x < 20) {
          if (x < 15) { }
          else if (x < 12) { }
        } else { }
      }
    )";

    auto *root = GenerateAst(src);

    IfFinder finder(root);

    finder.Run();

    EXPECT_EQ(6u, finder.NumIfs());
  }

  // Serial Ifs
  {
    const auto src = R"(
      fun f1(x: int) -> void {
        if (x < 10) { }
        else if (x < 5) { }

        if (x < 2) { }
        else { }

        if (x < 20) { }
        if (x < 15) { }
        else { }
      }
    )";

    auto *root = GenerateAst(src);

    IfFinder finder(root);

    finder.Run();

    EXPECT_EQ(5u, finder.NumIfs());
  }
}

}  // namespace tpl::ast
