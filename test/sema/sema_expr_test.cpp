#include "tpl_test.h"

#include "ast/context.h"
#include "ast/ast_node_factory.h"
#include "ast/type.h"
#include "sema/sema.h"
#include "util/region_containers.h"

namespace tpl::sema::test {

class SemaExprTest : public TplTest {
 public:
  SemaExprTest()
      : region_("test"),
        error_reporter_(region()),
        ctx_(region(), error_reporter_) {}

  util::Region *region() { return &region_; }
  ErrorReporter *error_reporter() { return &error_reporter_; }
  ast::Context *ctx() { return &ctx_; }
  ast::AstNodeFactory *node_factory() { return &ctx_.node_factory(); }

  ast::Identifier Ident(const std::string &s) {
    return ctx()->GetIdentifier(s);
  }

  ast::Expr *IdentExpr(const std::string &s) {
    return node_factory()->NewIdentifierExpr(empty_, Ident(s));
  }

  ast::Expr *BoolLit(bool b) {
    return node_factory()->NewBoolLiteral(empty_, b);
  }

  ast::Expr *IntLit(u32 i) { return node_factory()->NewIntLiteral(empty_, i); }

  ast::Expr *CmpLt(ast::Expr *left, ast::Expr *right) {
    return node_factory()->NewComparisonOpExpr(
        empty_, parsing::Token::Type::LESS, left, right);
  }

  ast::Expr *Field(ast::Expr *obj, ast::Expr *field) {
    return node_factory()->NewMemberExpr(empty_, obj, field);
  }

  ast::VariableDecl *DeclVar(ast::Identifier name, ast::Expr *init) {
    return node_factory()->NewVariableDecl(empty_, name, nullptr, init);
  }

  ast::Stmt *DeclStmt(ast::Decl *decl) {
    return node_factory()->NewDeclStmt(decl);
  }

  ast::Stmt *ScanTable(
      const std::string &table_name,
      const std::function<void(const std::string &,
                               util::RegionVector<ast::Stmt *> &)> &body) {
    util::RegionVector<ast::Stmt *> stmts(region());
    body("row", stmts);

    return node_factory()->NewForInStmt(
        empty_, IdentExpr("row"), IdentExpr(table_name), nullptr,
        node_factory()->NewBlockStmt(empty_, empty_, std::move(stmts)));
  }

 private:
  util::Region region_;
  ErrorReporter error_reporter_;
  ast::Context ctx_;

  SourcePosition empty_{0, 0};
};

struct TestCase {
  bool has_errors;
  std::string msg;
  ast::AstNode *tree;
};

TEST_F(SemaExprTest, LogicalOperationTest) {
  SourcePosition empty{0, 0};

  TestCase tests[] = {
      /*
       * Test: 1 and 2
       * Expectation: Error
       */
      {true, "1 and 2 is not a valid logical operation",
       node_factory()->NewBinaryOpExpr(
           empty, parsing::Token::Type::AND,
           node_factory()->NewIntLiteral(empty, 1),
           node_factory()->NewIntLiteral(empty, 2))},
      /*
       * Test: 1 and true
       * Expectation: Error
       */
      {true, "1 and true is not a valid logical operation",
       node_factory()->NewBinaryOpExpr(
           empty, parsing::Token::Type::AND,
           node_factory()->NewIntLiteral(empty, 1),
           node_factory()->NewBoolLiteral(empty, true))},

      /*
       * Test: false and 2
       * Expectation: Error
       */
      {true, "false and 1 is not a valid logical operation",
       node_factory()->NewBinaryOpExpr(
           empty, parsing::Token::Type::AND,
           node_factory()->NewBoolLiteral(empty, false),
           node_factory()->NewIntLiteral(empty, 2))},

      /*
       * Test: false and true
       * Expectation: Valid
       */
      {false, "false and true is a valid logical operation",
       node_factory()->NewBinaryOpExpr(
           empty, parsing::Token::Type::AND,
           node_factory()->NewBoolLiteral(empty, false),
           node_factory()->NewBoolLiteral(empty, true))},
  };

  for (const auto &test : tests) {
    Sema sema(*ctx());
    bool errors = sema.Run(test.tree);

    if (test.has_errors) {
      EXPECT_TRUE(errors) << test.msg;
    } else {
      EXPECT_FALSE(errors) << test.msg;
    }

    error_reporter()->Reset();
  }
}

TEST_F(SemaExprTest, ComparisonOperationWithImplicitCastTest) {
  std::vector<TestCase> tests;

  /*
   * Test: for (row in test_1) { var x = (row.colA < false) }
   * Expectation: Error
   */
  {
    TestCase t;
    t.has_errors = true;
    t.msg = "row.colA < false is not a valid comparison";

    t.tree = ScanTable("test_1", [this](auto row, auto &stmts) {
      stmts.push_back(DeclStmt(
          DeclVar(Ident("x"),
                  CmpLt(Field(IdentExpr("row"), BoolLit(false)), IntLit(10)))));
    });

    tests.emplace_back(t);
  }

  /*
   * Test: for (row in test_1) { var x = (row.colA < 10) }
   * Expectation: Valid
   */
  {
    TestCase t;
    t.has_errors = false;
    t.msg = "row.colA < 10 should implicitly cast the literal to a SQL value";

    t.tree = ScanTable("test_1", [this](auto row, auto &stmts) {
      stmts.push_back(DeclStmt(DeclVar(
          Ident("x"),
          CmpLt(Field(IdentExpr("row"), IdentExpr("colA")), IntLit(10)))));
    });

    tests.emplace_back(t);
  }

  for (const auto &test : tests) {
    Sema sema(*ctx());
    bool errors = sema.Run(test.tree);

    if (test.has_errors) {
      EXPECT_TRUE(errors) << test.msg;
    } else {
      EXPECT_FALSE(errors) << test.msg;
    }

    error_reporter()->Reset();
  }
}

}  // namespace tpl::sema::test
