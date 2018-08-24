#include "gtest/gtest.h"

#include "ast/ast.h"
#include "ast/ast_node_factory.h"

namespace tpl::ast::test {

class AstTest : public ::testing::Test {
 public:
  AstTest() : region_("ast_test"), pos_() {}

  util::Region &region() { return region_; }

  const SourcePosition &empty_pos() const { return pos_; }

 private:
  util::Region region_;

  SourcePosition pos_;
};

TEST_F(AstTest, HierechyTest) {
  AstNodeFactory factory(region());

#define CHECK_NODE_IS_NOT_KIND(n) \
  EXPECT_FALSE(node->Is<n>())     \
      << "Node " << node->kind_name() << " is not " << #n;

#define IS_MATCH(n) node->Is<n>() +
#define COUNT_MATCHES(NODE_LIST) NODE_LIST(IS_MATCH) 0
#define CSL(n) #n << ", "

  /// Test declarations
  {
    AstNode *all_decls[] = {
        factory.NewFieldDeclaration(empty_pos(), Identifier(nullptr), nullptr),
        factory.NewFunctionDeclaration(empty_pos(), Identifier(nullptr),
                                       nullptr),
        factory.NewStructDeclaration(empty_pos(), Identifier(nullptr), nullptr),
        factory.NewVariableDeclaration(empty_pos(), Identifier(nullptr),
                                       nullptr, nullptr),
    };

    for (const auto *node : all_decls) {
      // Ensure declarations aren't expressions, types or statements
      STATEMENT_NODES(CHECK_NODE_IS_NOT_KIND)
      EXPRESSION_NODES(CHECK_NODE_IS_NOT_KIND)

      // Ensure concrete declarations are also a base declaration type
      EXPECT_TRUE(node->Is<Declaration>())
          << "Node " << node->kind_name()
          << " isn't an Declaration? Ensure Declaration::classof() handles all "
             "cases if you've added a new Declaration node.";

      // Each declaration must match only one other declaration type (itself)
      EXPECT_EQ(1, COUNT_MATCHES(DECLARATION_NODES))
          << node->kind_name() << " matches more than one of "
          << DECLARATION_NODES(CSL);
    }
  }

  /// Test expressions
  {
    AstNode *all_exprs[] = {
        factory.NewBinaryExpression(empty_pos(), parsing::Token::Type::PLUS,
                                    nullptr, nullptr),
        factory.NewCallExpression(factory.NewNilLiteral(empty_pos()),
                                  util::RegionVector<Expression *>(region())),
        factory.NewFunctionLiteral(
            factory.NewFunctionType(
                empty_pos(), util::RegionVector<FieldDeclaration *>(region()),
                nullptr),
            nullptr),
        factory.NewNilLiteral(empty_pos()),
        factory.NewUnaryExpression(empty_pos(), parsing::Token::Type::MINUS,
                                   nullptr),
        factory.NewIdentifierExpression(empty_pos(), Identifier(nullptr)),
        factory.NewArrayType(empty_pos(), nullptr, nullptr),
        factory.NewFunctionType(
            empty_pos(), util::RegionVector<FieldDeclaration *>(region()),
            nullptr),
        factory.NewPointerType(empty_pos(), nullptr),
        factory.NewStructType(empty_pos(),
                              util::RegionVector<FieldDeclaration *>(region())),
    };

    for (const auto *node : all_exprs) {
      // Ensure expressions aren't declarations, types or statements
      DECLARATION_NODES(CHECK_NODE_IS_NOT_KIND)
      STATEMENT_NODES(CHECK_NODE_IS_NOT_KIND)

      // Ensure concrete expressions are also a base expression type
      EXPECT_TRUE(node->Is<Expression>())
          << "Node " << node->kind_name()
          << " isn't an Expression? Ensure Expression::classof() handles all "
             "cases if you've added a new Expression node.";

      // Each expression must match only one other expression type (itself)
      EXPECT_EQ(1, COUNT_MATCHES(EXPRESSION_NODES))
          << node->kind_name() << " matches more than one of "
          << EXPRESSION_NODES(CSL);
    }
  }

  /// Test statements
  {
    AstNode *all_stmts[] = {
        factory.NewBlockStatement(empty_pos(), empty_pos(),
                                  util::RegionVector<Statement *>(region())),
        factory.NewDeclarationStatement(factory.NewVariableDeclaration(
            empty_pos(), Identifier(nullptr), nullptr, nullptr)),
        factory.NewExpressionStatement(factory.NewNilLiteral(empty_pos())),
        factory.NewForStatement(empty_pos(), nullptr, nullptr, nullptr,
                                nullptr),
        factory.NewIfStatement(empty_pos(), nullptr, nullptr, nullptr),
        factory.NewReturnStatement(empty_pos(), nullptr),
    };

    for (const auto *node : all_stmts) {
      // Ensure statements aren't declarations, types or expressions
      DECLARATION_NODES(CHECK_NODE_IS_NOT_KIND)
      EXPRESSION_NODES(CHECK_NODE_IS_NOT_KIND)

      // Ensure concrete expressions are also a base expression type
      EXPECT_TRUE(node->Is<Statement>())
          << "Node " << node->kind_name()
          << " isn't an Statement? Ensure Statement::classof() handles all "
             "cases if you've added a new Statement node.";

      // Each expression must match only one other expression type (itself)
      EXPECT_EQ(1, COUNT_MATCHES(STATEMENT_NODES))
          << node->kind_name() << " matches more than one of "
          << STATEMENT_NODES(CSL);
    }
  }
}

}  // namespace tpl::ast::test
