#include "gtest/gtest.h"

#include "ast/ast.h"
#include "ast/ast_node_factory.h"

namespace tpl {
namespace ast {
namespace test {

class AstTest : public ::testing::Test {
 public:
  AstTest() : region_("ast_test") {}

  util::Region &region() { return region_; }

 private:
  util::Region region_;
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
        factory.NewFunctionDeclaration(nullptr, nullptr),
        factory.NewStructDeclaration(nullptr, nullptr),
        factory.NewVariableDeclaration(nullptr, nullptr, nullptr),
    };

    for (const auto *node : all_decls) {
      // Ensure declarations aren't expressions, types or statements
      STATEMENT_NODES(CHECK_NODE_IS_NOT_KIND)
      EXPRESSION_NODES(CHECK_NODE_IS_NOT_KIND)
      TYPE_NODES(CHECK_NODE_IS_NOT_KIND)

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
        factory.NewBinaryExpression(parsing::Token::Type::PLUS, nullptr,
                                    nullptr),
        factory.NewCallExpression(nullptr,
                                  util::RegionVector<Expression *>(region())),
        factory.NewFunctionLiteral(nullptr, nullptr),
        factory.NewNilLiteral(),
        factory.NewUnaryExpression(parsing::Token::Type::MINUS, nullptr),
        factory.NewVarExpression(nullptr),
    };

    for (const auto *node : all_exprs) {
      // Ensure expressions aren't declarations, types or statements
      DECLARATION_NODES(CHECK_NODE_IS_NOT_KIND)
      STATEMENT_NODES(CHECK_NODE_IS_NOT_KIND)
      TYPE_NODES(CHECK_NODE_IS_NOT_KIND)

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
        factory.NewBlockStatement(util::RegionVector<Statement *>(region())),
        factory.NewDeclarationStatement(nullptr),
        factory.NewExpressionStatement(nullptr),
        factory.NewIfStatement(nullptr, nullptr, nullptr),
        factory.NewReturnStatement(nullptr),
    };

    for (const auto *node : all_stmts) {
      // Ensure statements aren't declarations, types or expressions
      DECLARATION_NODES(CHECK_NODE_IS_NOT_KIND)
      EXPRESSION_NODES(CHECK_NODE_IS_NOT_KIND)
      TYPE_NODES(CHECK_NODE_IS_NOT_KIND)

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

  /// Test types
  {
    AstNode *all_stmts[] = {
        factory.NewArrayType(nullptr, nullptr),
        factory.NewFunctionType(util::RegionVector<Field *>(region()), nullptr),
        factory.NewIdentifierType(nullptr),
        factory.NewPointerType(nullptr),
        factory.NewStructType(util::RegionVector<Field *>(region())),
    };

    for (const auto *node : all_stmts) {
      // Ensure types aren't declarations, statements or expressions
      DECLARATION_NODES(CHECK_NODE_IS_NOT_KIND)
      EXPRESSION_NODES(CHECK_NODE_IS_NOT_KIND)
      STATEMENT_NODES(CHECK_NODE_IS_NOT_KIND)

      // Ensure concrete types are also a base type
      EXPECT_TRUE(node->Is<Type>())
          << "Node " << node->kind_name()
          << " isn't a Type? Ensure Type::classof() handles all "
             "cases if you've added a new Type node.";

      // Each expression must match only one other expression type (itself)
      EXPECT_EQ(1, COUNT_MATCHES(TYPE_NODES))
          << node->kind_name() << " matches more than one of "
          << TYPE_NODES(CSL);
    }
  }
}

}  // namespace test
}  // namespace ast
}  // namespace tpl
