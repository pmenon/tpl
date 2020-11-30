#include "ast/ast.h"
#include "ast/ast_node_factory.h"
#include "util/test_harness.h"

namespace tpl::ast {

class AstTest : public TplTest {
 public:
  AstTest() : region_("ast_test"), pos_() {}

  util::Region *region() { return &region_; }

  const SourcePosition &empty_pos() const { return pos_; }

 private:
  util::Region region_;

  SourcePosition pos_;
};

TEST_F(AstTest, HierechyTest) {
  AstNodeFactory factory(region());

#define CHECK_NODE_IS_NOT_KIND(n) \
  EXPECT_FALSE(node->Is<n>()) << "Node " << node->KindName() << " is not " << #n;

#define IS_MATCH(n) node->Is<n>() +
#define COUNT_MATCHES(NODE_LIST) NODE_LIST(IS_MATCH) 0
#define CSL(n) #n << ", "

  /// Test declarations
  {
    AstNode *all_decls[] = {
        factory.NewFieldDeclaration(empty_pos(), Identifier(), nullptr),
        factory.NewFunctionDeclaration(
            empty_pos(), Identifier(),
            factory.NewFunctionLiteralExpression(
                factory.NewFunctionType(empty_pos(),
                                        util::RegionVector<FieldDeclaration *>(region()), nullptr),
                nullptr)),
        factory.NewStructDeclaration(empty_pos(), Identifier(), nullptr),
        factory.NewVariableDeclaration(empty_pos(), Identifier(), nullptr, nullptr),
    };

    for (const auto *node : all_decls) {
      // Ensure declarations aren't expressions, types or statements
      STATEMENT_NODES(CHECK_NODE_IS_NOT_KIND)
      EXPRESSION_NODES(CHECK_NODE_IS_NOT_KIND)

      // Ensure concrete declarations are also a base declaration type
      EXPECT_TRUE(node->Is<Declaration>()) << "Node " << node->KindName()
                                           << " isn't an Decl? Ensure Decl::classof() handles all "
                                              "cases if you've added a new Decl node.";

      // Each declaration must match only one other declaration type (itself)
      EXPECT_EQ(1, COUNT_MATCHES(DECLARATION_NODES))
          << node->KindName() << " matches more than one of " << DECLARATION_NODES(CSL);
    }
  }

  /// Test expressions
  {
    AstNode *all_exprs[] = {
        factory.NewBinaryOpExpression(empty_pos(), parsing::Token::Type::PLUS, nullptr, nullptr),
        factory.NewCallExpression(factory.NewNilLiteral(empty_pos()),
                                  util::RegionVector<Expression *>(region())),
        factory.NewFunctionLiteralExpression(
            factory.NewFunctionType(empty_pos(), util::RegionVector<FieldDeclaration *>(region()),
                                    nullptr),
            nullptr),
        factory.NewNilLiteral(empty_pos()),
        factory.NewUnaryOpExpression(empty_pos(), parsing::Token::Type::MINUS, nullptr),
        factory.NewIdentifierExpression(empty_pos(), Identifier()),
        factory.NewArrayType(empty_pos(), nullptr, nullptr),
        factory.NewFunctionType(empty_pos(), util::RegionVector<FieldDeclaration *>(region()),
                                nullptr),
        factory.NewPointerType(empty_pos(), nullptr),
        factory.NewStructType(empty_pos(), util::RegionVector<FieldDeclaration *>(region())),
    };

    for (const auto *node : all_exprs) {
      // Ensure expressions aren't declarations, types or statements
      DECLARATION_NODES(CHECK_NODE_IS_NOT_KIND)
      STATEMENT_NODES(CHECK_NODE_IS_NOT_KIND)

      // Ensure concrete expressions are also a base expression type
      EXPECT_TRUE(node->Is<Expression>())
          << "Node " << node->KindName()
          << " isn't an Expression? Ensure Expression::classof() handles all "
             "cases if you've added a new Expression node.";

      // Each expression must match only one other expression type (itself)
      EXPECT_EQ(1, COUNT_MATCHES(EXPRESSION_NODES))
          << node->KindName() << " matches more than one of " << EXPRESSION_NODES(CSL);
    }
  }

  /// Test statements
  {
    AstNode *all_stmts[] = {
        factory.NewBlockStatement(empty_pos(), empty_pos(),
                                  util::RegionVector<Statement *>(region())),
        factory.NewDeclStatement(
            factory.NewVariableDeclaration(empty_pos(), Identifier(), nullptr, nullptr)),
        factory.NewExpressionStatement(factory.NewNilLiteral(empty_pos())),
        factory.NewForStatement(empty_pos(), nullptr, nullptr, nullptr, nullptr),
        factory.NewIfStatement(empty_pos(), nullptr, nullptr, nullptr),
        factory.NewReturnStatement(empty_pos(), nullptr),
    };

    for (const auto *node : all_stmts) {
      // Ensure statements aren't declarations, types or expressions
      DECLARATION_NODES(CHECK_NODE_IS_NOT_KIND)
      EXPRESSION_NODES(CHECK_NODE_IS_NOT_KIND)

      // Ensure concrete expressions are also a base expression type
      EXPECT_TRUE(node->Is<Statement>())
          << "Node " << node->KindName()
          << " isn't an Statement? Ensure Statement::classof() handles all "
             "cases if you've added a new Statement node.";

      // Each expression must match only one other expression type (itself)
      EXPECT_EQ(1, COUNT_MATCHES(STATEMENT_NODES))
          << node->KindName() << " matches more than one of " << STATEMENT_NODES(CSL);
    }
  }
}

}  // namespace tpl::ast
