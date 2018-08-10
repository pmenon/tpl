#pragma once

#include "ast/ast.h"
#include "util/region.h"

namespace tpl {

/**
 * A factory for AST nodes. This factory uses a region allocator to quickly
 * allocate AST nodes during parsing. The assumption here is that the nodes are
 * only required during parsing and are thrown away after code generation, hence
 * require quick deallocation as well, thus the use of a region.
 */
class AstNodeFactory {
 public:
  explicit AstNodeFactory(Region &region) : region_(region) {}

  Region &region() { return region_; }

  FunctionDeclaration *NewFunctionDeclaration(const AstString *name,
                                              FunctionLiteralExpression *fun) {
    return new (region_) FunctionDeclaration(name, fun);
  }

  StructDeclaration *NewStructDeclaration(const AstString *name,
                                          StructType *type) {
    return new (region_) StructDeclaration(name, type);
  }

  VariableDeclaration *NewVariableDeclaration(const AstString *name, Type *type,
                                              Expression *init) {
    return new (region_) VariableDeclaration(name, type, init);
  }

  BlockStatement *NewBlockStatement(
      util::RegionVector<Statement *> &&statements) {
    return new (region_) BlockStatement(std::move(statements));
  }

  DeclarationStatement *NewDeclarationStatement(Declaration *decl) {
    return new (region_) DeclarationStatement(decl);
  }

  ExpressionStatement *NewExpressionStatement(Expression *expression) {
    return new (region_) ExpressionStatement(expression);
  }

  IfStatement *NewIfStatement(Expression *cond, Statement *then_stmt,
                              Statement *else_stmt) {
    return new (region_) IfStatement(cond, then_stmt, else_stmt);
  }

  ReturnStatement *NewReturnStatement(Expression *ret) {
    return new (region_) ReturnStatement(ret);
  }

  BinaryExpression *NewBinaryExpression(Token::Type op, AstNode *left,
                                        AstNode *right) {
    return new (region_) BinaryExpression(op, left, right);
  }

  LiteralExpression *NewNilLiteral() {
    return new (region_) LiteralExpression();
  }

  LiteralExpression *NewBoolLiteral(bool val) {
    return new (region_) LiteralExpression(val);
  }

  LiteralExpression *NewNumLiteral(AstString *num) {
    return new (region_)
        LiteralExpression(LiteralExpression::Type::Number, num);
  }

  LiteralExpression *NewStringLiteral(AstString *str) {
    return new (region_)
        LiteralExpression(LiteralExpression::Type::String, str);
  }

  UnaryExpression *NewUnaryExpression(Token::Type op, AstNode *expr) {
    return new (region_) UnaryExpression(op, expr);
  }

  VarExpression *NewVarExpression(AstString *name) {
    return new (region_) VarExpression(name);
  }

 private:
  Region &region_;
};

}  // namespace tpl