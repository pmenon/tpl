#pragma once

#include "ast/ast.h"
#include "util/region.h"

namespace tpl::ast {

/**
 * A factory for AST nodes. This factory uses a region allocator to quickly
 * allocate AST nodes during parsing. The assumption here is that the nodes are
 * only required during parsing and are thrown away after code generation, hence
 * require quick deallocation as well, thus the use of a region.
 */
class AstNodeFactory {
 public:
  explicit AstNodeFactory(util::Region &region) : region_(region) {}

  util::Region &region() { return region_; }

  File *NewFile(util::RegionVector<Declaration *> &&declarations) {
    return new (region_) File(std::move(declarations));
  }

  FunctionDeclaration *NewFunctionDeclaration(const AstString *name,
                                              FunctionLiteralExpression *fun) {
    return new (region_) FunctionDeclaration(name, fun);
  }

  StructDeclaration *NewStructDeclaration(const AstString *name,
                                          StructType *type) {
    return new (region_) StructDeclaration(name, type);
  }

  VariableDeclaration *NewVariableDeclaration(const AstString *name,
                                              Expression *type,
                                              Expression *init) {
    return new (region_) VariableDeclaration(name, type, init);
  }

  BadStatement *NewBadStatement(uint64_t pos) {
    return new (region_) BadStatement(pos);
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

  ForStatement *NewForStatement(Statement *init, Expression *cond,
                                Statement *next, BlockStatement *body) {
    return new (region_) ForStatement(init, cond, next, body);
  }

  IfStatement *NewIfStatement(Expression *cond, BlockStatement *then_stmt,
                              Statement *else_stmt) {
    return new (region_) IfStatement(cond, then_stmt, else_stmt);
  }

  ReturnStatement *NewReturnStatement(Expression *ret) {
    return new (region_) ReturnStatement(ret);
  }

  BadExpression *NewBadExpression(uint64_t pos) {
    return new (region_) BadExpression(pos);
  }

  BinaryExpression *NewBinaryExpression(parsing::Token::Type op,
                                        Expression *left, Expression *right) {
    return new (region_) BinaryExpression(op, left, right);
  }

  CallExpression *NewCallExpression(Expression *fun,
                                    util::RegionVector<Expression *> &&args) {
    return new (region_) CallExpression(fun, std::move(args));
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

  FunctionLiteralExpression *NewFunctionLiteral(FunctionType *type,
                                                BlockStatement *body) {
    return new (region_) FunctionLiteralExpression(type, body);
  }

  UnaryExpression *NewUnaryExpression(parsing::Token::Type op,
                                      Expression *expr) {
    return new (region_) UnaryExpression(op, expr);
  }

  IdentifierExpression *NewIdentifierExpression(AstString *name) {
    return new (region_) IdentifierExpression(name);
  }

  ArrayType *NewArrayType(Expression *len, Expression *elem_type) {
    return new (region_) ArrayType(len, elem_type);
  }

  Field *NewField(const AstString *name, Expression *type) {
    return new (region_) Field(name, type);
  }

  FunctionType *NewFunctionType(util::RegionVector<Field *> &&params,
                                Expression *ret) {
    return new (region_) FunctionType(std::move(params), ret);
  }

  PointerType *NewPointerType(Expression *pointee_type) {
    return new (region_) PointerType(pointee_type);
  }

  StructType *NewStructType(util::RegionVector<Field *> &&fields) {
    return new (region_) StructType(std::move(fields));
  }

 private:
  util::Region &region_;
};

}  // namespace tpl::ast