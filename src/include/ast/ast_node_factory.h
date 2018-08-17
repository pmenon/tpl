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

  File *NewFile(const SourcePosition &pos,
                util::RegionVector<Declaration *> &&declarations) {
    return new (region_) File(pos, std::move(declarations));
  }

  FunctionDeclaration *NewFunctionDeclaration(const SourcePosition &pos,
                                              const AstString *name,
                                              FunctionLiteralExpression *fun) {
    return new (region_) FunctionDeclaration(pos, name, fun);
  }

  StructDeclaration *NewStructDeclaration(const SourcePosition &pos,
                                          const AstString *name,
                                          StructTypeRepr *type) {
    return new (region_) StructDeclaration(pos, name, type);
  }

  VariableDeclaration *NewVariableDeclaration(const SourcePosition &pos,
                                              const AstString *name,
                                              Expression *type,
                                              Expression *init) {
    return new (region_) VariableDeclaration(pos, name, type, init);
  }

  BadStatement *NewBadStatement(const SourcePosition &pos) {
    return new (region_) BadStatement(pos);
  }

  BlockStatement *NewBlockStatement(
      const SourcePosition &start_pos, const SourcePosition &end_pos,
      util::RegionVector<Statement *> &&statements) {
    return new (region_)
        BlockStatement(start_pos, end_pos, std::move(statements));
  }

  DeclarationStatement *NewDeclarationStatement(Declaration *decl) {
    return new (region_) DeclarationStatement(decl);
  }

  ExpressionStatement *NewExpressionStatement(Expression *expression) {
    return new (region_) ExpressionStatement(expression);
  }

  ForStatement *NewForStatement(const SourcePosition &pos, Statement *init,
                                Expression *cond, Statement *next,
                                BlockStatement *body) {
    return new (region_) ForStatement(pos, init, cond, next, body);
  }

  IfStatement *NewIfStatement(const SourcePosition &pos, Expression *cond,
                              BlockStatement *then_stmt, Statement *else_stmt) {
    return new (region_) IfStatement(pos, cond, then_stmt, else_stmt);
  }

  ReturnStatement *NewReturnStatement(const SourcePosition &pos,
                                      Expression *ret) {
    return new (region_) ReturnStatement(pos, ret);
  }

  BadExpression *NewBadExpression(const SourcePosition &pos) {
    return new (region_) BadExpression(pos);
  }

  BinaryExpression *NewBinaryExpression(const SourcePosition &pos,
                                        parsing::Token::Type op,
                                        Expression *left, Expression *right) {
    return new (region_) BinaryExpression(pos, op, left, right);
  }

  CallExpression *NewCallExpression(Expression *fun,
                                    util::RegionVector<Expression *> &&args) {
    return new (region_) CallExpression(fun, std::move(args));
  }

  LiteralExpression *NewNilLiteral(const SourcePosition &pos) {
    return new (region_) LiteralExpression(pos);
  }

  LiteralExpression *NewBoolLiteral(const SourcePosition &pos, bool val) {
    return new (region_) LiteralExpression(pos, val);
  }

  LiteralExpression *NewIntLiteral(const SourcePosition &pos, AstString *num) {
    return new (region_)
        LiteralExpression(pos, LiteralExpression::Type::Int, num);
  }

  LiteralExpression *NewFloatLiteral(const SourcePosition &pos,
                                     AstString *num) {
    return new (region_)
        LiteralExpression(pos, LiteralExpression::Type::Float, num);
  }

  LiteralExpression *NewStringLiteral(const SourcePosition &pos,
                                      AstString *str) {
    return new (region_)
        LiteralExpression(pos, LiteralExpression::Type::String, str);
  }

  FunctionLiteralExpression *NewFunctionLiteral(FunctionTypeRepr *type,
                                                BlockStatement *body) {
    return new (region_) FunctionLiteralExpression(type, body);
  }

  UnaryExpression *NewUnaryExpression(const SourcePosition &pos,
                                      parsing::Token::Type op,
                                      Expression *expr) {
    return new (region_) UnaryExpression(pos, op, expr);
  }

  IdentifierExpression *NewIdentifierExpression(const SourcePosition &pos,
                                                AstString *name) {
    return new (region_) IdentifierExpression(pos, name);
  }

  ArrayTypeRepr *NewArrayType(const SourcePosition &pos, Expression *len,
                              Expression *elem_type) {
    return new (region_) ArrayTypeRepr(pos, len, elem_type);
  }

  Field *NewField(const SourcePosition &pos, const AstString *name,
                  Expression *type) {
    return new (region_) Field(pos, name, type);
  }

  FunctionTypeRepr *NewFunctionType(const SourcePosition &pos,
                                    util::RegionVector<Field *> &&params,
                                    Expression *ret) {
    return new (region_) FunctionTypeRepr(pos, std::move(params), ret);
  }

  PointerTypeRepr *NewPointerType(const SourcePosition &pos, Expression *base) {
    return new (region_) PointerTypeRepr(pos, base);
  }

  StructTypeRepr *NewStructType(const SourcePosition &pos,
                                util::RegionVector<Field *> &&fields) {
    return new (region_) StructTypeRepr(pos, std::move(fields));
  }

 private:
  util::Region &region_;
};

}  // namespace tpl::ast