#pragma once

#include <utility>

#include "ast/ast.h"
#include "util/region.h"

namespace tpl::ast {

/**
 * A factory for AST nodes.
 */
class AstNodeFactory {
 public:
  explicit AstNodeFactory(util::Region *region) : region_(region) {}

  DISALLOW_COPY_AND_MOVE(AstNodeFactory);

  File *NewFile(const SourcePosition &pos, util::RegionVector<Declaration *> &&declarations) {
    return new (region_) File(pos, std::move(declarations));
  }

  FunctionDeclaration *NewFunctionDeclaration(const SourcePosition &pos, Identifier name,
                                              FunctionLiteralExpression *fun) {
    return new (region_) FunctionDeclaration(pos, name, fun);
  }

  StructDeclaration *NewStructDeclaration(const SourcePosition &pos, Identifier name,
                                          StructTypeRepr *type_repr) {
    return new (region_) StructDeclaration(pos, name, type_repr);
  }

  VariableDeclaration *NewVariableDeclaration(const SourcePosition &pos, Identifier name,
                                              Expression *type_repr, Expression *init) {
    return new (region_) VariableDeclaration(pos, name, type_repr, init);
  }

  BlockStatement *NewBlockStatement(const SourcePosition &start_pos, const SourcePosition &end_pos,
                                    util::RegionVector<Statement *> &&statements) {
    return new (region_) BlockStatement(start_pos, end_pos, std::move(statements));
  }

  DeclarationStatement *NewDeclStatement(Declaration *decl) {
    return new (region_) DeclarationStatement(decl);
  }

  AssignmentStatement *NewAssignmentStatement(const SourcePosition &pos, Expression *dest,
                                              Expression *src) {
    return new (region_) AssignmentStatement(pos, dest, src);
  }

  ExpressionStatement *NewExpressionStatement(Expression *expression) {
    return new (region_) ExpressionStatement(expression);
  }

  ForStatement *NewForStatement(const SourcePosition &pos, Statement *init, Expression *cond,
                                Statement *next, BlockStatement *body) {
    return new (region_) ForStatement(pos, init, cond, next, body);
  }

  ForInStatement *NewForInStatement(const SourcePosition &pos, Expression *target, Expression *iter,
                                    BlockStatement *body) {
    return new (region_) ForInStatement(pos, target, iter, body);
  }

  IfStatement *NewIfStatement(const SourcePosition &pos, Expression *cond,
                              BlockStatement *then_stmt, Statement *else_stmt) {
    return new (region_) IfStatement(pos, cond, then_stmt, else_stmt);
  }

  ReturnStatement *NewReturnStatement(const SourcePosition &pos, Expression *ret) {
    return new (region_) ReturnStatement(pos, ret);
  }

  BadExpression *NewBadExpr(const SourcePosition &pos) { return new (region_) BadExpression(pos); }

  BinaryOpExpression *NewBinaryOpExpr(const SourcePosition &pos, parsing::Token::Type op,
                                      Expression *left, Expression *right) {
    return new (region_) BinaryOpExpression(pos, op, left, right);
  }

  ComparisonOpExpression *NewComparisonOpExpr(const SourcePosition &pos, parsing::Token::Type op,
                                              Expression *left, Expression *right) {
    return new (region_) ComparisonOpExpression(pos, op, left, right);
  }

  CallExpression *NewCallExpr(Expression *fun, util::RegionVector<Expression *> &&args) {
    return new (region_) CallExpression(fun, std::move(args));
  }

  CallExpression *NewBuiltinCallExpr(Expression *fun, util::RegionVector<Expression *> &&args) {
    return new (region_) CallExpression(fun, std::move(args), CallExpression::CallKind::Builtin);
  }

  LiteralExpression *NewNilLiteral(const SourcePosition &pos) {
    return new (region_) LiteralExpression(pos);
  }

  LiteralExpression *NewBoolLiteral(const SourcePosition &pos, bool val) {
    return new (region_) LiteralExpression(pos, val);
  }

  LiteralExpression *NewIntLiteral(const SourcePosition &pos, int64_t num) {
    return new (region_) LiteralExpression(pos, num);
  }

  LiteralExpression *NewFloatLiteral(const SourcePosition &pos, float num) {
    return new (region_) LiteralExpression(pos, num);
  }

  LiteralExpression *NewStringLiteral(const SourcePosition &pos, Identifier str) {
    return new (region_) LiteralExpression(pos, str);
  }

  FunctionLiteralExpression *NewFunctionLitExpr(FunctionTypeRepr *type_repr, BlockStatement *body) {
    return new (region_) FunctionLiteralExpression(type_repr, body);
  }

  UnaryOpExpression *NewUnaryOpExpr(const SourcePosition &pos, parsing::Token::Type op,
                                    Expression *expr) {
    return new (region_) UnaryOpExpression(pos, op, expr);
  }

  IdentifierExpression *NewIdentifierExpr(const SourcePosition &pos, Identifier name) {
    return new (region_) IdentifierExpression(pos, name);
  }

  ImplicitCastExpression *NewImplicitCastExpr(const SourcePosition &pos, CastKind cast_kind,
                                              Type *target_type, Expression *input) {
    return new (region_) ImplicitCastExpression(pos, cast_kind, target_type, input);
  }

  IndexExpression *NewIndexExpr(const SourcePosition &pos, Expression *object, Expression *index) {
    return new (region_) IndexExpression(pos, object, index);
  }

  MemberExpression *NewMemberExpr(const SourcePosition &pos, Expression *object,
                                  Expression *member) {
    return new (region_) MemberExpression(pos, object, member);
  }

  ArrayTypeRepr *NewArrayType(const SourcePosition &pos, Expression *len, Expression *elem_type) {
    return new (region_) ArrayTypeRepr(pos, len, elem_type);
  }

  FieldDeclaration *NewFieldDeclaration(const SourcePosition &pos, Identifier name,
                                        Expression *type_repr) {
    return new (region_) FieldDeclaration(pos, name, type_repr);
  }

  FunctionTypeRepr *NewFunctionType(const SourcePosition &pos,
                                    util::RegionVector<FieldDeclaration *> &&params,
                                    Expression *ret) {
    return new (region_) FunctionTypeRepr(pos, std::move(params), ret);
  }

  PointerTypeRepr *NewPointerType(const SourcePosition &pos, Expression *base) {
    return new (region_) PointerTypeRepr(pos, base);
  }

  StructTypeRepr *NewStructType(const SourcePosition &pos,
                                util::RegionVector<FieldDeclaration *> &&fields) {
    return new (region_) StructTypeRepr(pos, std::move(fields));
  }

  MapTypeRepr *NewMapType(const SourcePosition &pos, Expression *key_type, Expression *val_type) {
    return new (region_) MapTypeRepr(pos, key_type, val_type);
  }

 private:
  util::Region *region_;
};

}  // namespace tpl::ast
