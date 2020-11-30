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
                                              FunctionLiteralExpr *fun) {
    return new (region_) FunctionDeclaration(pos, name, fun);
  }

  StructDeclaration *NewStructDeclaration(const SourcePosition &pos, Identifier name,
                                          StructTypeRepr *type_repr) {
    return new (region_) StructDeclaration(pos, name, type_repr);
  }

  VariableDeclaration *NewVariableDeclaration(const SourcePosition &pos, Identifier name,
                                              Expr *type_repr, Expr *init) {
    return new (region_) VariableDeclaration(pos, name, type_repr, init);
  }

  BlockStatement *NewBlockStatement(const SourcePosition &start_pos, const SourcePosition &end_pos,
                                    util::RegionVector<Statement *> &&statements) {
    return new (region_) BlockStatement(start_pos, end_pos, std::move(statements));
  }

  DeclarationStatement *NewDeclStatement(Declaration *decl) {
    return new (region_) DeclarationStatement(decl);
  }

  AssignmentStatement *NewAssignmentStatement(const SourcePosition &pos, Expr *dest, Expr *src) {
    return new (region_) AssignmentStatement(pos, dest, src);
  }

  ExpressionStatement *NewExpressionStatement(Expr *expression) {
    return new (region_) ExpressionStatement(expression);
  }

  ForStatement *NewForStatement(const SourcePosition &pos, Statement *init, Expr *cond,
                                Statement *next, BlockStatement *body) {
    return new (region_) ForStatement(pos, init, cond, next, body);
  }

  ForInStatement *NewForInStatement(const SourcePosition &pos, Expr *target, Expr *iter,
                                    BlockStatement *body) {
    return new (region_) ForInStatement(pos, target, iter, body);
  }

  IfStatement *NewIfStatement(const SourcePosition &pos, Expr *cond, BlockStatement *then_stmt,
                              Statement *else_stmt) {
    return new (region_) IfStatement(pos, cond, then_stmt, else_stmt);
  }

  ReturnStatement *NewReturnStatement(const SourcePosition &pos, Expr *ret) {
    return new (region_) ReturnStatement(pos, ret);
  }

  BadExpr *NewBadExpr(const SourcePosition &pos) { return new (region_) BadExpr(pos); }

  BinaryOpExpr *NewBinaryOpExpr(const SourcePosition &pos, parsing::Token::Type op, Expr *left,
                                Expr *right) {
    return new (region_) BinaryOpExpr(pos, op, left, right);
  }

  ComparisonOpExpr *NewComparisonOpExpr(const SourcePosition &pos, parsing::Token::Type op,
                                        Expr *left, Expr *right) {
    return new (region_) ComparisonOpExpr(pos, op, left, right);
  }

  CallExpr *NewCallExpr(Expr *fun, util::RegionVector<Expr *> &&args) {
    return new (region_) CallExpr(fun, std::move(args));
  }

  CallExpr *NewBuiltinCallExpr(Expr *fun, util::RegionVector<Expr *> &&args) {
    return new (region_) CallExpr(fun, std::move(args), CallExpr::CallKind::Builtin);
  }

  LiteralExpr *NewNilLiteral(const SourcePosition &pos) { return new (region_) LiteralExpr(pos); }

  LiteralExpr *NewBoolLiteral(const SourcePosition &pos, bool val) {
    return new (region_) LiteralExpr(pos, val);
  }

  LiteralExpr *NewIntLiteral(const SourcePosition &pos, int64_t num) {
    return new (region_) LiteralExpr(pos, num);
  }

  LiteralExpr *NewFloatLiteral(const SourcePosition &pos, float num) {
    return new (region_) LiteralExpr(pos, num);
  }

  LiteralExpr *NewStringLiteral(const SourcePosition &pos, Identifier str) {
    return new (region_) LiteralExpr(pos, str);
  }

  FunctionLiteralExpr *NewFunctionLitExpr(FunctionTypeRepr *type_repr, BlockStatement *body) {
    return new (region_) FunctionLiteralExpr(type_repr, body);
  }

  UnaryOpExpr *NewUnaryOpExpr(const SourcePosition &pos, parsing::Token::Type op, Expr *expr) {
    return new (region_) UnaryOpExpr(pos, op, expr);
  }

  IdentifierExpr *NewIdentifierExpr(const SourcePosition &pos, Identifier name) {
    return new (region_) IdentifierExpr(pos, name);
  }

  ImplicitCastExpr *NewImplicitCastExpr(const SourcePosition &pos, CastKind cast_kind,
                                        Type *target_type, Expr *input) {
    return new (region_) ImplicitCastExpr(pos, cast_kind, target_type, input);
  }

  IndexExpr *NewIndexExpr(const SourcePosition &pos, Expr *object, Expr *index) {
    return new (region_) IndexExpr(pos, object, index);
  }

  MemberExpr *NewMemberExpr(const SourcePosition &pos, Expr *object, Expr *member) {
    return new (region_) MemberExpr(pos, object, member);
  }

  ArrayTypeRepr *NewArrayType(const SourcePosition &pos, Expr *len, Expr *elem_type) {
    return new (region_) ArrayTypeRepr(pos, len, elem_type);
  }

  FieldDeclaration *NewFieldDeclaration(const SourcePosition &pos, Identifier name,
                                        Expr *type_repr) {
    return new (region_) FieldDeclaration(pos, name, type_repr);
  }

  FunctionTypeRepr *NewFunctionType(const SourcePosition &pos,
                                    util::RegionVector<FieldDeclaration *> &&params, Expr *ret) {
    return new (region_) FunctionTypeRepr(pos, std::move(params), ret);
  }

  PointerTypeRepr *NewPointerType(const SourcePosition &pos, Expr *base) {
    return new (region_) PointerTypeRepr(pos, base);
  }

  StructTypeRepr *NewStructType(const SourcePosition &pos,
                                util::RegionVector<FieldDeclaration *> &&fields) {
    return new (region_) StructTypeRepr(pos, std::move(fields));
  }

  MapTypeRepr *NewMapType(const SourcePosition &pos, Expr *key_type, Expr *val_type) {
    return new (region_) MapTypeRepr(pos, key_type, val_type);
  }

 private:
  util::Region *region_;
};

}  // namespace tpl::ast
