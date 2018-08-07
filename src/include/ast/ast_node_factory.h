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

  ExpressionStatement *NewExpressionStatement(Expression *expression) {
    return new (region_) ExpressionStatement(expression);
  }

  Block *NewBlock(RegionVector<Statement *> &&statements) {
    return new (region_) Block(std::move(statements));
  }

  BinaryOperation *NewBinaryOperation(Token::Type op, AstNode *left,
                                      AstNode *right) {
    return new (region_) BinaryOperation(op, left, right);
  }

  Literal *NewNilLiteral() { return new (region_) Literal(Literal::Type::Nil); }

  Literal *NewBoolLiteral(bool val) { return new (region_) Literal(val); }

#if 0
  Literal *NewNumLiteral(AstString *str) { return (region_) Literal()}
#endif

  Literal *NewStringLiteral(AstString *str) {
    return new (region_) Literal(str);
  }

  UnaryOperation *NewUnaryOperation(Token::Type op, AstNode *expr) {
    return new (region_) UnaryOperation(op, expr);
  }

  Variable *NewVariable(AstString *name, Expression *init) {
    return new (region_) Variable(name, init);
  }

 private:
  Region &region_;
};

}  // namespace tpl