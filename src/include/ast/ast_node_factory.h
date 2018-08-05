#pragma once

#include "ast/ast.h"
#include "util/region.h"

namespace tpl {

/**
 *
 */
class AstNodeFactory {
 public:
  explicit AstNodeFactory(Region &region) : region_(region) {}

  BinaryOperation *NewBinaryOperation(Token::Type op, AstNode *left,
                                      AstNode *right) {
    return new (region_) BinaryOperation(op, left, right);
  }

  Literal *NewLiteral(Literal::Type lit_type) {
    return new (region_) Literal(lit_type);
  }

  UnaryOperation *NewUnaryOperation(Token::Type op, AstNode *expr) {
    return new (region_) UnaryOperation(op, expr);
  }

 private:
  Region &region_;
};

}  // namespace tpl