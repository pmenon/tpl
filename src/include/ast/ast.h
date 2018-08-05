#pragma once

#include <cstdint>

#include "parsing/token.h"
#include "util/region.h"

namespace tpl {

/*
 * Below are all the statement nodes in the AST hierarchy
 */
#define STATEMENT_NODES(T)

/*
 * Below are all the expression nodes in the AST hierarchy
 */
#define EXPRESSION_NODES(T) \
  T(BinaryOperation)        \
  T(Literal)                \
  T(UnaryOperation)

/*
 * All possible AST nodes
 */
#define AST_NODES(T) \
  STATEMENT_NODES(T) \
  EXPRESSION_NODES(T)

/**
 * The base class for all AST nodes
 */
class AstNode : public RegionObject {
 public:
#define T(type) type,
  enum class Type : uint8_t { AST_NODES(T) };
#undef T

  Type node_type() const { return type_; }

  void *operator new(std::size_t size, Region &region) {
    return region.Allocate(size);
  }

  // Use the region-based allocation
  void *operator new(std::size_t size) = delete;

 protected:
  explicit AstNode(Type type) : type_(type) {}

 private:
  Type type_;
};

/**
 * Base class for all expression nodes
 */
class Expression : public AstNode {
 public:
  Expression(AstNode::Type type) : AstNode(type) {}
};

/**
 * A binary expression with non-null left and right children and an operator
 */
class BinaryOperation : public Expression {
 public:
  BinaryOperation(Token::Type op, AstNode *left, AstNode *right)
      : Expression(AstNode::Type::BinaryOperation),
        op_(op),
        left_(left),
        right_(right) {}

  Token::Type op() { return op_; }
  AstNode *left() { return left_; }
  AstNode *right() { return right_; }

 private:
  Token::Type op_;
  AstNode *left_;
  AstNode *right_;
};

/**
 * A literal in the original source code
 */
class Literal : public Expression {
 public:
  enum class Type : uint8_t { Nil, True, False, Identifier, Number, String };

  Literal(Literal::Type lit_type)
      : Expression(AstNode::Type::Literal), lit_type_(lit_type) {}

  Literal::Type type() const { return lit_type_; }

 private:
  Type lit_type_;
};

/**
 * A unary expression with a non-null inner expression and an operator
 */
class UnaryOperation : public Expression {
 public:
  UnaryOperation(Token::Type op, AstNode *expr)
      : Expression(AstNode::Type::UnaryOperation), op_(op), expr_(expr) {}

  Token::Type op() { return op_; }
  AstNode *expr() { return expr_; }

 private:
  Token::Type op_;
  AstNode *expr_;
};

}  // namespace tpl