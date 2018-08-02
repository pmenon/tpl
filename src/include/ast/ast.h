#pragma once

#include <stdint.h>

namespace tpl {

/*
 * Below are all the statement nodes in the AST hierarchy
 */
#define STATEMENT_NODES(T)

/*
 * Below are all the expression nodes in the AST hierarchy
 */
#define EXPRESSION_NODES(T) T(BinaryExpression)

/*
 * All possible AST nodes
 */
#define AST_NODES(T) \
  STATEMENT_NODES(T) \
  EXPRESSION_NODES(T)

/**
 * The base class for all AST nodes
 */
class AstNode {
 public:
#define T(type) type,
  enum class Type : uint8_t { AST_NODES(T) };
#undef T

  Type node_type() const { return type_; }

 protected:
  explicit AstNode(Type type) : type_(type) {}

 private:
  Type type_;
};

/**
 *
 */
class BinaryExpression : public AstNode {};

/**
 *
 */
class AstNodeFactory {};

}  // namespace tpl