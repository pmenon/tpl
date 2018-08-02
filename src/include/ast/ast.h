#pragma once

#include <stdint.h>

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
 *
 */
class BinaryOperation : public AstNode {
 public:
  BinaryOperation(AstNode *left, AstNode *right)
      : AstNode(Type::BinaryOperation) {}

  AstNode *left() { return left_; }
  AstNode *right() { return right_; }

 private:
  AstNode *left_;
  AstNode *right_;
};

/**
 *
 */
class UnaryOperation : public AstNode {};

}  // namespace tpl