#pragma once

#include <cstdint>

#include "ast/ast_value.h"
#include "parsing/token.h"
#include "util/region.h"

namespace tpl {

/*
 * Below are all the statement nodes in the AST hierarchy
 */
#define STATEMENT_NODES(T) \
  T(Block)                 \
  T(ExpressionStatement)   \
  T(IfStatement)

/*
 * Below are all the expression nodes in the AST hierarchy
 */
#define EXPRESSION_NODES(T) \
  T(BinaryOperation)        \
  T(Literal)                \
  T(UnaryOperation)         \
  T(Variable)

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

 protected:
  explicit AstNode(Type type) : type_(type) {}

 private:
  Type type_;
};

class Expression;
class Statement;

////////////////////////////////////////////////////////////////////////////////
///
/// Statement nodes below
///
////////////////////////////////////////////////////////////////////////////////

/**
 * Base class for all statement nodes
 */
class Statement : public AstNode {
 public:
  Statement(AstNode::Type type) : AstNode(type) {}
};

/*
 * A block of statements
 */
class Block : public Statement {
 public:
  Block(RegionVector<Statement *> &&statements)
      : Statement(AstNode::Type::Block), statements_(std::move(statements)) {}

  RegionVector<Statement *> statements() { return statements_; }

 private:
  RegionVector<Statement *> statements_;
};

/**
 * The bridge between statements and expressions
 */
class ExpressionStatement : public Statement {
 public:
  ExpressionStatement(Expression *expression)
      : Statement(AstNode::Type::ExpressionStatement),
        expression_(expression) {}

  Expression *expr() { return expression_; }

 private:
  Expression *expression_;
};

/**
 * An if-then-else statement
 */
class IfStatement : public Statement {
 public:
  IfStatement(Expression *cond, Statement *then_stmt, Statement *else_stmt)
      : Statement(AstNode::Type::IfStatement),
        cond_(cond),
        then_stmt_(then_stmt),
        else_stmt_(else_stmt) {}

  Expression *cond() { return cond_; }
  Statement *then_stmt() { return then_stmt_; }
  Statement *else_stmt() { return else_stmt_; }

 private:
  Expression *cond_;
  Statement *then_stmt_;
  Statement *else_stmt_;
};

////////////////////////////////////////////////////////////////////////////////
///
/// Expression nodes below
///
////////////////////////////////////////////////////////////////////////////////

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
  enum class Type : uint8_t { Nil, Boolean, Identifier, Number, String };

  Literal(Literal::Type lit_type)
      : Expression(AstNode::Type::Literal), lit_type_(lit_type) {}

  Literal(bool val)
      : Expression(AstNode::Type::Literal),
        lit_type_(Literal::Type::Boolean),
        boolean_(val) {}

  Literal(int64_t val)
      : Expression(AstNode::Type::Literal),
        lit_type_(Literal::Type::Number),
        int_(val) {}

  Literal(double val)
      : Expression(AstNode::Type::Literal),
        lit_type_(Literal::Type::Number),
        double_(val) {}
  Literal(AstString *str)
      : Expression(AstNode::Type::Literal),
        lit_type_(Literal::Type::String),
        str_(str) {}

  Literal::Type type() const { return lit_type_; }

  bool bool_val() const {
    TPL_ASSERT(type() == Type::Boolean);
    return boolean_;
  }

 private:
  Type lit_type_;

  union {
    bool boolean_;
    int64_t int_;
    double double_;
    AstString *str_;
  };
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

/**
 *
 */
class Variable : public Expression {
 public:
  Variable(AstString *name, Expression *init)
      : Expression(AstNode::Type::Variable), name_(name), init_(init) {}

  AstString *name() { return name_; }
  bool has_init() { return init_ != nullptr; }
  Expression *init() { return init_; }

 private:
  AstString *name_;
  Expression *init_;
};

}  // namespace tpl