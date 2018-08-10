#pragma once

#include <cstdint>

#include "ast/ast_value.h"
#include "parsing/token.h"
#include "util/region.h"
#include "util/region_containers.h"

namespace tpl {

#define TYPES(T)    \
  T(ArrayType)      \
  T(FunctionType)   \
  T(IdentifierType) \
  T(PointerType)    \
  T(StructType)

#define DECLARATION_NODES(T) \
  T(FunctionDeclaration)     \
  T(StructDeclaration)       \
  T(VariableDeclaration)

/*
 * Below are all the statement nodes in the AST hierarchy
 */
#define STATEMENT_NODES(T) \
  T(BlockStatement)        \
  T(DeclarationStatement)  \
  T(ExpressionStatement)   \
  T(IfStatement)           \
  T(ReturnStatement)

/*
 * Below are all the expression nodes in the AST hierarchy
 */
#define EXPRESSION_NODES(T)    \
  T(BinaryExpression)          \
  T(CallExpression)            \
  T(FunctionLiteralExpression) \
  T(LiteralExpression)         \
  T(UnaryExpression)           \
  T(VarExpression)

/*
 * All possible AST nodes
 */
#define AST_NODES(T)   \
  DECLARATION_NODES(T) \
  EXPRESSION_NODES(T)  \
  STATEMENT_NODES(T)

class Declaration;
class Expression;
class Statement;

// Forward declare all nodes
#define FORWARD_DECLARE(name) class name;
AST_NODES(FORWARD_DECLARE)
#undef FORWARD_DECLARE

////////////////////////////////////////////////////////////////////////////////
///
/// Types
///
////////////////////////////////////////////////////////////////////////////////

/**
 * Base class for all types
 */
class Type : public RegionObject {
 public:
#define T(type) type,
  enum Id : uint8_t { TYPES(T) };
#undef T

  explicit Type(Id type_id) : type_id_(type_id) {}

  Id type_id() const { return type_id_; }

 private:
  Id type_id_;
};

class Field : public RegionObject {
 public:
  Field(const AstString *name, Type *type) : name_(name), type_(type) {}

  const AstString *name() const { return name_; }
  Type *type() const { return type_; }

 private:
  const AstString *name_;
  Type *type_;
};

/**
 * Array type
 */
class ArrayType : public Type {
 public:
  ArrayType(Type *elem_type, Expression *len)
      : Type(Type::Id::ArrayType), elem_type_(elem_type), len_(len) {}

  Type *element_type() const { return elem_type_; }
  Expression *length() const { return len_; }

 private:
  Type *elem_type_;
  Expression *len_;
};

/**
 * Function type
 */
class FunctionType : public Type {
 public:
  FunctionType(util::RegionVector<Field *> &&param_types, Type *ret_type)
      : Type(Type::Id::FunctionType),
        param_types_(std::move(param_types)),
        ret_type_(ret_type) {}

  const util::RegionVector<Field *> parameter_types() const {
    return param_types_;
  }

  const Type *return_type() const { return ret_type_; }

 private:
  util::RegionVector<Field *> param_types_;
  Type *ret_type_;
};

/**
 * An identifier for a type e.g., i32, bool, or custom struct types
 */
class IdentifierType : public Type {
 public:
  /// Constructor when initializing an unbound identifier type
  explicit IdentifierType(const AstString *name)
      : Type(Type::Id::IdentifierType), name_(name), declaration_(nullptr) {}

  IdentifierType(const AstString *name, Declaration *declaration)
      : Type(Type::Id::IdentifierType),
        name_(name),
        declaration_(declaration) {}

  const AstString *name() const { return name_; }
  Declaration *declaration() const { return declaration_; }

  void BindTo(Declaration *declaration) { declaration_ = declaration; }

 private:
  const AstString *name_;
  Declaration *declaration_;
};

/**
 * Pointer type
 */
class PointerType : public Type {
 public:
  explicit PointerType(Type *pointee_type)
      : Type(Type::Id::PointerType), pointee_type_(pointee_type) {}

  Type *pointee_type() const { return pointee_type_; }

 private:
  Type *pointee_type_;
};

/**
 * Struct type
 */
class StructType : public Type {
 public:
  StructType(util::RegionVector<Field *> &&fields)
      : Type(Type::Id::StructType), fields_(std::move(fields)) {}

  const util::RegionVector<Field *> &fields() const { return fields_; }

 private:
  util::RegionVector<Field *> fields_;
};

////////////////////////////////////////////////////////////////////////////////
///
/// AST Nodes
///
////////////////////////////////////////////////////////////////////////////////

/**
 * The base class for all AST nodes
 */
class AstNode : public RegionObject {
 public:
#define T(type) type,
  enum class NodeType : uint8_t { AST_NODES(T) };
#undef T

  NodeType node_type() const { return type_; }

 protected:
  explicit AstNode(NodeType type) : type_(type) {}

 private:
  NodeType type_;
};

////////////////////////////////////////////////////////////////////////////////
///
/// Declaration nodes
///
////////////////////////////////////////////////////////////////////////////////

class Declaration : public AstNode {
 public:
  Declaration(AstNode::NodeType type, const AstString *name)
      : AstNode(type), name_(name) {}

  const AstString *name() const { return name_; }

 private:
  const AstString *name_;
};

/**
 * A function declaration
 */
class FunctionDeclaration : public Declaration {
 public:
  FunctionDeclaration(const AstString *name, FunctionLiteralExpression *fun)
      : Declaration(AstNode::NodeType::FunctionDeclaration, name), fun_(fun) {}

  FunctionLiteralExpression *function() const { return fun_; }
  const FunctionType *type() const;

 private:
  FunctionLiteralExpression *fun_;
};

/**
 * A variable declaration
 */
class VariableDeclaration : public Declaration {
 public:
  VariableDeclaration(const AstString *name, Type *type, Expression *init)
      : Declaration(AstNode::NodeType::VariableDeclaration, name),
        type_(type),
        init_(init) {}

  Type *type() const { return type_; }
  Expression *initial() const { return init_; }

 private:
  Type *type_;
  Expression *init_;
};

/**
 *
 */
class StructDeclaration : public Declaration {
 public:
  StructDeclaration(const AstString *name, StructType *type)
      : Declaration(AstNode::NodeType::StructDeclaration, name), type_(type) {}

  const StructType *type() const { return type_; }

 private:
  StructType *type_;
};

////////////////////////////////////////////////////////////////////////////////
///
/// Statement nodes
///
////////////////////////////////////////////////////////////////////////////////

/**
 * Base class for all statement nodes
 */
class Statement : public AstNode {
 public:
  explicit Statement(AstNode::NodeType type) : AstNode(type) {}
};

/**
 * A block of statements
 */
class BlockStatement : public Statement {
 public:
  explicit BlockStatement(util::RegionVector<Statement *> &&statements)
      : Statement(AstNode::NodeType::BlockStatement),
        statements_(std::move(statements)) {}

  util::RegionVector<Statement *> statements() { return statements_; }

 private:
  util::RegionVector<Statement *> statements_;
};

/**
 * A declaration
 */
class DeclarationStatement : public Statement {
 public:
  explicit DeclarationStatement(Declaration *decl)
      : Statement(AstNode::NodeType::DeclarationStatement), decl_(decl) {}

  const Declaration *declaration() const { return decl_; }

 private:
  Declaration *decl_;
};

/**
 * The bridge between statements and expressions
 */
class ExpressionStatement : public Statement {
 public:
  explicit ExpressionStatement(Expression *expression)
      : Statement(AstNode::NodeType::ExpressionStatement),
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
      : Statement(AstNode::NodeType::IfStatement),
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

/**
 * A return statement
 */
class ReturnStatement : public Statement {
 public:
  explicit ReturnStatement(Expression *ret)
      : Statement(AstNode::NodeType::ReturnStatement), ret_(ret) {}

  Expression *ret() { return ret_; }

 private:
  Expression *ret_;
};

////////////////////////////////////////////////////////////////////////////////
///
/// Expression nodes
///
////////////////////////////////////////////////////////////////////////////////

/**
 * Base class for all expression nodes
 */
class Expression : public AstNode {
 public:
  explicit Expression(AstNode::NodeType type) : AstNode(type) {}
};

/**
 * A function call expression
 */
class CallExpression : public Expression {
 public:
  CallExpression(Expression *fun, util::RegionVector<Expression *> &&args)
      : Expression(AstNode::NodeType::CallExpression),
        fun_(fun),
        args_(std::move(args)) {}

  Expression *function() { return fun_; }
  util::RegionVector<Expression *> &arguments() { return args_; }

 private:
  Expression *fun_;
  util::RegionVector<Expression *> args_;
};

/**
 * A binary expression with non-null left and right children and an operator
 */
class BinaryExpression : public Expression {
 public:
  BinaryExpression(Token::Type op, AstNode *left, AstNode *right)
      : Expression(AstNode::NodeType::BinaryExpression),
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

class FunctionLiteralExpression : public Expression {
 public:
  FunctionLiteralExpression(const AstString *name, FunctionType *type)
      : Expression(AstNode::NodeType::FunctionLiteralExpression),
        name_(name),
        type_(type) {}

  const AstString *name() const { return name_; }
  const FunctionType *type() const { return type_; }

 private:
  const AstString *name_;
  FunctionType *type_;
};

/**
 * A literal in the original source code
 */
class LiteralExpression : public Expression {
 public:
  enum class Type : uint8_t { Nil, Boolean, Number, String };

  explicit LiteralExpression()
      : Expression(AstNode::NodeType::LiteralExpression),
        lit_type_(LiteralExpression::Type::Nil) {}

  explicit LiteralExpression(bool val)
      : Expression(AstNode::NodeType::LiteralExpression),
        lit_type_(LiteralExpression::Type::Boolean),
        boolean_(val) {}

  explicit LiteralExpression(LiteralExpression::Type lit_type, AstString *str)
      : Expression(AstNode::NodeType::LiteralExpression),
        lit_type_(lit_type),
        str_(str) {}

  LiteralExpression::Type type() const { return lit_type_; }

  bool bool_val() const {
    TPL_ASSERT(type() == Type::Boolean);
    return boolean_;
  }

  const AstString *raw_string() const {
    TPL_ASSERT(type() == Type::String);
    return str_;
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
class UnaryExpression : public Expression {
 public:
  UnaryExpression(Token::Type op, AstNode *expr)
      : Expression(AstNode::NodeType::UnaryExpression), op_(op), expr_(expr) {}

  Token::Type op() { return op_; }
  AstNode *expr() { return expr_; }

 private:
  Token::Type op_;
  AstNode *expr_;
};

/**
 * A reference to a variable
 */
class VarExpression : public Expression {
 public:
  explicit VarExpression(const AstString *name)
      : Expression(AstNode::NodeType::VarExpression), name_(name) {}

  const AstString *name() { return name_; }

 private:
  const AstString *name_;
};

}  // namespace tpl