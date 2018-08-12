#pragma once

#include <cstdint>

#include "ast/ast_value.h"
#include "parsing/token.h"
#include "util/casting.h"
#include "util/region.h"
#include "util/region_containers.h"

namespace tpl::ast {

/*
 *
 */
#define FILE_NODE(T) T(File)

/*
 * All possible declarations
 *
 * If you add a new declaration node to either the beginning or end of the list,
 * remember to modify Declaration::classof() to update the bounds check.
 */
#define DECLARATION_NODES(T) \
  T(FunctionDeclaration)     \
  T(StructDeclaration)       \
  T(VariableDeclaration)

/*
 * All possible statements
 *
 * If you add a new statement node to either the beginning or end of the list,
 * remember to modify Statement::classof() to update the bounds check.
 */
#define STATEMENT_NODES(T) \
  T(BlockStatement)        \
  T(DeclarationStatement)  \
  T(ExpressionStatement)   \
  T(IfStatement)           \
  T(ReturnStatement)

/*
 * All possible expressions
 *
 * If you add a new expression node to either the beginning or end of the list,
 * remember to modify Expression::classof() to update the bounds check.
 */
#define EXPRESSION_NODES(T)    \
  T(BinaryExpression)          \
  T(CallExpression)            \
  T(FunctionLiteralExpression) \
  T(LiteralExpression)         \
  T(UnaryExpression)           \
  T(VarExpression)

/*
 * All possible types.
 *
 * If you add a new type to either the beginning or end of the list, remember to
 * modify Type::classof() to update the bounds check.
 */
#define TYPE_NODES(T) \
  T(ArrayType)        \
  T(FunctionType)     \
  T(IdentifierType)   \
  T(PointerType)      \
  T(StructType)

/*
 * All possible AST nodes
 */
#define AST_NODES(T)   \
  DECLARATION_NODES(T) \
  EXPRESSION_NODES(T)  \
  FILE_NODE(T)         \
  STATEMENT_NODES(T)   \
  TYPE_NODES(T)

class Declaration;
class Expression;
class Statement;
class Type;

// Forward declare all nodes
#define FORWARD_DECLARE(name) class name;
AST_NODES(FORWARD_DECLARE)
#undef FORWARD_DECLARE

////////////////////////////////////////////////////////////////////////////////
///
/// AST Nodes
///
////////////////////////////////////////////////////////////////////////////////

/**
 * The base class for all AST nodes
 */
class AstNode : public util::RegionObject {
 public:
  // The kind enumeration listing all possible node kinds
#define T(kind) kind,
  enum class Kind : uint8_t { AST_NODES(T) };
#undef T

  // The kind of this node
  Kind kind() const { return kind_; }

  // This is mainly used in tests!
  const char *kind_name() const {
#define KIND_CASE(kind) \
  case Kind::kind:      \
    return #kind;

    // Main type switch
    // clang-format off
    switch (kind()) {
      default: { UNREACHABLE(); }
      AST_NODES(KIND_CASE)
    }
      // clang-format on
#undef KIND_CASE
  }

 public:
  // Checks if this node is an instance of the specified class
  template <typename T>
  bool Is() const {
    return util::Is<T>(this);
  }

  // Casts this node to an instance of the specified class, asserting if the
  // conversion is invalid. This is probably most similar to std::static_cast<>
  // or std::reinterpret_cast<>
  template <typename T>
  T *As() {
    TPL_ASSERT(Is<T>());
    return reinterpret_cast<T *>(this);
  }

  template <typename T>
  const T *As() const {
    TPL_ASSERT(Is<T>());
    return reinterpret_cast<const T *>(this);
  }

  // Casts this node to an instance of the provided class if valid. If the
  // conversion is invalid, this returns a NULL pointer. This is most similar to
  // std::dynamic_cast<T>, i.e., it's a checked cast.
  template <typename T>
  T *SafeAs() {
    return (Is<T>() ? As<T>() : nullptr);
  }

  template <typename T>
  const T *SafeAs() const {
    return (Is<T>() ? As<T>() : nullptr);
  }

 protected:
  explicit AstNode(Kind kind) : kind_(kind) {}

 private:
  Kind kind_;
};

/**
 *
 */
class File : public AstNode {
 public:
  explicit File(util::RegionVector<Declaration *> &&decls)
      : AstNode(Kind::File), decls_(std::move(decls)) {}

  util::RegionVector<Declaration *> &declarations() { return decls_; }

  static bool classof(const AstNode *node) {
    return node->kind() >= Kind::File;
  }

 private:
  util::RegionVector<Declaration *> decls_;
};

////////////////////////////////////////////////////////////////////////////////
///
/// Declaration nodes
///
////////////////////////////////////////////////////////////////////////////////

class Declaration : public AstNode {
 public:
  Declaration(Kind kind, const AstString *name) : AstNode(kind), name_(name) {}

  const AstString *name() const { return name_; }

  static bool classof(const AstNode *node) {
    return node->kind() >= Kind::FunctionDeclaration &&
           node->kind() <= Kind::VariableDeclaration;
  }

 private:
  const AstString *name_;
};

/**
 * A function declaration
 */
class FunctionDeclaration : public Declaration {
 public:
  FunctionDeclaration(const AstString *name, FunctionLiteralExpression *fun)
      : Declaration(Kind::FunctionDeclaration, name), fun_(fun) {}

  FunctionLiteralExpression *function() const { return fun_; }

  FunctionType *type();

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::FunctionDeclaration;
  }

 private:
  FunctionLiteralExpression *fun_;
};

/**
 *
 */
class StructDeclaration : public Declaration {
 public:
  StructDeclaration(const AstString *name, StructType *type)
      : Declaration(Kind::StructDeclaration, name), type_(type) {}

  const StructType *type() const { return type_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::StructDeclaration;
  }

 private:
  StructType *type_;
};

/**
 * A variable declaration
 */
class VariableDeclaration : public Declaration {
 public:
  VariableDeclaration(const AstString *name, Type *type, Expression *init)
      : Declaration(Kind::VariableDeclaration, name),
        type_(type),
        init_(init) {}

  Type *type() const { return type_; }

  Expression *initial() const { return init_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::VariableDeclaration;
  }

 private:
  Type *type_;
  Expression *init_;
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
  explicit Statement(Kind kind) : AstNode(kind) {}

  static bool classof(const AstNode *node) {
    return node->kind() >= Kind::BlockStatement &&
           node->kind() <= Kind::ReturnStatement;
  }
};

/**
 * A block of statements
 */
class BlockStatement : public Statement {
 public:
  explicit BlockStatement(util::RegionVector<Statement *> &&statements)
      : Statement(Kind::BlockStatement), statements_(std::move(statements)) {}

  util::RegionVector<Statement *> statements() { return statements_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::BlockStatement;
  }

 private:
  util::RegionVector<Statement *> statements_;
};

/**
 * A declaration
 */
class DeclarationStatement : public Statement {
 public:
  explicit DeclarationStatement(Declaration *decl)
      : Statement(Kind::DeclarationStatement), decl_(decl) {}

  Declaration *declaration() const { return decl_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::DeclarationStatement;
  }

 private:
  Declaration *decl_;
};

/**
 * The bridge between statements and expressions
 */
class ExpressionStatement : public Statement {
 public:
  explicit ExpressionStatement(Expression *expression)
      : Statement(Kind::ExpressionStatement), expression_(expression) {}

  Expression *expr() { return expression_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::ExpressionStatement;
  }

 private:
  Expression *expression_;
};

/**
 * An if-then-else statement
 */
class IfStatement : public Statement {
 public:
  IfStatement(Expression *cond, BlockStatement *then_stmt, Statement *else_stmt)
      : Statement(Kind::IfStatement),
        cond_(cond),
        then_stmt_(then_stmt),
        else_stmt_(else_stmt) {}

  Expression *cond() { return cond_; }

  BlockStatement *then_stmt() { return then_stmt_; }

  Statement *else_stmt() { return else_stmt_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::IfStatement;
  }

 private:
  Expression *cond_;
  BlockStatement *then_stmt_;
  Statement *else_stmt_;
};

/**
 * A return statement
 */
class ReturnStatement : public Statement {
 public:
  explicit ReturnStatement(Expression *ret)
      : Statement(Kind::ReturnStatement), ret_(ret) {}

  Expression *ret() { return ret_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::ReturnStatement;
  }

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
  explicit Expression(Kind kind) : AstNode(kind) {}

  static bool classof(const AstNode *node) {
    return node->kind() >= Kind::BinaryExpression &&
           node->kind() <= Kind::VarExpression;
  }
};

/**
 * A binary expression with non-null left and right children and an operator
 */
class BinaryExpression : public Expression {
 public:
  BinaryExpression(parsing::Token::Type op, AstNode *left, AstNode *right)
      : Expression(Kind::BinaryExpression),
        op_(op),
        left_(left),
        right_(right) {}

  parsing::Token::Type op() { return op_; }

  AstNode *left() { return left_; }

  AstNode *right() { return right_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::BinaryExpression;
  }

 private:
  parsing::Token::Type op_;
  AstNode *left_;
  AstNode *right_;
};

/**
 * A function call expression
 */
class CallExpression : public Expression {
 public:
  CallExpression(Expression *fun, util::RegionVector<Expression *> &&args)
      : Expression(Kind::CallExpression), fun_(fun), args_(std::move(args)) {}

  Expression *function() { return fun_; }

  util::RegionVector<Expression *> &arguments() { return args_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::CallExpression;
  }

 private:
  Expression *fun_;
  util::RegionVector<Expression *> args_;
};

class FunctionLiteralExpression : public Expression {
 public:
  FunctionLiteralExpression(FunctionType *type, BlockStatement *body)
      : Expression(Kind::FunctionLiteralExpression), type_(type), body_(body) {}

  FunctionType *type() const { return type_; }

  BlockStatement *body() const { return body_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::FunctionLiteralExpression;
  }

 private:
  FunctionType *type_;
  BlockStatement *body_;
};

/**
 * A literal in the original source code
 */
class LiteralExpression : public Expression {
 public:
  enum class Type : uint8_t { Nil, Boolean, Number, String };

  explicit LiteralExpression()
      : Expression(Kind::LiteralExpression),
        lit_type_(LiteralExpression::Type::Nil) {}

  explicit LiteralExpression(bool val)
      : Expression(Kind::LiteralExpression),
        lit_type_(LiteralExpression::Type::Boolean),
        boolean_(val) {}

  explicit LiteralExpression(LiteralExpression::Type lit_type, AstString *str)
      : Expression(Kind::LiteralExpression), lit_type_(lit_type), str_(str) {}

  LiteralExpression::Type type() const { return lit_type_; }

  bool bool_val() const {
    TPL_ASSERT(type() == Type::Boolean);
    return boolean_;
  }

  const AstString *raw_string() const {
    // TODO(pmenon): Fix me to use actual AstNumbers for numbers?
    TPL_ASSERT(type() == Type::String || type() == Type::Number);
    return str_;
  }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::LiteralExpression;
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
  UnaryExpression(parsing::Token::Type op, AstNode *expr)
      : Expression(Kind::UnaryExpression), op_(op), expr_(expr) {}

  parsing::Token::Type op() { return op_; }

  AstNode *expr() { return expr_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::UnaryExpression;
  }

 private:
  parsing::Token::Type op_;
  AstNode *expr_;
};

/**
 * A reference to a variable
 */
class VarExpression : public Expression {
 public:
  explicit VarExpression(const AstString *name)
      : Expression(Kind::VarExpression), name_(name), decl_(nullptr) {}

  const AstString *name() { return name_; }

  void BindTo(Declaration *decl) { decl_ = decl; }

  bool is_bound() const { return decl_ != nullptr; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::VarExpression;
  }

 private:
  const AstString *name_;
  Declaration *decl_;
};

////////////////////////////////////////////////////////////////////////////////
///
/// Types
///
////////////////////////////////////////////////////////////////////////////////

/**
 * Base class for all types
 */
class Type : public AstNode {
 public:
  explicit Type(Kind kind) : AstNode(kind) {}

  static bool classof(const AstNode *node) {
    return node->kind() >= Kind::ArrayType && node->kind() <= Kind::StructType;
  }
};

class Field : public util::RegionObject {
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
  ArrayType(Expression *len, Type *elem_type)
      : Type(Kind::ArrayType), len_(len), elem_type_(elem_type) {}

  Expression *length() const { return len_; }

  Type *element_type() const { return elem_type_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::ArrayType;
  }

 private:
  Expression *len_;
  Type *elem_type_;
};

/**
 * Function type
 */
class FunctionType : public Type {
 public:
  FunctionType(util::RegionVector<Field *> &&param_types, Type *ret_type)
      : Type(Kind::FunctionType),
        param_types_(std::move(param_types)),
        ret_type_(ret_type) {}

  const util::RegionVector<Field *> parameters() const { return param_types_; }

  Type *return_type() const { return ret_type_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::FunctionType;
  }

 private:
  util::RegionVector<Field *> param_types_;
  Type *ret_type_;
};

/**
 * An identifier for a type e.g., i32, bool, or custom struct type
 */
class IdentifierType : public Type {
 public:
  /// Constructor when initializing an unbound identifier type
  explicit IdentifierType(const AstString *name)
      : Type(Kind::IdentifierType), name_(name), declaration_(nullptr) {}

  IdentifierType(const AstString *name, Declaration *declaration)
      : Type(Kind::IdentifierType), name_(name), declaration_(declaration) {}

  const AstString *name() const { return name_; }

  Declaration *declaration() const { return declaration_; }

  bool is_bound() const { return declaration_ != nullptr; }

  void BindTo(Declaration *declaration) { declaration_ = declaration; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::IdentifierType;
  }

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
      : Type(Kind::PointerType), pointee_type_(pointee_type) {}

  Type *pointee_type() const { return pointee_type_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::PointerType;
  }

 private:
  Type *pointee_type_;
};

/**
 * Struct type
 */
class StructType : public Type {
 public:
  explicit StructType(util::RegionVector<Field *> &&fields)
      : Type(Kind::StructType), fields_(std::move(fields)) {}

  const util::RegionVector<Field *> &fields() const { return fields_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::StructType;
  }

 private:
  util::RegionVector<Field *> fields_;
};

}  // namespace tpl::ast