#pragma once

#include <cstdint>

#include "llvm/Support/Casting.h"

#include "ast/identifier.h"
#include "common.h"
#include "parsing/token.h"
#include "util/region.h"
#include "util/region_containers.h"

namespace tpl::ast {

class Type;

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
  T(FieldDeclaration)        \
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
  T(AssignmentStatement)   \
  T(BadStatement)          \
  T(BlockStatement)        \
  T(DeclarationStatement)  \
  T(ExpressionStatement)   \
  T(ForStatement)          \
  T(IfStatement)           \
  T(ReturnStatement)

/*
 * All possible expressions
 *
 * If you add a new expression node to either the beginning or end of the list,
 * remember to modify Expression::classof() to update the bounds check.
 */
#define EXPRESSION_NODES(T)    \
  T(BadExpression)             \
  T(BinaryExpression)          \
  T(CallExpression)            \
  T(FunctionLiteralExpression) \
  T(IdentifierExpression)      \
  T(LiteralExpression)         \
  T(UnaryExpression)           \
  /* Types */                  \
  T(ArrayTypeRepr)             \
  T(FunctionTypeRepr)          \
  T(PointerTypeRepr)           \
  T(StructTypeRepr)

/*
 * All possible AST nodes
 */
#define AST_NODES(T)   \
  DECLARATION_NODES(T) \
  EXPRESSION_NODES(T)  \
  FILE_NODE(T)         \
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

  // The position in the source where this element was found
  const SourcePosition &position() const { return pos_; }

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
    return llvm::isa<T>(this);
  }

  // Casts this node to an instance of the specified class, asserting if the
  // conversion is invalid. This is probably most similar to std::static_cast<>
  // or std::reinterpret_cast<>
  template <typename T>
  T *As() {
    TPL_ASSERT(Is<T>(), "Using unsafe cast on mismatched node types");
    return reinterpret_cast<T *>(this);
  }

  template <typename T>
  const T *As() const {
    TPL_ASSERT(Is<T>(), "Using unsafe cast on mismatched node types");
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

#define F(kind) \
  bool Is##kind() const { return Is<kind>(); }
  AST_NODES(F)
#undef F

 protected:
  AstNode(Kind kind, const SourcePosition &pos) : kind_(kind), pos_(pos) {}

 private:
  // The kind of AST node
  Kind kind_;

  // The position in the original source where this node's underlying
  // information was found
  const SourcePosition pos_;
};

/**
 * Represents a file
 */
class File : public AstNode {
 public:
  File(const SourcePosition &pos, util::RegionVector<Declaration *> &&decls)
      : AstNode(Kind::File, pos), decls_(std::move(decls)) {}

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
  Declaration(Kind kind, const SourcePosition &pos, Identifier name)
      : AstNode(kind, pos), name_(name) {}

  Identifier name() const { return name_; }

  static bool classof(const AstNode *node) {
    return node->kind() >= Kind::FieldDeclaration &&
           node->kind() <= Kind::VariableDeclaration;
  }

 private:
  Identifier name_;
};

/**
 * A generic declaration of a function argument or a field in a struct
 */
class FieldDeclaration : public Declaration {
 public:
  FieldDeclaration(const SourcePosition &pos, Identifier name,
                   Expression *type_repr)
      : Declaration(Kind::FieldDeclaration, pos, name), type_repr_(type_repr) {}

  Expression *type_repr() const { return type_repr_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::FieldDeclaration;
  }

 private:
  Expression *type_repr_;
};

/**
 * A function declaration
 */
class FunctionDeclaration : public Declaration {
 public:
  FunctionDeclaration(const SourcePosition &pos, Identifier name,
                      FunctionLiteralExpression *fun)
      : Declaration(Kind::FunctionDeclaration, pos, name), fun_(fun) {}

  FunctionLiteralExpression *function() const { return fun_; }

  FunctionTypeRepr *type_repr();

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
  StructDeclaration(const SourcePosition &pos, Identifier name,
                    StructTypeRepr *type_repr)
      : Declaration(Kind::StructDeclaration, pos, name),
        type_repr_(type_repr) {}

  StructTypeRepr *type_repr() const { return type_repr_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::StructDeclaration;
  }

 private:
  StructTypeRepr *type_repr_;
};

/**
 * A variable declaration
 */
class VariableDeclaration : public Declaration {
 public:
  VariableDeclaration(const SourcePosition &pos, Identifier name,
                      Expression *type_repr, Expression *init)
      : Declaration(Kind::VariableDeclaration, pos, name),
        type_repr_(type_repr),
        init_(init) {}

  Expression *type_repr() const { return type_repr_; }

  Expression *initial() const { return init_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::VariableDeclaration;
  }

 private:
  Expression *type_repr_;
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
  Statement(Kind kind, const SourcePosition &pos) : AstNode(kind, pos) {}

  static bool classof(const AstNode *node) {
    return node->kind() >= Kind::AssignmentStatement &&
           node->kind() <= Kind::ReturnStatement;
  }
};

/**
 * An assignment, dest = source
 */
class AssignmentStatement : public Statement {
 public:
  AssignmentStatement(const SourcePosition &pos, Expression *dest,
                      Expression *src)
      : Statement(AstNode::Kind::AssignmentStatement, pos),
        dest_(dest),
        src_(src) {}

  Expression *destination() const { return dest_; }

  Expression *source() const { return src_; };

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::AssignmentStatement;
  }

 private:
  Expression *dest_;
  Expression *src_;
};

/**
 * A bad statement
 */
class BadStatement : public Statement {
 public:
  explicit BadStatement(const SourcePosition &pos)
      : Statement(AstNode::Kind::BadStatement, pos) {}

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::BadStatement;
  }
};

/**
 * A block of statements
 */
class BlockStatement : public Statement {
 public:
  BlockStatement(const SourcePosition &pos, const SourcePosition &rbrace_pos,
                 util::RegionVector<Statement *> &&statements)
      : Statement(Kind::BlockStatement, pos),
        rbrace_pos_(rbrace_pos),
        statements_(std::move(statements)) {}

  util::RegionVector<Statement *> &statements() { return statements_; }

  const SourcePosition &right_brace_position() const { return rbrace_pos_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::BlockStatement;
  }

 private:
  const SourcePosition rbrace_pos_;

  util::RegionVector<Statement *> statements_;
};

/**
 * A declaration
 */
class DeclarationStatement : public Statement {
 public:
  explicit DeclarationStatement(Declaration *decl)
      : Statement(Kind::DeclarationStatement, decl->position()), decl_(decl) {}

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
  explicit ExpressionStatement(Expression *expr);

  Expression *expression() { return expr_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::ExpressionStatement;
  }

 private:
  Expression *expr_;
};

/**
 * A for statement
 */
class ForStatement : public Statement {
 public:
  ForStatement(const SourcePosition &pos, Statement *init, Expression *cond,
               Statement *next, BlockStatement *body)
      : Statement(AstNode::Kind::ForStatement, pos),
        init_(init),
        cond_(cond),
        next_(next),
        body_(body) {}

  Statement *init() const { return init_; }
  Expression *cond() const { return cond_; }
  Statement *next() const { return next_; }
  BlockStatement *body() const { return body_; }

  bool is_infinite() const {
    return init_ == nullptr && cond_ == nullptr && next_ == nullptr;
  }

  bool is_while_like() const {
    return init_ == nullptr && cond_ != nullptr && next_ == nullptr;
  }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::ForStatement;
  }

 private:
  Statement *init_;
  Expression *cond_;
  Statement *next_;
  BlockStatement *body_;
};

/**
 * An if-then-else statement
 */
class IfStatement : public Statement {
 public:
  IfStatement(const SourcePosition &pos, Expression *cond,
              BlockStatement *then_stmt, Statement *else_stmt)
      : Statement(Kind::IfStatement, pos),
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
  ReturnStatement(const SourcePosition &pos, Expression *ret)
      : Statement(Kind::ReturnStatement, pos), ret_(ret) {}

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
  Expression(Kind kind, const SourcePosition &pos, Type *type = nullptr)
      : AstNode(kind, pos), type_(type) {}

  Type *type() { return type_; }
  const Type *type() const { return type_; }

  void set_type(Type *type) { type_ = type; }

  static bool classof(const AstNode *node) {
    return node->kind() >= Kind::BadExpression &&
           node->kind() <= Kind::StructTypeRepr;
  }

 private:
  Type *type_;
};

/**
 * A bad statement
 */
class BadExpression : public Expression {
 public:
  explicit BadExpression(const SourcePosition &pos)
      : Expression(AstNode::Kind::BadExpression, pos) {}

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::BadExpression;
  }
};

/**
 * A binary expression with non-null left and right children and an operator
 */
class BinaryExpression : public Expression {
 public:
  BinaryExpression(const SourcePosition &pos, parsing::Token::Type op,
                   Expression *left, Expression *right)
      : Expression(Kind::BinaryExpression, pos),
        op_(op),
        left_(left),
        right_(right) {}

  parsing::Token::Type op() { return op_; }

  Expression *left() { return left_; }

  Expression *right() { return right_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::BinaryExpression;
  }

 private:
  parsing::Token::Type op_;
  Expression *left_;
  Expression *right_;
};

/**
 * A function call expression
 */
class CallExpression : public Expression {
 public:
  CallExpression(Expression *fun, util::RegionVector<Expression *> &&args)
      : Expression(Kind::CallExpression, fun->position()),
        fun_(fun),
        args_(std::move(args)) {}

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
  FunctionLiteralExpression(FunctionTypeRepr *type_repr, BlockStatement *body);

  FunctionTypeRepr *type_repr() const { return type_repr_; }

  BlockStatement *body() const { return body_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::FunctionLiteralExpression;
  }

 private:
  FunctionTypeRepr *type_repr_;
  BlockStatement *body_;
};

/**
 * A reference to a variable, function or struct
 */
class IdentifierExpression : public Expression {
 public:
  IdentifierExpression(const SourcePosition &pos, Identifier name)
      : Expression(Kind::IdentifierExpression, pos),
        name_(name),
        decl_(nullptr) {}

  Identifier name() const { return name_; }

  void BindTo(Declaration *decl) { decl_ = decl; }

  bool is_bound() const { return decl_ != nullptr; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::IdentifierExpression;
  }

 private:
  // TODO(pmenon) Should these two be a union since only one should be active?
  // Pre-binding, 'name_' is used, and post-binding 'decl_' should be used?
  Identifier name_;
  Declaration *decl_;
};

/**
 * A literal in the original source code
 */
class LiteralExpression : public Expression {
 public:
  enum class LitKind : uint8_t { Nil, Boolean, Int, Float, String };

  explicit LiteralExpression(const SourcePosition &pos)
      : Expression(Kind::LiteralExpression, pos),
        lit_kind_(LiteralExpression::LitKind::Nil) {}

  LiteralExpression(const SourcePosition &pos, bool val)
      : Expression(Kind::LiteralExpression, pos),
        lit_kind_(LiteralExpression::LitKind::Boolean),
        boolean_(val) {}

  LiteralExpression(const SourcePosition &pos,
                    LiteralExpression::LitKind lit_kind, Identifier str)
      : Expression(Kind::LiteralExpression, pos),
        lit_kind_(lit_kind),
        str_(str) {}

  LiteralExpression::LitKind literal_kind() const { return lit_kind_; }

  bool bool_val() const {
    TPL_ASSERT(literal_kind() == LitKind::Boolean,
               "Getting boolean value from a non-bool expression!");
    return boolean_;
  }

  Identifier raw_string() const {
    TPL_ASSERT(
        literal_kind() != LitKind::Nil && literal_kind() != LitKind::Boolean,
        "Getting a raw string value from a non-string or numeric value");
    return str_;
  }

  int64_t integer() const {
    TPL_ASSERT(literal_kind() == LitKind::Int,
               "Getting integer value from a non-integer literal expression");
    // TODO(pmenon): Check safe conversion
    char *end;
    return std::strtol(str_.data(), &end, 10);
  }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::LiteralExpression;
  }

 private:
  LitKind lit_kind_;

  union {
    bool boolean_;
    Identifier str_;
  };
};

/**
 * A unary expression with a non-null inner expression and an operator
 */
class UnaryExpression : public Expression {
 public:
  UnaryExpression(const SourcePosition &pos, parsing::Token::Type op,
                  Expression *expr)
      : Expression(Kind::UnaryExpression, pos), op_(op), expr_(expr) {}

  parsing::Token::Type op() { return op_; }

  Expression *expr() { return expr_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::UnaryExpression;
  }

 private:
  parsing::Token::Type op_;
  Expression *expr_;
};

////////////////////////////////////////////////////////////////////////////////
///
/// Types
///
////////////////////////////////////////////////////////////////////////////////

/**
 * Array type
 */
class ArrayTypeRepr : public Expression {
 public:
  ArrayTypeRepr(const SourcePosition &pos, Expression *len,
                Expression *elem_type)
      : Expression(Kind::ArrayTypeRepr, pos),
        len_(len),
        elem_type_(elem_type) {}

  Expression *length() const { return len_; }

  Expression *element_type() const { return elem_type_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::ArrayTypeRepr;
  }

 private:
  Expression *len_;
  Expression *elem_type_;
};

/**
 * Function type
 */
class FunctionTypeRepr : public Expression {
 public:
  FunctionTypeRepr(const SourcePosition &pos,
                   util::RegionVector<FieldDeclaration *> &&param_types,
                   Expression *ret_type)
      : Expression(Kind::FunctionTypeRepr, pos),
        param_types_(std::move(param_types)),
        ret_type_(ret_type) {}

  const util::RegionVector<FieldDeclaration *> &parameters() const {
    return param_types_;
  }

  Expression *return_type() const { return ret_type_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::FunctionTypeRepr;
  }

 private:
  util::RegionVector<FieldDeclaration *> param_types_;
  Expression *ret_type_;
};

/**
 * Pointer type
 */
class PointerTypeRepr : public Expression {
 public:
  PointerTypeRepr(const SourcePosition &pos, Expression *base)
      : Expression(Kind::PointerTypeRepr, pos), base_(base) {}

  Expression *base() const { return base_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::PointerTypeRepr;
  }

 private:
  Expression *base_;
};

/**
 * Struct type
 */
class StructTypeRepr : public Expression {
 public:
  StructTypeRepr(const SourcePosition &pos,
                 util::RegionVector<FieldDeclaration *> &&fields)
      : Expression(Kind::StructTypeRepr, pos), fields_(std::move(fields)) {}

  const util::RegionVector<FieldDeclaration *> &fields() const {
    return fields_;
  }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::StructTypeRepr;
  }

 private:
  util::RegionVector<FieldDeclaration *> fields_;
};

}  // namespace tpl::ast