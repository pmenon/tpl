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
 * remember to modify Decl::classof() to update the bounds check.
 */
#define DECLARATION_NODES(T) \
  T(FieldDecl)               \
  T(FunctionDecl)            \
  T(StructDecl)              \
  T(VariableDecl)

/*
 * All possible statements
 *
 * If you add a new statement node to either the beginning or end of the list,
 * remember to modify Statement::classof() to update the bounds check.
 */
#define STATEMENT_NODES(T) \
  T(AssignmentStmt)        \
  T(BadStmt)               \
  T(BlockStmt)             \
  T(DeclStmt)              \
  T(ExpressionStmt)        \
  T(ForStmt)               \
  T(IfStmt)                \
  T(ReturnStmt)

/*
 * All possible expressions
 *
 * If you add a new expression node to either the beginning or end of the list,
 * remember to modify Expression::classof() to update the bounds check.
 */
#define EXPRESSION_NODES(T) \
  T(BadExpr)                \
  T(BinaryOpExpr)           \
  T(CallExpr)               \
  T(FunctionLitExpr)        \
  T(IdentifierExpr)         \
  T(LitExpr)                \
  T(UnaryOpExpr)            \
  /* Types */               \
  T(ArrayTypeRepr)          \
  T(FunctionTypeRepr)       \
  T(PointerTypeRepr)        \
  T(StructTypeRepr)

/*
 * All possible AST nodes
 */
#define AST_NODES(T)   \
  DECLARATION_NODES(T) \
  EXPRESSION_NODES(T)  \
  FILE_NODE(T)         \
  STATEMENT_NODES(T)

class Decl;
class Expression;
class Stmt;

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
  File(const SourcePosition &pos, util::RegionVector<Decl *> &&decls)
      : AstNode(Kind::File, pos), decls_(std::move(decls)) {}

  util::RegionVector<Decl *> &declarations() { return decls_; }

  static bool classof(const AstNode *node) {
    return node->kind() >= Kind::File;
  }

 private:
  util::RegionVector<Decl *> decls_;
};

////////////////////////////////////////////////////////////////////////////////
///
/// Decl nodes
///
////////////////////////////////////////////////////////////////////////////////

class Decl : public AstNode {
 public:
  Decl(Kind kind, const SourcePosition &pos, Identifier name)
      : AstNode(kind, pos), name_(name) {}

  Identifier name() const { return name_; }

  static bool classof(const AstNode *node) {
    return node->kind() >= Kind::FieldDecl &&
           node->kind() <= Kind::VariableDecl;
  }

 private:
  Identifier name_;
};

/**
 * A generic declaration of a function argument or a field in a struct
 */
class FieldDecl : public Decl {
 public:
  FieldDecl(const SourcePosition &pos, Identifier name, Expression *type_repr)
      : Decl(Kind::FieldDecl, pos, name), type_repr_(type_repr) {}

  Expression *type_repr() const { return type_repr_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::FieldDecl;
  }

 private:
  Expression *type_repr_;
};

/**
 * A function declaration
 */
class FunctionDecl : public Decl {
 public:
  FunctionDecl(const SourcePosition &pos, Identifier name, FunctionLitExpr *fun)
      : Decl(Kind::FunctionDecl, pos, name), fun_(fun) {}

  FunctionLitExpr *function() const { return fun_; }

  FunctionTypeRepr *type_repr();

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::FunctionDecl;
  }

 private:
  FunctionLitExpr *fun_;
};

/**
 *
 */
class StructDecl : public Decl {
 public:
  StructDecl(const SourcePosition &pos, Identifier name,
             StructTypeRepr *type_repr)
      : Decl(Kind::StructDecl, pos, name), type_repr_(type_repr) {}

  StructTypeRepr *type_repr() const { return type_repr_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::StructDecl;
  }

 private:
  StructTypeRepr *type_repr_;
};

/**
 * A variable declaration
 */
class VariableDecl : public Decl {
 public:
  VariableDecl(const SourcePosition &pos, Identifier name,
               Expression *type_repr, Expression *init)
      : Decl(Kind::VariableDecl, pos, name),
        type_repr_(type_repr),
        init_(init) {}

  Expression *type_repr() const { return type_repr_; }

  Expression *initial() const { return init_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::VariableDecl;
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
class Stmt : public AstNode {
 public:
  Stmt(Kind kind, const SourcePosition &pos) : AstNode(kind, pos) {}

  static bool classof(const AstNode *node) {
    return node->kind() >= Kind::AssignmentStmt &&
           node->kind() <= Kind::ReturnStmt;
  }
};

/**
 * An assignment, dest = source
 */
class AssignmentStmt : public Stmt {
 public:
  AssignmentStmt(const SourcePosition &pos, Expression *dest, Expression *src)
      : Stmt(AstNode::Kind::AssignmentStmt, pos), dest_(dest), src_(src) {}

  Expression *destination() const { return dest_; }

  Expression *source() const { return src_; };

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::AssignmentStmt;
  }

 private:
  Expression *dest_;
  Expression *src_;
};

/**
 * A bad statement
 */
class BadStmt : public Stmt {
 public:
  explicit BadStmt(const SourcePosition &pos)
      : Stmt(AstNode::Kind::BadStmt, pos) {}

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::BadStmt;
  }
};

/**
 * A block of statements
 */
class BlockStmt : public Stmt {
 public:
  BlockStmt(const SourcePosition &pos, const SourcePosition &rbrace_pos,
            util::RegionVector<Stmt *> &&statements)
      : Stmt(Kind::BlockStmt, pos),
        rbrace_pos_(rbrace_pos),
        statements_(std::move(statements)) {}

  util::RegionVector<Stmt *> &statements() { return statements_; }

  const SourcePosition &right_brace_position() const { return rbrace_pos_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::BlockStmt;
  }

 private:
  const SourcePosition rbrace_pos_;

  util::RegionVector<Stmt *> statements_;
};

/**
 * A statement that is just a declaration
 */
class DeclStmt : public Stmt {
 public:
  explicit DeclStmt(Decl *decl)
      : Stmt(Kind::DeclStmt, decl->position()), decl_(decl) {}

  Decl *declaration() const { return decl_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::DeclStmt;
  }

 private:
  Decl *decl_;
};

/**
 * The bridge between statements and expressions
 */
class ExpressionStmt : public Stmt {
 public:
  explicit ExpressionStmt(Expression *expr);

  Expression *expression() { return expr_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::ExpressionStmt;
  }

 private:
  Expression *expr_;
};

/**
 * A for statement
 */
class ForStmt : public Stmt {
 public:
  ForStmt(const SourcePosition &pos, Stmt *init, Expression *cond, Stmt *next,
          BlockStmt *body)
      : Stmt(AstNode::Kind::ForStmt, pos),
        init_(init),
        cond_(cond),
        next_(next),
        body_(body) {}

  Stmt *init() const { return init_; }
  Expression *cond() const { return cond_; }
  Stmt *next() const { return next_; }
  BlockStmt *body() const { return body_; }

  bool is_infinite() const {
    return init_ == nullptr && cond_ == nullptr && next_ == nullptr;
  }

  bool is_while_like() const {
    return init_ == nullptr && cond_ != nullptr && next_ == nullptr;
  }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::ForStmt;
  }

 private:
  Stmt *init_;
  Expression *cond_;
  Stmt *next_;
  BlockStmt *body_;
};

/**
 * An if-then-else statement
 */
class IfStmt : public Stmt {
 public:
  IfStmt(const SourcePosition &pos, Expression *cond, BlockStmt *then_stmt,
         Stmt *else_stmt)
      : Stmt(Kind::IfStmt, pos),
        cond_(cond),
        then_stmt_(then_stmt),
        else_stmt_(else_stmt) {}

  Expression *cond() { return cond_; }

  BlockStmt *then_stmt() { return then_stmt_; }

  Stmt *else_stmt() { return else_stmt_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::IfStmt;
  }

 private:
  Expression *cond_;
  BlockStmt *then_stmt_;
  Stmt *else_stmt_;
};

/**
 * A return statement
 */
class ReturnStmt : public Stmt {
 public:
  ReturnStmt(const SourcePosition &pos, Expression *ret)
      : Stmt(Kind::ReturnStmt, pos), ret_(ret) {}

  Expression *ret() { return ret_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::ReturnStmt;
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
    return node->kind() >= Kind::BadExpr &&
           node->kind() <= Kind::StructTypeRepr;
  }

 private:
  Type *type_;
};

/**
 * A bad statement
 */
class BadExpr : public Expression {
 public:
  explicit BadExpr(const SourcePosition &pos)
      : Expression(AstNode::Kind::BadExpr, pos) {}

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::BadExpr;
  }
};

/**
 * A binary expression with non-null left and right children and an operator
 */
class BinaryOpExpr : public Expression {
 public:
  BinaryOpExpr(const SourcePosition &pos, parsing::Token::Type op,
               Expression *left, Expression *right)
      : Expression(Kind::BinaryOpExpr, pos),
        op_(op),
        left_(left),
        right_(right) {}

  parsing::Token::Type op() { return op_; }

  Expression *left() { return left_; }

  Expression *right() { return right_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::BinaryOpExpr;
  }

 private:
  parsing::Token::Type op_;
  Expression *left_;
  Expression *right_;
};

/**
 * A function call expression
 */
class CallExpr : public Expression {
 public:
  CallExpr(Expression *fun, util::RegionVector<Expression *> &&args)
      : Expression(Kind::CallExpr, fun->position()),
        fun_(fun),
        args_(std::move(args)) {}

  Expression *function() { return fun_; }

  util::RegionVector<Expression *> &arguments() { return args_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::CallExpr;
  }

 private:
  Expression *fun_;
  util::RegionVector<Expression *> args_;
};

class FunctionLitExpr : public Expression {
 public:
  FunctionLitExpr(FunctionTypeRepr *type_repr, BlockStmt *body);

  FunctionTypeRepr *type_repr() const { return type_repr_; }

  BlockStmt *body() const { return body_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::FunctionLitExpr;
  }

 private:
  FunctionTypeRepr *type_repr_;
  BlockStmt *body_;
};

/**
 * A reference to a variable, function or struct
 */
class IdentifierExpr : public Expression {
 public:
  IdentifierExpr(const SourcePosition &pos, Identifier name)
      : Expression(Kind::IdentifierExpr, pos), name_(name), decl_(nullptr) {}

  Identifier name() const { return name_; }

  void BindTo(Decl *decl) { decl_ = decl; }

  bool is_bound() const { return decl_ != nullptr; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::IdentifierExpr;
  }

 private:
  // TODO(pmenon) Should these two be a union since only one should be active?
  // Pre-binding, 'name_' is used, and post-binding 'decl_' should be used?
  Identifier name_;
  Decl *decl_;
};

/**
 * A literal in the original source code
 */
class LitExpr : public Expression {
 public:
  enum class LitKind : uint8_t { Nil, Boolean, Int, Float, String };

  explicit LitExpr(const SourcePosition &pos)
      : Expression(Kind::LitExpr, pos), lit_kind_(LitExpr::LitKind::Nil) {}

  LitExpr(const SourcePosition &pos, bool val)
      : Expression(Kind::LitExpr, pos),
        lit_kind_(LitExpr::LitKind::Boolean),
        boolean_(val) {}

  LitExpr(const SourcePosition &pos, LitExpr::LitKind lit_kind, Identifier str)
      : Expression(Kind::LitExpr, pos), lit_kind_(lit_kind), str_(str) {}

  LitExpr::LitKind literal_kind() const { return lit_kind_; }

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
    return node->kind() == Kind::LitExpr;
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
class UnaryOpExpr : public Expression {
 public:
  UnaryOpExpr(const SourcePosition &pos, parsing::Token::Type op,
              Expression *expr)
      : Expression(Kind::UnaryOpExpr, pos), op_(op), expr_(expr) {}

  parsing::Token::Type op() { return op_; }

  Expression *expr() { return expr_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::UnaryOpExpr;
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
                   util::RegionVector<FieldDecl *> &&param_types,
                   Expression *ret_type)
      : Expression(Kind::FunctionTypeRepr, pos),
        param_types_(std::move(param_types)),
        ret_type_(ret_type) {}

  const util::RegionVector<FieldDecl *> &parameters() const {
    return param_types_;
  }

  Expression *return_type() const { return ret_type_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::FunctionTypeRepr;
  }

 private:
  util::RegionVector<FieldDecl *> param_types_;
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
                 util::RegionVector<FieldDecl *> &&fields)
      : Expression(Kind::StructTypeRepr, pos), fields_(std::move(fields)) {}

  const util::RegionVector<FieldDecl *> &fields() const { return fields_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::StructTypeRepr;
  }

 private:
  util::RegionVector<FieldDecl *> fields_;
};

}  // namespace tpl::ast