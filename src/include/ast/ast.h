#pragma once

#include <cstdint>
#include <string>
#include <utility>

#include "llvm/Support/Casting.h"

#include "ast/identifier.h"
#include "common/common.h"
#include "parsing/token.h"
#include "util/region.h"
#include "util/region_containers.h"

namespace tpl {

namespace sema {
class Sema;
}  // namespace sema

namespace ast {

/**
 * Top-level file node
 */
#define FILE_NODE(T) T(File)

/**
 * All possible declaration types.
 * NOTE: If you add a new declaration node to either the beginning or end of the list, remember to
 *       modify Declaration::classof() to update the bounds check.
 */
#define DECLARATION_NODES(T) \
  T(FieldDeclaration)        \
  T(FunctionDeclaration)     \
  T(StructDeclaration)       \
  T(VariableDeclaration)

/**
 * All possible statements
 * NOTE: If you add a new statement node to either the beginning or end of the list, remember to
 *       modify Statement::classof() to update the bounds check.
 */
#define STATEMENT_NODES(T) \
  T(AssignmentStatement)   \
  T(BlockStatement)        \
  T(DeclarationStatement)  \
  T(ExpressionStatement)   \
  T(ForStatement)          \
  T(ForInStatement)        \
  T(IfStatement)           \
  T(ReturnStatement)

/**
 * All possible expressions
 * NOTE: If you add a new expression node to either the beginning or end of the
 * list, remember to modify Expr::classof() to update the bounds check.
 */
#define EXPRESSION_NODES(T)             \
  T(BadExpr)                            \
  T(BinaryOpExpr)                       \
  T(CallExpr)                           \
  T(ComparisonOpExpr)                   \
  T(FunctionLiteralExpr)                \
  T(IdentifierExpr)                     \
  T(ImplicitCastExpr)                   \
  T(IndexExpr)                          \
  T(LiteralExpr)                        \
  T(MemberExpr)                         \
  T(UnaryOpExpr)                        \
  /* Type Representation Expressions */ \
  T(ArrayTypeRepr)                      \
  T(FunctionTypeRepr)                   \
  T(MapTypeRepr)                        \
  T(PointerTypeRepr)                    \
  T(StructTypeRepr)

/**
 * All AST nodes
 */
#define AST_NODES(T)   \
  DECLARATION_NODES(T) \
  EXPRESSION_NODES(T)  \
  FILE_NODE(T)         \
  STATEMENT_NODES(T)

// Forward declare some base classes
class Declaration;
class Expr;
class Statement;
class Type;

// Forward declare all nodes
#define FORWARD_DECLARE(name) class name;
AST_NODES(FORWARD_DECLARE)
#undef FORWARD_DECLARE

// ---------------------------------------------------------
// AST Node
// ---------------------------------------------------------

/**
 * The base class for all AST nodes. AST nodes can only allocated from regions. This is because they
 * are often allocated and de-allocated in a bulk-process, i.e., during parsing and compilation.
 * AST nodes are effectively immutable after they've been constructed. The only exception is during
 * semantic analysis where TPL types are filled in. This is why you'll often see sema::Sema declared
 * as a friend class in some concrete node subclasses.
 *
 * All AST nodes have a "kind" that represents as an ID indicating the specific kind of AST node it
 * is (i.e., an if-statement, loop, or a binary expression). You can query the node for its kind,
 * but it's usually more informative and clear to use Is(). We use kind instead of type to not
 * confuse the type of TPL AST node it is, and it's resolved TPL type as it appears in TPL code.
 */
class AstNode : public util::RegionObject {
 public:
  // The kind enumeration listing all possible node kinds
#define T(kind) kind,
  enum class Kind : uint8_t { AST_NODES(T) };
#undef T

  /**
   * @return The kind of this node.
   */
  Kind GetKind() const { return kind_; }

  /**
   * @return The position in the source where this element was found.
   */
  const SourcePosition &Position() const { return pos_; }

  /**
   * @return The name of this node. NOTE: this is mainly used in tests!
   */
  const char *KindName() const {
#define KIND_CASE(kind) \
  case Kind::kind:      \
    return #kind;

    // Main type switch
    // clang-format off
    switch (GetKind()) {
      default: { UNREACHABLE("Impossible kind name"); }
      AST_NODES(KIND_CASE)
    }
      // clang-format on
#undef KIND_CASE
  }

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
  // The kind of AST node.
  Kind kind_;
  // The position in the original source where this node's underlying
  // information was found.
  const SourcePosition pos_;
};

/**
 * Represents a file composed of a list of declarations.
 */
class File : public AstNode {
 public:
  /**
   * @return A const-view of the declarations making up the file.
   */
  const util::RegionVector<Declaration *> &GetDeclarations() const { return decls_; }

  /**
   * Is the given node an AST File? Needed as part of the custom AST RTTI infrastructure.
   * @param node The node to check.
   * @return True if the node is a file; false otherwise.
   */
  static bool classof(const AstNode *node) { return node->GetKind() == Kind::File; }

 private:
  friend class AstNodeFactory;

  // Private to force factory usage.
  File(const SourcePosition &pos, util::RegionVector<Declaration *> &&decls)
      : AstNode(Kind::File, pos), decls_(std::move(decls)) {}

 private:
  // The declarations.
  util::RegionVector<Declaration *> decls_;
};

// ---------------------------------------------------------
// Declaration Nodes
// ---------------------------------------------------------

/**
 * Base class for all declarations in TPL. All declarations have a name, and an optional type
 * representation. Structure and function declarations have an explicit type, but variables may not.
 */
class Declaration : public AstNode {
 public:
  /**
   * @return The name of the declaration as it appears in code.
   */
  Identifier GetName() const { return name_; }

  /**
   * @return The type representation of the declaration. May be null for variables.
   */
  Expr *GetTypeRepr() const { return type_repr_; }

  /**
   * Is the given node an AST Declaration? Needed as part of the custom AST RTTI infrastructure.
   * @param node The node to check.
   * @return True if the node is a declaration; false otherwise.
   */
  static bool classof(const AstNode *node) {
    return node->GetKind() >= Kind::FieldDeclaration &&
           node->GetKind() <= Kind::VariableDeclaration;
  }

 protected:
  // Protected to force usage of concrete subclass.
  Declaration(Kind kind, const SourcePosition &pos, Identifier name, Expr *type_repr)
      : AstNode(kind, pos), name_(name), type_repr_(type_repr) {}

 private:
  // The name of the declaration.
  Identifier name_;
  // The unresolved type representation of the declaration.
  Expr *type_repr_;
};

/**
 * A generic declaration of a function argument or a field in a struct.
 */
class FieldDeclaration : public Declaration {
 public:
  /**
   * Is the given node an AST field? Needed as part of the custom AST RTTI infrastructure.
   * @param node The node to check.
   * @return True if the node is a field; false otherwise.
   */
  static bool classof(const AstNode *node) { return node->GetKind() == Kind::FieldDeclaration; }

 private:
  friend class AstNodeFactory;

  // Private to force factory usage.
  FieldDeclaration(const SourcePosition &pos, Identifier name, Expr *type_repr)
      : Declaration(Kind::FieldDeclaration, pos, name, type_repr) {}
};

/**
 * A function declaration.
 */
class FunctionDeclaration : public Declaration {
 public:
  /**
   * @return The function literal defining the body of the function declaration.
   */
  FunctionLiteralExpr *GetFunctionLiteral() const { return func_; }

  /**
   * Is the given node a function declaration? Needed as part of the custom AST RTTI infrastructure.
   * @param node The node to check.
   * @return True if the node is a function declaration; false otherwise.
   */
  static bool classof(const AstNode *node) { return node->GetKind() == Kind::FunctionDeclaration; }

 private:
  friend class AstNodeFactory;

  // Private to force factory usage.
  FunctionDeclaration(const SourcePosition &pos, Identifier name, FunctionLiteralExpr *func);

 private:
  // The function definition (signature and body).
  FunctionLiteralExpr *func_;
};

/**
 * A structure declaration.
 */
class StructDeclaration : public Declaration {
 public:
  /**
   * @return The number of fields in the declaration.
   */
  uint32_t NumFields() const;

  /**
   * @return The field at the given index within the structure declaration. Note: this method does
   *         not perform any bounds checking. It is the responsibility of the caller to access only
   *         valid fields.
   */
  FieldDeclaration *GetFieldAt(uint32_t field_idx) const;

  /**
   * Is the given node a struct declaration? Needed as part of the custom AST RTTI infrastructure.
   * @param node The node to check.
   * @return True if the node is a struct declaration; false otherwise.
   */
  static bool classof(const AstNode *node) { return node->GetKind() == Kind::StructDeclaration; }

 private:
  friend class AstNodeFactory;

  // Private to force factory usage.
  StructDeclaration(const SourcePosition &pos, Identifier name, StructTypeRepr *type_repr);
};

/**
 * A variable declaration.
 */
class VariableDeclaration : public Declaration {
 public:
  /**
   * @return The initial value assigned to the variable, if one was provided; null otherwise.
   */
  Expr *GetInitialValue() const { return init_; }

  /**
   * @return True if the variable declaration came with an explicit type, i.e., var v: int = 0.
   *         False if no explicit type was provided.
   */
  bool HasDeclaredType() const { return GetTypeRepr() != nullptr; }

  /**
   * @return True if the variable is assigned an initial value; false otherwise.
   */
  bool HasInitialValue() const { return init_ != nullptr; }

  /**
   * Is the given node a variable declaration? Needed as part of the custom AST RTTI infrastructure.
   * @param node The node to check.
   * @return True if the node is a variable declaration; false otherwise.
   */
  static bool classof(const AstNode *node) { return node->GetKind() == Kind::VariableDeclaration; }

 private:
  friend class AstNodeFactory;
  friend class sema::Sema;

  // Private to force factory usage.
  VariableDeclaration(const SourcePosition &pos, Identifier name, Expr *type_repr, Expr *init)
      : Declaration(Kind::VariableDeclaration, pos, name, type_repr), init_(init) {}

  // Only set during semantic analysis.
  void SetInitialValue(Expr *initial) { init_ = initial; }

 private:
  Expr *init_;
};

// ---------------------------------------------------------
// Statement Nodes
// ---------------------------------------------------------

/**
 * Base class for all statement nodes.
 */
class Statement : public AstNode {
 public:
  /**
   * Determines if the provided statement, the last in a statement list, is terminating.
   * @param stmt The statement node to check.
   * @return True if statement has a terminator; false otherwise.
   */
  static bool IsTerminating(Statement *stmt);

  /**
   * Is the given node an AST statement? Needed as part of the custom AST RTTI infrastructure.
   * @param node The node to check.
   * @return True if the node is a statement; false otherwise.
   */
  static bool classof(const AstNode *node) {
    return node->GetKind() >= Kind::AssignmentStatement && node->GetKind() <= Kind::ReturnStatement;
  }

 protected:
  // Protected to force usage of concrete subclass.
  Statement(Kind kind, const SourcePosition &pos) : AstNode(kind, pos) {}
};

/**
 * An assignment, dest = source.
 */
class AssignmentStatement : public Statement {
 public:
  /**
   * @return The target/destination of the assignment.
   */
  Expr *GetDestination() { return dest_; }

  /**
   * @return The source of the assignment.
   */
  Expr *GetSource() { return src_; }

  /**
   * Is the given node an AST assignment? Needed as part of the custom AST RTTI infrastructure.
   * @param node The node to check.
   * @return True if the node is a assignment; false otherwise.
   */
  static bool classof(const AstNode *node) { return node->GetKind() == Kind::AssignmentStatement; }

 private:
  friend class AstNodeFactory;
  friend class sema::Sema;

  // Private to force factory usage.
  AssignmentStatement(const SourcePosition &pos, Expr *dest, Expr *src)
      : Statement(AstNode::Kind::AssignmentStatement, pos), dest_(dest), src_(src) {}

  // Used for implicit casts
  void SetSource(Expr *source) { src_ = source; }

 private:
  // The destination of the assignment.
  Expr *dest_;
  // The source of the assignment.
  Expr *src_;
};

/**
 * A list statements all within one block scope.
 */
class BlockStatement : public Statement {
 public:
  /**
   * @return The statements making up the block.
   */
  const util::RegionVector<Statement *> &GetStatements() const { return statements_; }

  /**
   * Append a new statement to the list of statements.
   * @param stmt The statement to append.
   */
  void AppendStatement(Statement *stmt) { statements_.push_back(stmt); }

  /**
   * @return The position of the right-brace.
   */
  const SourcePosition &GetRightBracePosition() const { return rbrace_pos_; }

  /**
   * Set the right-brace position for the end of the block.
   * @param pos The right brace position.
   */
  void SetRightBracePosition(const SourcePosition &pos) { rbrace_pos_ = pos; }

  /**
   * @return True if the block is empty; false otherwise.
   */
  bool IsEmpty() const { return statements_.empty(); }

  /**
   * @return The last statement in the block; null if the block is empty;
   */
  Statement *GetLast() const { return (IsEmpty() ? nullptr : statements_.back()); }

  /**
   * Is the given node an AST statement list? Needed as part of the custom AST RTTI infrastructure.
   * @param node The node to check.
   * @return True if the node is a statement list; false otherwise.
   */
  static bool classof(const AstNode *node) { return node->GetKind() == Kind::BlockStatement; }

 private:
  friend class AstNodeFactory;

  // Private to force factory usage.
  BlockStatement(const SourcePosition &pos, const SourcePosition &rbrace_pos,
                 util::RegionVector<Statement *> &&statements)
      : Statement(Kind::BlockStatement, pos),
        rbrace_pos_(rbrace_pos),
        statements_(std::move(statements)) {}

 private:
  // The right brace position.
  SourcePosition rbrace_pos_;
  // The list of statements.
  util::RegionVector<Statement *> statements_;
};

/**
 * The bridge between statements and declarations.
 */
class DeclarationStatement : public Statement {
 public:
  /**
   * @return The wrapped declaration.
   */
  Declaration *GetDeclaration() const { return decl_; }

  /**
   * Is the given node an AST declaration? Needed as part of the custom AST RTTI infrastructure.
   * @param node The node to check.
   * @return True if the node is a declaration; false otherwise.
   */
  static bool classof(const AstNode *node) { return node->GetKind() == Kind::DeclarationStatement; }

 private:
  friend class AstNodeFactory;

  // Private to force factory usage.
  explicit DeclarationStatement(Declaration *decl)
      : Statement(Kind::DeclarationStatement, decl->Position()), decl_(decl) {}

 private:
  // The wrapped declaration.
  Declaration *decl_;
};

/**
 * The bridge between statements and expressions.
 */
class ExpressionStatement : public Statement {
 public:
  /**
   * @return The wrapped expression.
   */
  Expr *GetExpression() { return expr_; }

  /**
   * Is the given node an AST expression? Needed as part of the custom AST RTTI infrastructure.
   * @param node The node to check.
   * @return True if the node is an expression; false otherwise.
   */
  static bool classof(const AstNode *node) { return node->GetKind() == Kind::ExpressionStatement; }

 private:
  friend class AstNodeFactory;

  // Private to force factory usage.
  explicit ExpressionStatement(Expr *expr);

 private:
  // The wrapped expression.
  Expr *expr_;
};

/**
 * Base class for all iteration-based statements
 */
class IterationStatement : public Statement {
 public:
  /**
   * @return The block making up the body of the iteration.
   */
  BlockStatement *GetBody() const { return body_; }

  /**
   * Is the given node an AST iteration? Needed as part of the custom AST RTTI infrastructure.
   * @param node The node to check.
   * @return True if the node is an iteration; false otherwise.
   */
  static bool classof(const AstNode *node) {
    return node->GetKind() >= Kind::ForStatement && node->GetKind() <= Kind::ForInStatement;
  }

 protected:
  // Protected to force usage of concrete subclass.
  IterationStatement(const SourcePosition &pos, AstNode::Kind kind, BlockStatement *body)
      : Statement(kind, pos), body_(body) {}

 private:
  // The body of the iteration.
  BlockStatement *body_;
};

/**
 * A vanilla for-statement.
 */
class ForStatement : public IterationStatement {
 public:
  /**
   * @return The initialization statement(s). Can be null.
   */
  Statement *GetInit() const { return init_; }

  /**
   * @return The loop condition. Can be null if infinite loop.
   */
  Expr *GetCondition() const { return cond_; }

  /**
   * @return The advancement statement(s). Can be null.
   */
  Statement *GetNext() const { return next_; }

  /**
   * Is the given node an AST for loop? Needed as part of the custom AST RTTI infrastructure.
   * @param node The node to check.
   * @return True if the node is a for loop; false otherwise.
   */
  static bool classof(const AstNode *node) { return node->GetKind() == Kind::ForStatement; }

 private:
  friend class AstNodeFactory;

  // Private to force factory usage.
  ForStatement(const SourcePosition &pos, Statement *init, Expr *cond, Statement *next,
               BlockStatement *body)
      : IterationStatement(pos, AstNode::Kind::ForStatement, body),
        init_(init),
        cond_(cond),
        next_(next) {}

 private:
  Statement *init_;
  Expr *cond_;
  Statement *next_;
};

/**
 * A range for statement.
 *
 * @code
 * for (row in table) {
 *   // body
 * }
 * @endcode
 *
 * 'row' is the target and 'table' is the iterable object in a for-in statement.
 */
class ForInStatement : public IterationStatement {
 public:
  /**
   * @return The loop iteration variable.
   */
  Expr *Target() const { return target_; }

  /**
   * @return The iterable.
   */
  Expr *Iterable() const { return iter_; }

  /**
   * Is the given node an AST for-in loop? Needed as part of the custom AST RTTI infrastructure.
   * @param node The node to check.
   * @return True if the node is a for-in loop; false otherwise.
   */
  static bool classof(const AstNode *node) { return node->GetKind() == Kind::ForInStatement; }

 private:
  friend class AstNodeFactory;

  // Private to force factory usage.
  ForInStatement(const SourcePosition &pos, Expr *target, Expr *iter, BlockStatement *body)
      : IterationStatement(pos, AstNode::Kind::ForInStatement, body),
        target_(target),
        iter_(iter) {}

 private:
  Expr *target_;
  Expr *iter_;
};

/**
 * An if-then-else statement.
 */
class IfStatement : public Statement {
 public:
  /**
   * @return The if-condition.
   */
  Expr *GetCondition() const { return cond_; }

  /**
   * @return The block of statements if the condition is true.
   */
  BlockStatement *GetThenStatement() const { return then_stmt_; }

  /**
   * @return The else statement.
   */
  Statement *GetElseStatement() const { return else_stmt_; }

  /**
   * @return True if there is an else statement; false otherwise.
   */
  bool HasElseStatement() const { return else_stmt_ != nullptr; }

  /**
   * Is the given node an AST if statement? Needed as part of the custom AST RTTI infrastructure.
   * @param node The node to check.
   * @return True if the node is an if statement; false otherwise.
   */
  static bool classof(const AstNode *node) { return node->GetKind() == Kind::IfStatement; }

 private:
  friend class AstNodeFactory;
  friend class sema::Sema;

  // Private to force factory usage.
  IfStatement(const SourcePosition &pos, Expr *cond, BlockStatement *then_stmt,
              Statement *else_stmt)
      : Statement(Kind::IfStatement, pos),
        cond_(cond),
        then_stmt_(then_stmt),
        else_stmt_(else_stmt) {}

  // Set only during semantic analysis.
  void SetCondition(Expr *cond) {
    TPL_ASSERT(cond != nullptr, "Cannot set null condition");
    cond_ = cond;
  }

 private:
  // The if condition.
  Expr *cond_;
  // The block of statements if the condition is true.
  BlockStatement *then_stmt_;
  // The else statement.
  Statement *else_stmt_;
};

/**
 * A return statement.
 */
class ReturnStatement : public Statement {
 public:
  /**
   * @return The expression representing the value that's to be returned.
   */
  Expr *GetReturnValue() const { return ret_; }

  /**
   * Is the given node a return statement? Needed as part of the custom AST RTTI infrastructure.
   * @param node The node to check.
   * @return True if the node is a return statement; false otherwise.
   */
  static bool classof(const AstNode *node) { return node->GetKind() == Kind::ReturnStatement; }

 private:
  friend class AstNodeFactory;
  friend class sema::Sema;

  // Private to force factory usage.
  ReturnStatement(const SourcePosition &pos, Expr *ret)
      : Statement(Kind::ReturnStatement, pos), ret_(ret) {}

  // Set only during semantic analysis.
  void SetReturnValue(Expr *ret) { ret_ = ret; }

 private:
  // The expression representing the value that's returned.
  Expr *ret_;
};

// ---------------------------------------------------------
// Expression Nodes
// ---------------------------------------------------------

/**
 * Base class for all expression nodes. Expression nodes all have a required type. This type is
 * filled in during semantic analysis. Thus, type() will return a null pointer before type-checking.
 */
class Expr : public AstNode {
 public:
  enum class Context : uint8_t {
    LValue,
    RValue,
    Test,
    Effect,
  };

  /**
   * @return The resolved TPL type of the expression. NULL if type checking has yet to run.
   */
  Type *GetType() { return type_; }

  /**
   * @return The resolved TPL type of the expression. NULL if type checking has yet to run.
   */
  const Type *GetType() const { return type_; }

  /**
   * Set the type of the expression. Usually performed during semantic type checking.
   * @param type The type to set.
   */
  void SetType(Type *type) { type_ = type; }

  /**
   * @return True if this expression is a 'nil' literal; false otherwise.
   */
  bool IsNilLiteral() const;

  /**
   * @return True if this expression is a boolean literal (true or false); false otherwise.
   */
  bool IsBoolLiteral() const;

  /**
   * @return True if this expression is a string literal, an explicit quoted string appearing in TPL
   *         code; false otherwise.
   */
  bool IsStringLiteral() const;

  /**
   * @return True if this expression is an integer literal, an explicit number appearing in TPL
   *         code; false otherwise.
   */
  bool IsIntegerLiteral() const;

  /**
   * Is the given node an AST expression? Needed as part of the custom AST RTTI infrastructure.
   * @param node The node to check.
   * @return True if the node is an expression; false otherwise.
   */
  static bool classof(const AstNode *node) {
    return node->GetKind() >= Kind::BadExpr && node->GetKind() <= Kind::StructTypeRepr;
  }

 protected:
  // Protected to force usage of concrete subclass.
  Expr(Kind kind, const SourcePosition &pos, Type *type = nullptr)
      : AstNode(kind, pos), type_(type) {}

 private:
  // The resolved TPL type. Null if type checking has not run.
  Type *type_;
};

/**
 * A bad statement.
 */
class BadExpr : public Expr {
 public:
  static bool classof(const AstNode *node) { return node->GetKind() == Kind::BadExpr; }

 private:
  friend class AstNodeFactory;

  // Private to force factory usage.
  explicit BadExpr(const SourcePosition &pos) : Expr(AstNode::Kind::BadExpr, pos) {}
};

/**
 * A binary expression with non-null left and right children and an operator.
 */
class BinaryOpExpr : public Expr {
 public:
  /**
   * @return The parsing token representing the kind of binary operation. +, -, etc.
   */
  parsing::Token::Type Op() const { return op_; }

  /**
   * @return The left input to the binary expression.
   */
  Expr *GetLeft() const { return left_; }

  /**
   * @return The right input to the binary expression.
   */
  Expr *GetRight() const { return right_; }

  /**
   * Is the given node a binary expression? Needed as part of the custom AST RTTI infrastructure.
   * @param node The node to check.
   * @return True if the node is a binary expression; false otherwise.
   */
  static bool classof(const AstNode *node) { return node->GetKind() == Kind::BinaryOpExpr; }

 private:
  friend class AstNodeFactory;
  friend class sema::Sema;

  // Private to force factory usage.
  BinaryOpExpr(const SourcePosition &pos, parsing::Token::Type op, Expr *left, Expr *right)
      : Expr(Kind::BinaryOpExpr, pos), op_(op), left_(left), right_(right) {}

  // Set only during semantic analysis.
  void SetLeft(Expr *left) {
    TPL_ASSERT(left != nullptr, "Left cannot be null!");
    left_ = left;
  }

  // Set only during semantic analysis.
  void SetRight(Expr *right) {
    TPL_ASSERT(right != nullptr, "Right cannot be null!");
    right_ = right;
  }

 private:
  parsing::Token::Type op_;
  Expr *left_;
  Expr *right_;
};

/**
 * A function call expression.
 */
class CallExpr : public Expr {
 public:
  /**
   * The different kinds of function invocations.
   */
  enum class CallKind : uint8_t { Regular, Builtin };

  /**
   * @return The name of the function to call.
   */
  Identifier GetFuncName() const;

  /**
   * @return The function that's to be called.
   */
  Expr *GetFunction() const { return func_; }

  /**
   * @return A const-view of the arguments to the function.
   */
  const util::RegionVector<Expr *> &GetArguments() const { return args_; }

  /**
   * @return The number of call arguments.
   */
  uint32_t NumArgs() const { return static_cast<uint32_t>(args_.size()); }

  /**
   * @return The kind of call, either regular or a call to a builtin function.
   */
  CallKind GetCallKind() const { return call_kind_; }

  /**
   * @return True if the call is to a builtin function; false otherwise.
   */
  bool IsBuiltinCall() const { return call_kind_ == CallKind::Builtin; }

  /**
   * Is the given node a call? Needed as part of the custom AST RTTI infrastructure.
   * @param node The node to check.
   * @return True if the node is a call; false otherwise.
   */
  static bool classof(const AstNode *node) { return node->GetKind() == Kind::CallExpr; }

 private:
  friend class AstNodeFactory;
  friend class sema::Sema;

  // Create a regular call expression.
  // Private to force factory usage.
  CallExpr(Expr *func, util::RegionVector<Expr *> &&args)
      : CallExpr(func, std::move(args), CallKind::Regular) {}

  // Create a specific kind of call expression.
  // Private to force factory usage.
  CallExpr(Expr *func, util::RegionVector<Expr *> &&args, CallKind call_kind)
      : Expr(Kind::CallExpr, func->Position()),
        func_(func),
        args_(std::move(args)),
        call_kind_(call_kind) {}

  // Called only during semantic analysis to adjust arguments.
  void SetArgument(uint32_t arg_idx, Expr *expr) {
    TPL_ASSERT(arg_idx < NumArgs(), "Out-of-bounds argument access");
    args_[arg_idx] = expr;
  }

 private:
  // The function to call.
  Expr *func_;
  // The arguments to the invocation.
  util::RegionVector<Expr *> args_;
  // The kind of call.
  CallKind call_kind_;
};

/**
 * A binary comparison operator.
 */
class ComparisonOpExpr : public Expr {
 public:
  /**
   * @return The parsing token representing the kind of comparison, <, ==, etc.
   */
  parsing::Token::Type Op() const { return op_; }

  /**
   * @return The left input to the comparison.
   */
  Expr *GetLeft() const { return left_; }

  /**
   * @return The right input to the comparison.
   */
  Expr *GetRight() const { return right_; }

  /**
   * Is this a comparison between an expression and a nil literal?
   * @param[out] result If this is a literal nil comparison, result will point to the expression
   *                    we're checking nil against, either the left or right input.
   * @return True if this is a nil comparison; false otherwise
   */
  bool IsLiteralCompareNil(Expr **result) const;

  /**
   * Is the given node a comparison? Needed as part of the custom AST RTTI infrastructure.
   * @param node The node to check.
   * @return True if the node is a comparison; false otherwise.
   */
  static bool classof(const AstNode *node) { return node->GetKind() == Kind::ComparisonOpExpr; }

 private:
  friend class AstNodeFactory;
  friend class sema::Sema;

  // Private to force factory usage.
  ComparisonOpExpr(const SourcePosition &pos, parsing::Token::Type op, Expr *left, Expr *right)
      : Expr(Kind::ComparisonOpExpr, pos), op_(op), left_(left), right_(right) {}

  // Set only during semantic analysis.
  void SetLeft(Expr *left) {
    TPL_ASSERT(left != nullptr, "Left cannot be null!");
    left_ = left;
  }

  // Set only during semantic analysis.
  void SetRight(Expr *right) {
    TPL_ASSERT(right != nullptr, "Right cannot be null!");
    right_ = right;
  }

 private:
  // The kind of comparison.
  parsing::Token::Type op_;
  // The left side of comparison.
  Expr *left_;
  // The right side of comparison.
  Expr *right_;
};

/**
 * A function literal.
 */
class FunctionLiteralExpr : public Expr {
 public:
  /**
   * @return The function's signature.
   */
  FunctionTypeRepr *GetTypeRepr() const { return type_repr_; }

  /**
   * @return The statements making up the body of the function.
   */
  BlockStatement *GetBody() const { return body_; }

  /**
   * @return True if the function has no statements; false otherwise.
   */
  bool IsEmpty() const { return body_->IsEmpty(); }

  /**
   * Is the given node a function literal? Needed as part of the custom AST RTTI infrastructure.
   * @param node The node to check.
   * @return True if the node is a function literal; false otherwise.
   */
  static bool classof(const AstNode *node) { return node->GetKind() == Kind::FunctionLiteralExpr; }

 private:
  friend class AstNodeFactory;

  // Private to force factory usage.
  FunctionLiteralExpr(FunctionTypeRepr *type_repr, BlockStatement *body);

 private:
  // The function's signature.
  FunctionTypeRepr *type_repr_;
  // The body of the function.
  BlockStatement *body_;
};

/**
 * A reference to a variable, function or struct.
 */
class IdentifierExpr : public Expr {
 public:
  /**
   * @return The identifier the expression represents.
   */
  Identifier GetName() const { return name_; }

  /**
   * Bind an identifier to a source declaration.
   * @param decl The declaration to bind this identifier to.
   */
  void BindTo(Declaration *decl) { decl_ = decl; }

  /**
   * @return True if the expression has been bound; false otherwise.
   */
  bool IsBound() const { return decl_ != nullptr; }

  /**
   * Is the given node an identifier expression? Needed as part of the custom AST RTTI
   * infrastructure.
   * @param node The node to check.
   * @return True if the node is an identifier expression; false otherwise.
   */
  static bool classof(const AstNode *node) { return node->GetKind() == Kind::IdentifierExpr; }

 private:
  friend class AstNodeFactory;

  // Private to force factory usage.
  IdentifierExpr(const SourcePosition &pos, Identifier name)
      : Expr(Kind::IdentifierExpr, pos), name_(name), decl_(nullptr) {}

 private:
  // TODO(pmenon) Should these two be a union since only one should be active?
  // Pre-binding, 'name_' is used, and post-binding 'decl_' should be used.
  Identifier name_;
  Declaration *decl_;
};

/**
 * An enumeration capturing all possible casting operations.
 */
enum class CastKind : uint8_t {
  // Conversion of a 32-bit integer into a non-nullable SQL Integer value.
  IntToSqlInt,

  // Conversion of a 32-bit integer into a non-nullable SQL Decimal value.
  IntToSqlDecimal,

  // Conversion of a SQL boolean value into a primitive boolean value.
  SqlBoolToBool,

  // Conversion of a primitive boolean into a SQL boolean.
  BoolToSqlBool,

  // A cast between integral types (i.e., 8-bit, 16-bit, 32-bit, or 64-bit
  // numbers), excluding to boolean! Boils down to a bitcast, a truncation,
  // a sign-extension, or a zero-extension. The same as in C/C++.
  IntegralCast,

  // A simple bit cast reinterpretation.
  BitCast,

  // Conversion of a 64-bit float into a non-nullable SQL Real value.
  FloatToSqlReal,

  // Convert a SQL integer into a SQL real.
  SqlIntToSqlReal,
};

/**
 * @return A string representation for a given cast kind.
 */
std::string CastKindToString(CastKind cast_kind);

/**
 * An implicit cast operation is one that is inserted automatically by the compiler during semantic
 * analysis.
 */
class ImplicitCastExpr : public Expr {
 public:
  /**
   * @return The kind of cast operation this expression represents.
   */
  CastKind GetCastKind() const { return cast_kind_; }

  /**
   * @return The input to the cast operation.
   */
  Expr *GetInput() const { return input_; }

  /**
   * Is the given node an implicit cast? Needed as part of the custom AST RTTI infrastructure.
   * @param node The node to check.
   * @return True if the node is an implicit cast; false otherwise.
   */
  static bool classof(const AstNode *node) { return node->GetKind() == Kind::ImplicitCastExpr; }

 private:
  friend class AstNodeFactory;

  // Private to force factory usage.
  ImplicitCastExpr(const SourcePosition &pos, CastKind cast_kind, Type *target_type, Expr *input)
      : Expr(Kind::ImplicitCastExpr, pos, target_type), cast_kind_(cast_kind), input_(input) {}

 private:
  // The kind of cast operation.
  CastKind cast_kind_;
  // The input to the cast.
  Expr *input_;
};

/**
 * Expressions for array or map accesses, e.g., x[i]. The object ('x' in the example) can either be
 * an array or a map. The index ('i' in the example) must evaluate to an integer for array access
 * and the map's associated key type if the object is a map.
 */
class IndexExpr : public Expr {
 public:
  /**
   * @return The object that's being indexed into.
   */
  Expr *GetObject() const { return obj_; }

  /**
   * @return The index to use to access the object.
   */
  Expr *GetIndex() const { return index_; }

  /**
   * @return True if this expression for an array access; false otherwise.
   */
  bool IsArrayAccess() const;

  /**
   * @return True if this expression for a map access; false otherwise.
   */
  bool IsMapAccess() const;

  /**
   * Is the given node an index expression? Needed as part of the custom AST RTTI infrastructure.
   * @param node The node to check.
   * @return True if the node is an index expression; false otherwise.
   */
  static bool classof(const AstNode *node) { return node->GetKind() == Kind::IndexExpr; }

 private:
  friend class AstNodeFactory;

  // Private to force factory usage.
  IndexExpr(const SourcePosition &pos, Expr *obj, Expr *index)
      : Expr(Kind::IndexExpr, pos), obj_(obj), index_(index) {}

 private:
  // The object that's being indexes.
  Expr *obj_;
  // The index.
  Expr *index_;
};

/**
 * A literal in the original source code.
 */
class LiteralExpr : public Expr {
 public:
  /**
   * The kinds of literals that can appear in TPL.
   */
  enum class LiteralKind : uint8_t { Nil, Boolean, Int, Float, String };

  /**
   * @return The kind of literal this expression represents.
   */
  LiteralExpr::LiteralKind GetLiteralKind() const { return lit_kind_; }

  /**
   * @return True if this is a 'nil' literal; false otherwise.
   */
  bool IsNilLiteral() const { return lit_kind_ == LiteralKind::Nil; }

  /**
   * @return True if this is a bool literal ('true' or 'false'); false otherwise.
   */
  bool IsBoolLiteral() const { return lit_kind_ == LiteralKind::Boolean; }

  /**
   * @return True if this is an integer literal ('1', '44', etc.); false otherwise.
   */
  bool IsIntegerLiteral() const { return lit_kind_ == LiteralKind::Int; }

  /**
   * @return True if this is a floating point literal ('1.0', '77.12', etc.); false otherwise.
   */
  bool IsFloatLiteral() const { return lit_kind_ == LiteralKind::Float; }

  /**
   * @return True if this is a string literal ('hello', 'there', etc.); false otherwise.
   */
  bool IsStringLiteral() const { return lit_kind_ == LiteralKind::String; }

  /**
   * @return The boolean literal value. No check to ensure expression is a boolean literal.
   */
  bool BoolVal() const {
    TPL_ASSERT(IsBoolLiteral(), "Literal is not a boolean value literal");
    return bool_val_;
  }

  /**
   * @return The raw string value. No check to ensure expression is a string.
   */
  Identifier StringVal() const {
    TPL_ASSERT(IsStringLiteral(), "Literal is not a string or identifier");
    return string_val_;
  }

  /**
   * @return The integer value. No check to ensure expression is an integer.
   */
  int64_t IntegerVal() const {
    TPL_ASSERT(IsIntegerLiteral(), "Literal is not an integer literal");
    return int_val_;
  }

  /**
   * @return The floating point value. No check to ensure expression is a floating point value.
   */
  double FloatVal() const {
    TPL_ASSERT(IsFloatLiteral(), "Literal is not a floating point literal");
    return float_val_;
  }

  /**
   * @return True if this expression is representable as the given type; false otherwise.
   */
  bool IsRepresentable(Type *type) const;

  /**
   * Is the given node a literal? Needed as part of the custom AST RTTI infrastructure.
   * @param node The node to check.
   * @return True if the node is a literal; false otherwise.
   */
  static bool classof(const AstNode *node) { return node->GetKind() == Kind::LiteralExpr; }

 private:
  friend class AstNodeFactory;

  // Private to force factory usage.
  explicit LiteralExpr(const SourcePosition &pos)
      : Expr(Kind::LiteralExpr, pos), lit_kind_(LiteralKind::Nil) {}

  LiteralExpr(const SourcePosition &pos, bool val)
      : Expr(Kind::LiteralExpr, pos), bool_val_(val), lit_kind_(LiteralKind::Boolean) {}

  LiteralExpr(const SourcePosition &pos, Identifier str)
      : Expr(Kind::LiteralExpr, pos), string_val_(str), lit_kind_(LiteralKind::String) {}

  LiteralExpr(const SourcePosition &pos, int64_t num)
      : Expr(Kind::LiteralExpr, pos), int_val_(num), lit_kind_(LiteralKind::Int) {}

  LiteralExpr(const SourcePosition &pos, double num)
      : Expr(Kind::LiteralExpr, pos), float_val_(num), lit_kind_(LiteralKind::Float) {}

 private:
  // A union of possible literal values.
  union {
    bool bool_val_;
    Identifier string_val_;
    int64_t int_val_;
    double float_val_;
  };
  // The kind of literal.
  LiteralKind lit_kind_;
};

/**
 * Expressions accessing structure members, e.g., x.f
 *
 * TPL uses the same member access syntax for regular struct member access and access through a
 * struct pointer. Thus, the language allows the following:
 *
 * @code
 * struct X {
 *   a: int
 * }
 *
 * var x: X
 * var px: *X
 *
 * x.a = 10
 * px.a = 20
 * @endcode
 *
 * Using dot-access for pointers to object is termed a sugared-arrow access.
 */
class MemberExpr : public Expr {
 public:
  /**
   * @return The object being accessed.
   */
  Expr *GetObject() const { return object_; }

  /**
   * @return The member of the object/struct to access.
   */
  Expr *GetMember() const { return member_; }

  /**
   * @return True if this member access is sugared. Refer to docs to understand arrow sugaring.
   */
  bool IsSugaredArrow() const;

  /**
   * Is the given node a member expression? Needed as part of the custom AST RTTI infrastructure.
   * @param node The node to check.
   * @return True if the node is a member expression; false otherwise.
   */
  static bool classof(const AstNode *node) { return node->GetKind() == Kind::MemberExpr; }

 private:
  friend class AstNodeFactory;

  // Private to force factory usage.
  MemberExpr(const SourcePosition &pos, Expr *obj, Expr *member)
      : Expr(Kind::MemberExpr, pos), object_(obj), member_(member) {}

 private:
  // The object being accessed.
  Expr *object_;
  // The member in the object to access.
  Expr *member_;
};

/**
 * A unary expression with a non-null inner expression and an operator.
 */
class UnaryOpExpr : public Expr {
 public:
  /**
   * @return The parsing token operator representing the unary operation.
   */
  parsing::Token::Type Op() const { return op_; }

  /**
   * @return The input expression to the unary operation.
   */
  Expr *GetInput() const { return expr_; }

  /**
   * Is the given node a unary expression? Needed as part of the custom AST RTTI infrastructure.
   * @param node The node to check.
   * @return True if the node is a unary expression; false otherwise.
   */
  static bool classof(const AstNode *node) { return node->GetKind() == Kind::UnaryOpExpr; }

 private:
  friend class AstNodeFactory;

  // Private to force factory usage.
  UnaryOpExpr(const SourcePosition &pos, parsing::Token::Type op, Expr *expr)
      : Expr(Kind::UnaryOpExpr, pos), op_(op), expr_(expr) {}

 private:
  // The unary operator.
  parsing::Token::Type op_;
  // The input to the unary operation.
  Expr *expr_;
};

// ---------------------------------------------------------
// Type Representation Nodes
// ---------------------------------------------------------

// Type representation nodes. A type representation is a thin representation of
// how the type appears in code. They are structurally the same as their full
// blown Type counterparts, but we use the expressions to defer their type
// resolution.

/**
 * Array type.
 */
class ArrayTypeRepr : public Expr {
 public:
  /**
   * @return The length of the array, if provided; null if not provided.
   */
  Expr *GetLength() const { return len_; }

  /**
   * @return The type of elements the array stores.
   */
  Expr *GetElementType() const { return elem_type_; }

  /**
   * @return True if a length was specified in the array type representation; false otherwise.
   */
  bool HasLength() const { return len_ != nullptr; }

  /**
   * Is the given node an array type? Needed as part of the custom AST RTTI infrastructure.
   * @param node The node to check.
   * @return True if the node is an array type; false otherwise.
   */
  static bool classof(const AstNode *node) { return node->GetKind() == Kind::ArrayTypeRepr; }

 private:
  friend class AstNodeFactory;

  // Private to force factory usage.
  ArrayTypeRepr(const SourcePosition &pos, Expr *len, Expr *elem_type)
      : Expr(Kind::ArrayTypeRepr, pos), len_(len), elem_type_(elem_type) {}

 private:
  // The specified length.
  Expr *len_;
  // The element type of the array.
  Expr *elem_type_;
};

/**
 * Function type.
 */
class FunctionTypeRepr : public Expr {
 public:
  /**
   * @return The parameters to the function.
   */
  const util::RegionVector<FieldDeclaration *> &GetParameters() const { return param_types_; }

  /**
   * @return The return type of the function.
   */
  Expr *GetReturnType() const { return ret_type_; }

  /**
   * Is the given node a function type? Needed as part of the custom AST RTTI infrastructure.
   * @param node The node to check.
   * @return True if the node is a function type; false otherwise.
   */
  static bool classof(const AstNode *node) { return node->GetKind() == Kind::FunctionTypeRepr; }

 private:
  friend class AstNodeFactory;

  // Private to force factory usage.
  FunctionTypeRepr(const SourcePosition &pos, util::RegionVector<FieldDeclaration *> &&param_types,
                   Expr *ret_type)
      : Expr(Kind::FunctionTypeRepr, pos),
        param_types_(std::move(param_types)),
        ret_type_(ret_type) {}

 private:
  // The parameters to the function.
  util::RegionVector<FieldDeclaration *> param_types_;
  // The return type.
  Expr *ret_type_;
};

/**
 * Map type.
 */
class MapTypeRepr : public Expr {
 public:
  /**
   * @return The key type of the map.
   */
  Expr *GetKeyType() const { return key_; }

  /**
   * @return The value type of the map.
   */
  Expr *GetValueType() const { return val_; }

  /**
   * Is the given node a map type? Needed as part of the custom AST RTTI infrastructure.
   * @param node The node to check.
   * @return True if the node is a map type; false otherwise.
   */
  static bool classof(const AstNode *node) { return node->GetKind() == Kind::MapTypeRepr; }

 private:
  friend class AstNodeFactory;

  // Private to force factory usage.
  MapTypeRepr(const SourcePosition &pos, Expr *key, Expr *val)
      : Expr(Kind::MapTypeRepr, pos), key_(key), val_(val) {}

 private:
  // The key type.
  Expr *key_;
  // The value type.
  Expr *val_;
};

/**
 * Pointer type.
 */
class PointerTypeRepr : public Expr {
 public:
  /**
   * @return The pointee type.
   */
  Expr *GetBase() const { return base_; }

  /**
   * Is the given node a pointer type? Needed as part of the custom AST RTTI infrastructure.
   * @param node The node to check.
   * @return True if the node is a pointer type; false otherwise.
   */
  static bool classof(const AstNode *node) { return node->GetKind() == Kind::PointerTypeRepr; }

 private:
  friend class AstNodeFactory;

  // Private to force factory usage.
  PointerTypeRepr(const SourcePosition &pos, Expr *base)
      : Expr(Kind::PointerTypeRepr, pos), base_(base) {}

 private:
  // The type of the element being pointed to.
  Expr *base_;
};

/**
 * Struct type.
 */
class StructTypeRepr : public Expr {
 public:
  /**
   * @return The fields of the struct.
   */
  const util::RegionVector<FieldDeclaration *> &GetFields() const { return fields_; }

  /**
   * @return The field at the provided index. No bounds checking is performed!
   */
  FieldDeclaration *GetFieldAt(uint32_t field_idx) const {
    TPL_ASSERT(field_idx < fields_.size(), "Out-of-bounds field access");
    return fields_[field_idx];
  }

  /**
   * Is the given node a struct type? Needed as part of the custom AST RTTI infrastructure.
   * @param node The node to check.
   * @return True if the node is a struct type; false otherwise.
   */
  static bool classof(const AstNode *node) { return node->GetKind() == Kind::StructTypeRepr; }

 private:
  friend class AstNodeFactory;

  // Private to force factory usage.
  StructTypeRepr(const SourcePosition &pos, util::RegionVector<FieldDeclaration *> &&fields)
      : Expr(Kind::StructTypeRepr, pos), fields_(std::move(fields)) {}

 private:
  // The fields of the struct.
  util::RegionVector<FieldDeclaration *> fields_;
};

}  // namespace ast
}  // namespace tpl
