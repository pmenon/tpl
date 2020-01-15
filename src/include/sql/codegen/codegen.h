#pragma once

#include <array>
#include <initializer_list>
#include <memory>
#include <string_view>
#include <vector>

#include "llvm/ADT/StringMap.h"

#include "ast/ast.h"
#include "ast/ast_node_factory.h"
#include "ast/builtins.h"
#include "ast/identifier.h"
#include "ast/type.h"
#include "common/common.h"
#include "sql/runtime_types.h"
#include "sql/sql.h"
#include "util/region_containers.h"

namespace tpl::sql::codegen {

class CodeContainer;
class FunctionBuilder;

/**
 * Bundles convenience methods needed by other classes during code generation.
 */
class CodeGen {
  // The default number of cached scopes to keep around.
  static constexpr uint32_t kDefaultScopeCacheSize = 4;

  /**
   * Scope object.
   */
  class Scope {
   public:
    // Create scope.
    explicit Scope(Scope *previous) : previous_(nullptr) { Init(previous); }

    // Initialize this scope.
    void Init(Scope *previous) { previous_ = previous; }

    // Get a fresh name in this scope.
    llvm::StringRef GetFreshName(const std::string &name);

    // Return the previous scope.
    Scope *Previous() { return previous_; }

   private:
    // The previous scope_;
    Scope *previous_;
    // Map of name/identifier to next ID.
    llvm::StringMap<uint64_t> names_;
  };

 public:
  /**
   * RAII scope class.
   */
  class CodeScope {
   public:
    /**
     * Create a scope.
     * @param codegen The code generator instance.
     */
    explicit CodeScope(CodeGen *codegen) : codegen_(codegen) { codegen_->EnterScope(); }

    /**
     * Destructor. Exits current scope.
     */
    ~CodeScope() { codegen_->ExitScope(); }

   private:
    CodeGen *codegen_;
  };

  /**
   * Create a code generator that generates code for the provided container.
   * @param context The context used create all expressions.
   */
  explicit CodeGen(ast::Context *context);

  /**
   * Destructor.
   */
  ~CodeGen();

  // ---------------------------------------------------------------------------
  //
  // Constant literals
  //
  // ---------------------------------------------------------------------------

  /**
   * @return A literal whose value is the provided boolean value.
   */
  [[nodiscard]] ast::Expr *ConstBool(bool val) const;

  /**
   * @return A literal whose value is the provided 8-bit signed integer.
   */
  [[nodiscard]] ast::Expr *Const8(int8_t val) const;

  /**
   * @return A literal whose value is the provided 16-bit signed integer.
   */
  [[nodiscard]] ast::Expr *Const16(int16_t val) const;

  /**
   * @return A literal whose value is the provided 32-bit signed integer.
   */
  [[nodiscard]] ast::Expr *Const32(int32_t val) const;

  /**
   * @return A literal whose value is the provided 64-bit signed integer.
   */
  [[nodiscard]] ast::Expr *Const64(int64_t val) const;

  /**
   * @return A literal whose value is the provided 64-bit floating point.
   */
  [[nodiscard]] ast::Expr *ConstDouble(double val) const;

  /**
   * @return A literal whose value is identical to the provided string.
   */
  [[nodiscard]] ast::Expr *ConstString(std::string_view str) const;

  // ---------------------------------------------------------------------------
  //
  // Type representations (not full TPL types !!)
  //
  // ---------------------------------------------------------------------------

  /**
   * @return The type representation for an 8-bit signed integer (i.e., int8)
   */
  [[nodiscard]] ast::Expr *Int8Type() const;

  /**
   * @return The type representation for an 16-bit signed integer (i.e., int16)
   */
  [[nodiscard]] ast::Expr *Int16Type() const;

  /**
   * @return The type representation for an 32-bit signed integer (i.e., int32)
   */
  [[nodiscard]] ast::Expr *Int32Type() const;

  /**
   * @return The type representation for an 64-bit signed integer (i.e., int64)
   */
  [[nodiscard]] ast::Expr *Int64Type() const;

  /**
   * @return The type representation for an 32-bit floating point number (i.e., float32)
   */
  [[nodiscard]] ast::Expr *Float32Type() const;

  /**
   * @return The type representation for an 64-bit floating point number (i.e., float64)
   */
  [[nodiscard]] ast::Expr *Float64Type() const;

  /**
   * @return The type representation for the provided builtin type.
   */
  [[nodiscard]] ast::Expr *BuiltinType(ast::BuiltinType::Kind builtin_kind) const;

  /**
   * @return The type representation for a TPL nil type.
   */
  [[nodiscard]] ast::Expr *Nil() const;

  /**
   * @return A type representation expression that is a pointer to the provided type.
   */
  [[nodiscard]] ast::Expr *PointerType(ast::Expr *base_type_repr) const;

  /**
   * @return A type representation expression that is a pointer to a named object.
   */
  [[nodiscard]] ast::Expr *PointerType(ast::Identifier type_name) const;

  /**
   * @return A type representation expression that is a pointer to the provided builtin type.
   */
  [[nodiscard]] ast::Expr *PointerType(ast::BuiltinType::Kind builtin) const;

  /**
   * Convert a SQL type into a type representation expression.
   * @param type The SQL type.
   * @return The corresponding TPL type.
   */
  [[nodiscard]] ast::Expr *TplType(sql::TypeId type);

  /**
   * @return An expression that represents the address of the provided object.
   */
  [[nodiscard]] ast::Expr *AddressOf(ast::Expr *obj) const;

  /**
   * @return An expression that represents the size of a type with the provided name, in bytes.
   */
  [[nodiscard]] ast::Expr *SizeOf(ast::Identifier type_name) const;

  /**
   * @return The offset of the given member within the given object.
   */
  [[nodiscard]] ast::Expr *OffsetOf(ast::Identifier obj, ast::Identifier member) const;

  // ---------------------------------------------------------------------------
  //
  // Declarations
  //
  // ---------------------------------------------------------------------------

  /**
   * Declare a variable with the provided name and type representation with no initial value.
   *
   * @param name The name of the variable to declare.
   * @param type_repr The provided type representation.
   * @return The variable declaration.
   */
  [[nodiscard]] ast::VariableDecl *DeclareVarNoInit(ast::Identifier name, ast::Expr *type_repr);

  /**
   * Declare a variable with the provided name and builtin kind, but with no initial value.
   *
   * @param name The name of the variable.
   * @param kind The builtin kind of the declared variable.
   * @return The variable declaration.
   */
  [[nodiscard]] ast::VariableDecl *DeclareVarNoInit(ast::Identifier name,
                                                    ast::BuiltinType::Kind kind);

  /**
   * Declare a variable with the provided name and initial value. The variable's type will be
   * inferred from its initial value.
   *
   * @param name The name of the variable to declare.
   * @param init The initial value to assign the variable.
   * @return The variable declaration.
   */
  [[nodiscard]] ast::VariableDecl *DeclareVarWithInit(ast::Identifier name, ast::Expr *init);

  /**
   * Declare a variable with the provided name, type representation, and initial value.
   *
   * Note: No check is performed to ensure the provided type representation matches the type of the
   *       provided initial expression here. That check will be done during semantic analysis. Thus,
   *       it's possible for users to pass wrong information here and for the call to return without
   *       error.
   *
   * @param name The name of the variable to declare.
   * @param type_repr The provided type representation of the variable.
   * @param init The initial value to assign the variable.
   * @return The variable declaration.
   */
  [[nodiscard]] ast::VariableDecl *DeclareVar(ast::Identifier name, ast::Expr *type_repr,
                                              ast::Expr *init);

  /**
   * Declare a struct with the provided name and struct field elements.
   *
   * @param name The name of the structure.
   * @param fields The fields constituting the struct.
   * @return The structure declaration.
   */
  [[nodiscard]] ast::StructDecl *DeclareStruct(ast::Identifier name,
                                               util::RegionVector<ast::FieldDecl *> &&fields) const;

  // ---------------------------------------------------------------------------
  //
  // Assignments
  //
  // ---------------------------------------------------------------------------

  /**
   * Generate an assignment of the client-provide value to the provided destination.
   *
   * @param dest Where the value is stored.
   * @param value The value to assign.
   * @return The assignment statement.
   */
  [[nodiscard]] ast::Stmt *Assign(ast::Expr *dest, ast::Expr *value);

  // ---------------------------------------------------------------------------
  //
  // Binary and comparison operations
  //
  // ---------------------------------------------------------------------------

  /**
   * Generate a binary operation of the provided operation type (<b>op</b>) between the
   * provided left and right operands, returning its result.
   * @param op The binary operation.
   * @param left The left input.
   * @param right The right input.
   * @return The result of the binary operation.
   */
  [[nodiscard]] ast::Expr *BinaryOp(parsing::Token::Type op, ast::Expr *left,
                                    ast::Expr *right) const;

  /**
   * Generate a unary operation of the provided operation type (<b>op</b>) on the provided input.
   * @param op The unary operation.
   * @param input The input.
   * @return The result of the unary operation.
   */
  [[nodiscard]] ast::Expr *UnaryOp(parsing::Token::Type op, ast::Expr *input) const;

  // ---------------------------------------------------------------------------
  //
  // Struct/Array access
  //
  // ---------------------------------------------------------------------------

  /**
   * Generate an access to a member within an object/struct.
   * @param object The object to index into.
   * @param member The name of the struct member to access.
   * @return An expression accessing the desired struct member.
   */
  [[nodiscard]] ast::Expr *AccessStructMember(ast::Expr *object, ast::Identifier member);

  // ---------------------------------------------------------------------------
  //
  // Generic function calls and all builtins function calls.
  //
  // ---------------------------------------------------------------------------

  /**
   * Generate a call to the provided function by name and with the provided arguments.
   * @param func_name The name of the function to call.
   * @param args The arguments to pass in the call.
   */
  [[nodiscard]] ast::Expr *Call(ast::Identifier func_name,
                                std::initializer_list<ast::Expr *> args) const;

  /**
   * Generate a call to the provided builtin function and arguments.
   * @param builtin The builtin to call.
   * @param args The arguments to pass in the call.
   * @return The expression representing the call.
   */
  [[nodiscard]] ast::Expr *CallBuiltin(ast::Builtin builtin,
                                       std::initializer_list<ast::Expr *> args) const;

  // ---------------------------------------------------------------------------
  //
  // Actual TPL builtins
  //
  // ---------------------------------------------------------------------------

  /**
   * Call @boolToSql(). Convert a boolean into a SQL boolean.
   * @param b The constant bool.
   * @return The SQL bool.
   */
  [[nodiscard]] ast::Expr *BoolToSql(bool b) const;

  /**
   * Call @intToSql(). Convert a 64-bit integer into a SQL integer.
   * @param num The number to convert.
   * @return The SQL integer.
   */
  [[nodiscard]] ast::Expr *IntToSql(int64_t num) const;

  /**
   * Call @floatToSql(). Convert a 64-bit floating point number into a SQL real.
   * @param num The number to convert.
   * @return The SQL real.
   */
  [[nodiscard]] ast::Expr *FloatToSql(int64_t num) const;

  /**
   * Call @dateToSql(). Convert a date into a SQL date.
   * @param date The date.
   * @return The SQL date.
   */
  [[nodiscard]] ast::Expr *DateToSql(Date date) const;

  /**
   * Call @dateToSql(). Convert a date into a SQL date.
   * @param year The number to convert.
   * @param month The number to convert.
   * @param day The number to convert.
   * @return The SQL date.
   */
  [[nodiscard]] ast::Expr *DateToSql(uint32_t year, uint32_t month, uint32_t day) const;

  /**
   * Call @stringToSql(). Convert a string literal into a SQL string.
   * @param str The string.
   * @return The SQL varlen.
   */
  [[nodiscard]] ast::Expr *StringToSql(std::string_view str) const;

  /**
   * Call @tableIterInit(). Initializes a TableVectorIterator instance with a table ID.
   * @param table_iter The table iterator variable.
   * @param table_name The name of the table to scan.
   * @return The call expression.
   */
  [[nodiscard]] ast::Expr *TableIterInit(ast::Expr *table_iter, std::string_view table_name) const;

  /**
   * Call @tableIterAdvance(). Attempt to advance the iterator, returning true if successful and
   * false otherwise.
   * @param table_iter The table vector iterator.
   * @return The call expression.
   */
  [[nodiscard]] ast::Expr *TableIterAdvance(ast::Expr *table_iter) const;

  /**
   * Call @tableIterGetVPI(). Retrieve the vector projection iterator from a table vector iterator.
   * @param table_iter The table vector iterator.
   * @return The call expression.
   */
  [[nodiscard]] ast::Expr *TableIterGetVPI(ast::Expr *table_iter) const;

  /**
   * Call @iterateTableParallel(). Performs a parallel scan over the table with the provided name,
   * using the provided query state and thread-state container and calling the provided scan
   * function.
   * @param table_name The name of the table to scan.
   * @param query_state The query state pointer.
   * @param tls The thread state container.
   * @param worker_name The work function name.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *IterateTableParallel(std::string_view table_name, ast::Expr *query_state,
                                                ast::Expr *tls, ast::Identifier worker_name) const;

  /**
   * Call @execCtxGetMemPool(). Return the memory pool within an execution context.
   * @param exec_ctx The execution context variable.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *ExecCtxGetMemoryPool(ast::Expr *exec_ctx) const;

  /**
   * Call @execCtxGetTLS(). Return the thread state container within an execution context.
   * @param exec_ctx The name of the execution context variable.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *ExecCtxGetTLS(ast::Expr *exec_ctx) const;

  /**
   * Call @tlsReset(). Reset the thread state container to a new state type with its own
   * initialization and tear-down functions.
   * @param tls The name of the thread state container variable.
   * @param tls_state_name The name of the thread state struct type.
   * @param init_fn The name of the initialization function.
   * @param tear_down_fn The name of the tear-down function.
   * @param context A context to pass along to each of the init and tear-down functions.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *TLSReset(ast::Expr *tls, ast::Identifier tls_state_name,
                                    ast::Identifier init_fn, ast::Identifier tear_down_fn,
                                    ast::Expr *context) const;

  /**
   * Call @sorterInit(). Initialize the provided sorter instance using a memory pool, comparison
   * function and the struct that will be materialized into the sorter instance.
   * @param sorter The sorter instance.
   * @param mem_pool The memory pool instance.
   * @param cmp_func_name The name of the comparison function to use.
   * @param sort_row_type_name The name of the materialized sort-row type.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *SorterInit(ast::Expr *sorter, ast::Expr *mem_pool,
                                      ast::Identifier cmp_func_name,
                                      ast::Identifier sort_row_type_name) const;

  /**
   * Call @sorterSort().  Sort the provided sorter instance.
   * @param sorter The sorter instance.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *SorterSort(ast::Expr *sorter) const;

  /**
   * Call @sorterSortParallel(). Perform a parallel sort of all sorter instances contained in the
   * provided thread-state  container at the given offset, merging sorter results into a central
   * sorter instance.
   * @param sorter The central sorter instance that will contain the results of the sort.
   * @param tls The thread state container.
   * @param offset The offset within the container where the thread-local sorter is.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *SortParallel(ast::Expr *sorter, ast::Expr *tls, ast::Expr *offset) const;

  /**
   * Call @sorterSortTopKParallel(). Perform a parallel top-k sort of all sorter instances contained
   * in the provided thread-local container at the given offset.
   * @param sorter The central sorter instance that will contain the results of the sort.
   * @param tls The thread-state container.
   * @param offset The offset within the container where the thread-local sorters are.
   * @param top_k The top-K value.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *SortTopKParallel(ast::Expr *sorter, ast::Expr *tls, ast::Expr *offset,
                                            std::size_t top_k) const;

  /**
   * Call @sorterFree(). Destroy the provided sorter instance.
   * @param sorter The sorter instance.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *SorterFree(ast::Expr *sorter) const;

  /**
   * Call @like(). Implements the SQL LIKE() operation.
   * @param str The input string.
   * @param pattern The input pattern.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *Like(ast::Expr *str, ast::Expr *pattern) const;

  /**
   * Invoke !@like(). Implements the SQL NOT LIKE() operation.
   * @param str The input string.
   * @param pattern The input pattern.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *NotLike(ast::Expr *str, ast::Expr *pattern) const;

  // ---------------------------------------------------------------------------
  //
  // Identifiers
  //
  // ---------------------------------------------------------------------------

  /**
   * @return A new unique identifier using the given string as a prefix.
   */
  ast::Identifier MakeFreshIdentifier(const std::string &str);

  /**
   * @return An identifier whose contents are identical to the input string.
   */
  ast::Identifier MakeIdentifier(std::string_view str) const;

  /**
   * @return A new identifier expression representing the given identifier.
   */
  ast::IdentifierExpr *MakeExpr(ast::Identifier ident) const;

  /**
   * @return An empty list of statements.
   */
  ast::BlockStmt *MakeEmptyBlock() const;

  /**
   * @return An empty list of fields.
   */
  util::RegionVector<ast::FieldDecl *> MakeEmptyFieldList() const;

  /**
   * @return A field list with the given fields.
   */
  util::RegionVector<ast::FieldDecl *> MakeFieldList(
      std::initializer_list<ast::FieldDecl *> fields) const;

  /**
   * Create a single field.
   * @param name The name of the field.
   * @param type The type representation of the field.
   * @return The field declaration.
   */
  ast::FieldDecl *MakeField(ast::Identifier name, ast::Expr *type) const;

  /**
   * @return The current function being built.
   */
  FunctionBuilder *CurrentFunction() const { return curr_function_; }

  /**
   * @return The current source code position.
   */
  const SourcePosition &GetPosition() const { return position_; }

  /**
   * Move to a new line.
   */
  void NewLine() { position_.line++; }

  /**
   * Increase current indentation level.
   */
  void Indent() { position_.column += 4; }

  /**
   * Decrease Remove current indentation level.
   */
  void UnIndent() { position_.column -= 4; }

 private:
  friend class CodeScope;
  friend class If;
  friend class FunctionBuilder;
  friend class Loop;

  // Enter a new lexical scope.
  void EnterScope();

  // Exit the current lexical scope.
  void ExitScope();

  // Return the AST node factory.
  ast::AstNodeFactory *GetFactory();

  // Build a call expression to a function with the provided name using the provided arguments.
  ast::Expr *BuildCall(ast::Identifier func_name, std::initializer_list<ast::Expr *> args) const;

 private:
  // The context used to create AST nodes.
  ast::Context *context_;
  // The current position in the source.
  SourcePosition position_;
  // The current function we're generating.
  FunctionBuilder *curr_function_;
  // Unique ID generation.
  // TODO(pmenon) Fix me.
  uint64_t id_counter_;
  // Cache of code scopes.
  uint32_t num_cached_scopes_;
  std::array<std::unique_ptr<Scope>, kDefaultScopeCacheSize> scope_cache_ = {nullptr};
  // Current scope.
  Scope *scope_;
};

}  // namespace tpl::sql::codegen
