#pragma once

#include <array>
#include <initializer_list>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "llvm/ADT/StringMap.h"

#include "ast/ast.h"
#include "ast/builtins.h"
#include "ast/identifier.h"
#include "ast/type.h"
#include "ast/type_builder.h"
#include "common/common.h"
#include "parsing/token.h"
#include "sql/codegen/compilation_unit.h"
#include "sql/planner/expressions/expression_defs.h"
#include "sql/sql.h"

namespace tpl::sql::codegen {

class FunctionBuilder;

/**
 * Bundles convenience methods needed by other classes during code generation.
 */
class CodeGen {
  friend class If;
  friend class FunctionBuilder;
  friend class Loop;

 public:
  /**
   * Create a code generator that generates code for the provided container.
   * @param container Where all code is generated into.
   */
  explicit CodeGen(CompilationUnit *container);

  // ---------------------------------------------------------------------------
  //
  // Types.
  //
  // ---------------------------------------------------------------------------

  /**
   * Return the TPL type equivalent of the given C++ template type. Note that this function relies
   * on C++ proxies for any complex types.
   * @tparam T The C++ (proxy) type to create the TPL equivalent type of.
   * @return The TPL type.
   */
  template <typename T>
  [[nodiscard]] ast::Type *GetType() const {
    return ast::TypeBuilder<T>::Get(Context());
  }

  /**
   * @return A TPL bool type.
   */
  [[nodiscard]] ast::Type *BoolType() const { return GetType<bool>(); }

  /**
   * @return A TPL nil type.
   */
  [[nodiscard]] ast::Type *NilType() const { return GetType<void>(); }

  /**
   * @return The TPL type of the SQL type with type id @em type_id.
   */
  [[nodiscard]] ast::Type *GetTPLType(TypeId type_id);

  /**
   * @return The primitive TPL type underlying the SQL type with type @em type_id.
   */
  [[nodiscard]] ast::Type *GetPrimitiveTPLType(TypeId type_id);

  /**
   * @return The TPL aggregate type for the given aggregation and return value.
   */
  [[nodiscard]] ast::Type *AggregateType(planner::AggregateKind agg_kind, TypeId ret_type) const;

  /**
   * @return The TPL type of an array of size @em size and element type @em elem_type.
   */
  [[nodiscard]] ast::ArrayType *ArrayType(uint32_t size, ast::Type *elem_type);

  // ---------------------------------------------------------------------------
  //
  // Constant literals.
  //
  // ---------------------------------------------------------------------------

 private:
  // Return a constant boolean expression.
  [[nodiscard]] ast::Expression *ConstBool(bool v);

  // Return a constant integer expression.
  [[nodiscard]] ast::Expression *ConstInt(int64_t v);

  // Return a constant FP expression.
  [[nodiscard]] ast::Expression *ConstFloat(double v);

  // Return a constant string expression.
  [[nodiscard]] ast::Expression *ConstString(std::string_view v);

  template <typename T, typename Enable = void>
  struct LiteralHelper;

  // This is dumb, I can't specialize for bool specifically ...
  template <typename T>
  struct LiteralHelper<T, std::enable_if_t<std::is_same_v<bool, T>>> {
    static ast::Expression *Make(CodeGen *codegen, bool val) {
      auto result = codegen->ConstBool(val);
      result->SetType(codegen->BoolType());
      return result;
    }
  };

  // Specialization for integers.
  template <typename T>
  struct LiteralHelper<T, std::enable_if_t<std::is_integral_v<T> && !std::is_same_v<bool, T>>> {
    static ast::Expression *Make(CodeGen *codegen, T val) {
      auto result = codegen->ConstInt(val);
      result->SetType(codegen->template GetType<T>());
      return result;
    }
  };

  // Specialization for floating-point numbers.
  template <typename T>
  struct LiteralHelper<T, std::enable_if_t<std::is_floating_point_v<T>>> {
    static ast::Expression *Make(CodeGen *codegen, T val) {
      auto result = codegen->ConstFloat(val);
      result->SetType(codegen->template GetType<T>());
      return result;
    }
  };

  // Specialization for strings.
  template <typename T>
  struct LiteralHelper<T, std::enable_if_t<std::is_convertible_v<T, std::string_view>>> {
    static ast::Expression *Make(CodeGen *codegen, T val) {
      auto result = codegen->ConstString(val);
      result->SetType(codegen->template GetType<T>());
      return result;
    }
  };

 public:
  /**
   * Generate an expression representing the literal value @em val. The value can be either a C++
   * boolean, unsigned or signed integer, single- or double-precision floating pointer value, or a
   * string literal.
   * @tparam T The C++ or proxy type of the value.
   * @param val The literal value.
   * @return An expression representing the constant value.
   */
  template <typename T>
  [[nodiscard]] ast::Expression *Literal(T val) {
    return LiteralHelper<T>::Make(this, val);
  }

  /**
   * @return The type representation for a TPL nil type.
   */
  [[nodiscard]] ast::Expression *Nil() const;

  // ---------------------------------------------------------------------------
  //
  // Declarations
  //
  // ---------------------------------------------------------------------------

  /**
   * Declare an uninitialized variable with the given name and resolved type.
   * @param name The name of the variable to declare.
   * @param type The type of the variable.
   * @return The declaration.
   */
  [[nodiscard]] ast::Statement *DeclareVar(ast::Identifier name, ast::Type *type);

  /**
   * Declare a variable with the given name @em name and initial value @em val. The type of the
   * variable will take on the type of the initial value.
   * @pre The initial value must have a resolved type.
   * @param name The name of the variable.
   * @param val The initial value to assign the variable.
   * @return The declaration.
   */
  [[nodiscard]] ast::Statement *DeclareVarWithInit(ast::Identifier name, ast::Expression *val);

  /**
   * Declare the given struct with the given name.
   * @param name The name of the structure.
   * @param members The members of the structure.
   * @return The construct structure declare in the current container.
   */
  [[nodiscard]] ast::StructType *DeclareStruct(ast::Identifier name,
                                               const std::vector<ast::Field> &members) const;

  // ---------------------------------------------------------------------------
  //
  // Common statements and expressions
  //
  // ---------------------------------------------------------------------------

  /**
   * @return An expression that represents the address of the provided object, i.e., "&obj".
   */
  [[nodiscard]] ast::Expression *AddressOf(ast::Expression *obj) const;

  /**
   * @return An expression representing the dereferencing of the given pointer, i.e., "*ptr". We
   *         assert that the input pointer actually is a pointer type.
   */
  [[nodiscard]] ast::Expression *Deref(ast::Expression *ptr) const;

  /**
   * Generate an assignment of the client-provide value to the provided destination.
   *
   * @param dest Where the value is stored.
   * @param value The value to assign.
   * @return The assignment statement.
   */
  [[nodiscard]] ast::Statement *Assign(ast::Expression *dest, ast::Expression *value);

  /**
   * @return An expression representing "arr[idx]".
   */
  [[nodiscard]] ast::Expression *ArrayAccess(ast::Expression *arr, uint64_t idx);

  /**
   * @return An expression representing "arr[idx]".
   */
  [[nodiscard]] ast::Expression *ArrayAccess(ast::Expression *arr, ast::Expression *idx);

  /**
   * @return An expression representing a struct member access, i.e., "obj.member" where @em obj can
   *         be either a struct reference or a pointer to a struct.
   */
  [[nodiscard]] ast::Expression *StructMember(ast::Expression *obj, std::string_view member);

  /**
   * @pre Both the input and target types must be resolved pointer types.
   * @return An expression representing a pointer cast of @em input to type @em type.
   */
  [[nodiscard]] ast::Expression *PtrCast(ast::Type *type, ast::Expression *input) const;

  /**
   * Create a return statement without a return value.
   * @return The statement.
   */
  [[nodiscard]] ast::Statement *Return();

  /**
   * Create a return statement that returns the given value.
   * @param ret The return value.
   * @return The statement.
   */
  [[nodiscard]] ast::Statement *Return(ast::Expression *ret);

  // ---------------------------------------------------------------------------
  //
  // Common expressions.
  //
  // ---------------------------------------------------------------------------

  /**
   * Binary boolean-logic operation.
   * T op T -> bool (T must be a primitive boolean).
   * @param op The type of operation.
   * @param lhs The left input to the operation.
   * @param rhs The right input to the operation.
   * @return The result of the operation.
   */
  [[nodiscard]] ast::Expression *BooleanOp(parsing::Token::Type op, ast::Expression *lhs,
                                           ast::Expression *rhs);

  /**
   * Build binary arithmetic operation.
   * T op T -> T (T must be a primitive or SQL type.)
   * @param op The operation.
   * @param lhs The left input to the operation.
   * @param rhs The right input to the operation.
   * @return The result of the operation.
   */
  [[nodiscard]] ast::Expression *ArithmeticOp(parsing::Token::Type op, ast::Expression *lhs,
                                              ast::Expression *rhs);

  /**
   * Build binary comparison operation.
   * T op T -> bool (T must be a primitive or SQL type.)
   * @param op The operation.
   * @param lhs The left input to the operation.
   * @param rhs The right input to the operation.
   * @return The boolean result of the operation.
   */
  [[nodiscard]] ast::Expression *ComparisonOp(parsing::Token::Type op, ast::Expression *lhs,
                                              ast::Expression *rhs);

  /**
   * Arithmetic negation.
   * @pre The input must be arithmetic and support negation.
   * @param input The input.
   * @return The arithmetic negation of the input, i.e., -input
   */
  [[nodiscard]] ast::Expression *Neg(ast::Expression *input);

  /**
   * Arithmetic addition.
   * @pre Both inputs must have the same types and support addition.
   * @param lhs The left input.
   * @param rhs The right input.
   * @return The result of lhs + rhs.
   */
  [[nodiscard]] ast::Expression *Add(ast::Expression *lhs, ast::Expression *rhs);

  /**
   * Arithmetic subtraction.
   * @pre Both inputs must have the same types and support subtraction.
   * @param lhs The left input.
   * @param rhs The right input.
   * @return The result of lhs - rhs.
   */
  [[nodiscard]] ast::Expression *Sub(ast::Expression *lhs, ast::Expression *rhs);

  /**
   * Arithmetic multiplication.
   * @pre Both inputs must have the same types and support multiplication.
   * @param lhs The left input.
   * @param rhs The right input.
   * @return The result of lhs * rhs.
   */
  [[nodiscard]] ast::Expression *Mul(ast::Expression *lhs, ast::Expression *rhs);

  /**
   * Arithmetic division.
   * @pre Both inputs must have the same types and support division.
   * @param lhs The left input.
   * @param rhs The right input.
   * @return The result of lhs / rhs.
   */
  [[nodiscard]] ast::Expression *Div(ast::Expression *lhs, ast::Expression *rhs);

  /**
   * Arithmetic modulus.
   * @pre Both inputs must have the same types and support modulus.
   * @param lhs The left input.
   * @param rhs The right input.
   * @return The result of lhs % rhs.
   */
  [[nodiscard]] ast::Expression *Mod(ast::Expression *lhs, ast::Expression *rhs);

  /**
   * Comparison.
   * @pre Both inputs must have the same type and support comparison.
   * @param lhs The left input.
   * @param rhs The right input.
   * @return The result of lhs == rhs.
   */
  [[nodiscard]] ast::Expression *CompareEq(ast::Expression *lhs, ast::Expression *rhs);

  /**
   * Comparison.
   * @pre Both inputs must have the same type and support comparison.
   * @param lhs The left input.
   * @param rhs The right input.
   * @return The result of lhs != rhs.
   */
  [[nodiscard]] ast::Expression *CompareNe(ast::Expression *lhs, ast::Expression *rhs);

  /**
   * Comparison.
   * @pre Both inputs must have the same type and support comparison.
   * @param lhs The left input.
   * @param rhs The right input.
   * @return The result of lhs < rhs.
   */
  [[nodiscard]] ast::Expression *CompareLt(ast::Expression *lhs, ast::Expression *rhs);

  /**
   * Comparison.
   * @pre Both inputs must have the same type and support comparison.
   * @param lhs The left input.
   * @param rhs The right input.
   * @return The result of lhs <= rhs.
   */
  [[nodiscard]] ast::Expression *CompareLe(ast::Expression *lhs, ast::Expression *rhs);

  /**
   * Comparison.
   * @pre Both inputs must have the same type and support comparison.
   * @param lhs The left input.
   * @param rhs The right input.
   * @return The result of lhs > rhs.
   */
  [[nodiscard]] ast::Expression *CompareGt(ast::Expression *lhs, ast::Expression *rhs);

  /**
   * Comparison.
   * @pre Both inputs must have the same type and support comparison.
   * @param lhs The left input.
   * @param rhs The right input.
   * @return The result of lhs >= rhs.
   */
  [[nodiscard]] ast::Expression *CompareGe(ast::Expression *lhs, ast::Expression *rhs);

  /**
   * Boolean logic.
   * @pre Both inputs must be primitive boolean types.
   * @param lhs The left input.
   * @param rhs The right input.
   * @return The result of lhs && rhs.
   */
  [[nodiscard]] ast::Expression *LogicalAnd(ast::Expression *lhs, ast::Expression *rhs);

  /**
   * Boolean logic.
   * @pre Both inputs must be primitive boolean types.
   * @param lhs The left input.
   * @param rhs The right input.
   * @return The result of lhs || rhs.
   */
  [[nodiscard]] ast::Expression *LogicalOr(ast::Expression *lhs, ast::Expression *rhs);

  /**
   * Boolean logic.
   * @pre Both inputs must be primitive boolean types.
   * @param input The left input.
   * @return The result of !input.
   */
  [[nodiscard]] ast::Expression *LogicalNot(ast::Expression *input);

  // ---------------------------------------------------------------------------
  //
  // Regular and builtin function calls
  //
  // ---------------------------------------------------------------------------

  /**
   * Generate a call to the provided function by name and with the provided arguments.
   * @param func_name The name of the function to call.
   * @param args The arguments to pass in the call.
   */
  [[nodiscard]] ast::Expression *Call(ast::Identifier func_name,
                                      std::initializer_list<ast::Expression *> args) const;

  /**
   * Generate a call to the provided function by name and with the provided arguments.
   * @param func_name The name of the function to call.
   * @param args The arguments to pass in the call.
   */
  [[nodiscard]] ast::Expression *Call(ast::Identifier func_name,
                                      const std::vector<ast::Expression *> &args) const;

  /**
   * Generate a call to the provided builtin function and arguments.
   * @param builtin The builtin to call.
   * @param args The arguments to pass in the call.
   * @return The expression representing the call.
   */
  [[nodiscard]] ast::Expression *CallBuiltin(ast::Builtin builtin,
                                             std::initializer_list<ast::Expression *> args) const;

  /**
   * Generate a call to the provided function using the provided arguments. This function is almost
   * identical to the previous with the exception of the type of the arguments parameter. It's
   * an alternative API for callers that manually build up their arguments list.
   * @param builtin The builtin to call.
   * @param args The arguments to pass in the call.
   * @return The expression representing the call.
   */
  [[nodiscard]] ast::Expression *CallBuiltin(ast::Builtin builtin,
                                             const std::vector<ast::Expression *> &args) const;

  /**
   * @return A new unique identifier using the given string as a prefix.
   */
  ast::Identifier MakeFreshIdentifier(std::string_view str);

  /**
   * @return An identifier whose contents are identical to the input string.
   */
  ast::Identifier MakeIdentifier(std::string_view str) const;

  /**
   * @return A new identifier expression representing the given identifier.
   */
  [[nodiscard]] ast::Expression *MakeExpr(ast::Identifier ident) const;

  /**
   * @return A new identifier expression with the given name and resolved type.
   */
  [[nodiscard]] ast::Expression *MakeExpr(ast::Identifier name, ast::Type *type);

  /**
   * @return The expression as a standalone statement.
   */
  [[nodiscard]] ast::Statement *MakeStatement(ast::Expression *expression) const;

  /**
   * @return An empty list of statements.
   */
  [[nodiscard]] ast::BlockStatement *MakeEmptyBlock() const;

  /**
   * @return The current (potentially null) function being built.
   */
  FunctionBuilder *GetCurrentFunction() const { return function_; }

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

  [[nodiscard]] ast::Expression *BuildTypeRepresentation(ast::Type *type, bool for_struct) const;

 private:
  // Return the context.
  ast::Context *Context() const { return container_->Context(); }

  // Return the AST node factory.
  ast::AstNodeFactory *NodeFactory() const;

 private:
  // The container for the current compilation.
  CompilationUnit *container_;
  // The current position in the source.
  SourcePosition position_;
  // Name cache.
  llvm::StringMap<uint64_t> names_;
  // The current function being generated.
  FunctionBuilder *function_;
};

}  // namespace tpl::sql::codegen
