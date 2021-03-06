#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "ast/ast_visitor.h"
#include "ast/builtins.h"
#include "vm/bytecode_emitter.h"

namespace tpl::ast {
class Context;
class FunctionType;
class Type;
}  // namespace tpl::ast

namespace tpl::vm {

class BytecodeModule;
class LoopBuilder;

/**
 * BytecodeGenerator is responsible for converting a parsed TPL AST into TPL bytecode (TBC). It is
 * assumed that the input TPL program has been type-checked. Once compiled into a TBC unit, all
 * defined functions are fully executable.
 *
 * BytecodeGenerator exposes a single public static function BytecodeGenerator::Compile() that
 * performs the heavy lifting and orchestration involved in the compilation process.
 */
class BytecodeGenerator final : public ast::AstVisitor<BytecodeGenerator> {
 public:
  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(BytecodeGenerator);

  /**
   * Main entry point to convert a valid (i.e., parsed and type-checked) AST into a bytecode module.
   * @param root The root of the AST.
   * @param name The (optional) name of the program.
   * @return A compiled bytecode module.
   */
  static std::unique_ptr<BytecodeModule> Compile(ast::AstNode *root, const std::string &name);

  // Declare all node visit methods here
#define DECLARE_VISIT_METHOD(type) void Visit##type(ast::type *node);
  AST_NODES(DECLARE_VISIT_METHOD)
#undef DECLARE_VISIT_METHOD

  /**
   * @return The emitter used by this generator to write bytecode.
   */
  BytecodeEmitter *GetEmitter() { return &emitter_; }

 private:
  // Private constructor to force users to call Compile()
  BytecodeGenerator() noexcept;

  class ExpressionResultScope;
  class LValueResultScope;
  class RValueResultScope;
  class BytecodePositionScope;

  // Allocate a new function ID
  FunctionInfo *AllocateFunc(const std::string &func_name, ast::FunctionType *func_type);

  // Dispatched from VisitBuiltinCallExpr() to handle the various builtin
  // functions, including filtering, hash table interaction, sorting etc.
  void VisitSqlConversionCall(ast::CallExpression *call, ast::Builtin builtin);
  void VisitNullValueCall(ast::CallExpression *call, ast::Builtin builtin);
  void VisitLNotCall(ast::CallExpression *call);
  void VisitSqlStringLikeCall(ast::CallExpression *call);
  void VisitBuiltinDateFunctionCall(ast::CallExpression *call, ast::Builtin builtin);
  void VisitBuiltinConcatCall(ast::CallExpression *call);
  void VisitBuiltinTableIterCall(ast::CallExpression *call, ast::Builtin builtin);
  void VisitBuiltinTableIterParallelCall(ast::CallExpression *call);
  void VisitBuiltinVPICall(ast::CallExpression *call, ast::Builtin builtin);
  void VisitBuiltinCompactStorageCall(ast::CallExpression *call, ast::Builtin builtin);
  void VisitBuiltinHashCall(ast::CallExpression *call);
  void VisitBuiltinFilterManagerCall(ast::CallExpression *call, ast::Builtin builtin);
  void VisitBuiltinVectorFilterCall(ast::CallExpression *call, ast::Builtin builtin);
  void VisitBuiltinAggHashTableCall(ast::CallExpression *call, ast::Builtin builtin);
  void VisitBuiltinAggHashTableIterCall(ast::CallExpression *call, ast::Builtin builtin);
  void VisitBuiltinAggPartIterCall(ast::CallExpression *call, ast::Builtin builtin);
  void VisitBuiltinAggregatorCall(ast::CallExpression *call, ast::Builtin builtin);
  void VisitBuiltinJoinHashTableCall(ast::CallExpression *call, ast::Builtin builtin);
  void VisitBuiltinHashTableEntryCall(ast::CallExpression *call, ast::Builtin builtin);
  void VisitBuiltinAnalysisStatsCall(ast::CallExpression *call, ast::Builtin builtin);
  void VisitBuiltinSorterCall(ast::CallExpression *call, ast::Builtin builtin);
  void VisitBuiltinSorterIterCall(ast::CallExpression *call, ast::Builtin builtin);
  void VisitResultBufferCall(ast::CallExpression *call, ast::Builtin builtin);
  void VisitCSVReaderCall(ast::CallExpression *call, ast::Builtin builtin);
  void VisitExecutionContextCall(ast::CallExpression *call, ast::Builtin builtin);
  void VisitBuiltinThreadStateContainerCall(ast::CallExpression *call, ast::Builtin builtin);
  void VisitBuiltinSizeOfCall(ast::CallExpression *call);
  void VisitBuiltinOffsetOfCall(ast::CallExpression *call);
  void VisitBuiltinIntCastCall(ast::CallExpression *call);
  void VisitBuiltinTrigCall(ast::CallExpression *call, ast::Builtin builtin);
  void VisitBuiltinBitsCall(ast::CallExpression *call, ast::Builtin builtin);

  // Dispatched from VisitCallExpr() for handling builtins
  void VisitBuiltinCallExpression(ast::CallExpression *call);
  void VisitRegularCallExpression(ast::CallExpression *call);

  // Dispatched from VisitBinaryOpExpr() for handling logical boolean expressions and arithmetic
  // expressions
  void VisitLogicalAndOrExpression(ast::BinaryOpExpression *node);
  void VisitArithmeticExpression(ast::BinaryOpExpression *node);

  // Dispatched from VisitArithmeticExpr for or SQL vs. primitive arithmetic
  void VisitPrimitiveArithmeticExpression(ast::BinaryOpExpression *node);
  void VisitSqlArithmeticExpression(ast::BinaryOpExpression *node);

  // Dispatched from VisitUnaryOp()
  void VisitAddressOfExpression(ast::UnaryOpExpression *op);
  void VisitDerefExpression(ast::UnaryOpExpression *op);
  void VisitArithmeticUnaryExpression(ast::UnaryOpExpression *op);
  void VisitLogicalNotExpression(ast::UnaryOpExpression *op);

  // Dispatched from VisitIndexExpr() to distinguish between array and map access
  void VisitArrayIndexExpression(ast::IndexExpression *node);
  void VisitMapIndexExpression(ast::IndexExpression *node);

  // Visit an expression for its L-Value
  LocalVar VisitExpressionForLValue(ast::Expression *expr);

  // Visit an expression for its R-Value and return the local variable holding its result
  LocalVar VisitExpressionForRValue(ast::Expression *expr);

  // Visit an expression for its R-Value, providing a destination variable where the result should
  // be stored
  void VisitExpressionForRValue(ast::Expression *expr, LocalVar dest);

  // Visit an expression that returns a SQL value. The address of the computed
  // value is returned.
  LocalVar VisitExpressionForSQLValue(ast::Expression *expr);

  enum class TestFallthrough : uint8_t { None, Then, Else };

  void VisitExpressionForTest(ast::Expression *expr, BytecodeLabel *then_label,
                              BytecodeLabel *else_label, TestFallthrough fallthrough);

  // Visit the body of an iteration statement
  void VisitIterationStatement(ast::IterationStatement *iteration, LoopBuilder *loop_builder);

  // Dispatched from VisitCompareOp for SQL vs. primitive comparisons
  void VisitSqlComparisonExpression(ast::ComparisonOpExpression *compare);
  void VisitPrimitiveComparisonExpression(ast::ComparisonOpExpression *compare);

  void BuildDeref(LocalVar dest, LocalVar ptr, ast::Type *dest_type);
  void BuildAssign(LocalVar dest, LocalVar val, ast::Type *dest_type);
  LocalVar BuildLoadPointer(LocalVar double_ptr, ast::Type *type);

  Bytecode GetIntTypedBytecode(Bytecode bytecode, ast::Type *type, bool sign = true);

  Bytecode GetFloatTypedBytecode(Bytecode bytecode, ast::Type *type);

  // Lookup a function's ID by its name
  FunctionId LookupFuncIdByName(const std::string &name) const;

  // Create a new static
  LocalVar NewStatic(const std::string &name, ast::Type *type, const void *contents);

  // Create a new static string
  LocalVar NewStaticString(ast::Context *ctx, ast::Identifier string);

  // Access the current execution result scope
  ExpressionResultScope *GetExecutionResult() { return execution_result_; }

  // Set the current execution result scope. We lose any previous scope, so it's the caller's
  // responsibility to restore it, if any.
  void SetExecutionResult(ExpressionResultScope *exec_result) { execution_result_ = exec_result; }

  // Access the current function that's being generated. May be NULL.
  FunctionInfo *GetCurrentFunction() { return &functions_.back(); }

 private:
  // The data section of the module
  std::vector<uint8_t> data_;

  // The bytecode generated during compilation
  std::vector<uint8_t> code_;

  // Constants stored in the data section
  std::vector<LocalInfo> static_locals_;
  std::unordered_map<std::string, uint32_t> static_locals_versions_;
  std::unordered_map<ast::Identifier, LocalVar> static_string_cache_;

  // Information about all generated functions
  std::vector<FunctionInfo> functions_;

  // Cache of function names to IDs for faster lookup
  std::unordered_map<std::string, FunctionId> func_map_;

  // Emitter to write bytecode into the code section
  BytecodeEmitter emitter_;

  // RAII struct to capture semantics of expression evaluation
  ExpressionResultScope *execution_result_;
};

}  // namespace tpl::vm
