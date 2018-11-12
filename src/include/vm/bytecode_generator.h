#pragma once

#include "ast/ast.h"
#include "ast/ast_visitor.h"
#include "vm/bytecode_emitter.h"

namespace tpl::vm {

class BytecodeUnit;
class LoopBuilder;

/**
 * This class takes a TPL program in the form of an AST and compiles it into a
 * TBC bytecode compilation unit.
 */
class BytecodeGenerator : public ast::AstVisitor<BytecodeGenerator> {
 public:
  BytecodeGenerator();

  DISALLOW_COPY_AND_MOVE(BytecodeGenerator);

  // Declare all node visit methods here
#define DECLARE_VISIT_METHOD(type) void Visit##type(ast::type *node);
  AST_NODES(DECLARE_VISIT_METHOD)
#undef DECLARE_VISIT_METHOD

  static std::unique_ptr<BytecodeUnit> Compile(ast::AstNode *root);

 private:
  class ExpressionResultScope;
  class LValueResultScope;
  class RValueResultScope;
  class TestResultScope;
  class BytecodePositionScope;

  // Allocate a new function ID
  FunctionInfo *AllocateFunc(const std::string &name);

  // These are dispatched from VisitBinaryOpExpr() for handling logical boolean
  // expressions and arithmetic expressions
  void VisitLogicalAndOrExpr(ast::BinaryOpExpr *node);
  void VisitArithmeticExpr(ast::BinaryOpExpr *node);

  // Visit an expression for its L-Value
  LocalVar VisitExpressionForLValue(ast::Expr *expr);

  // Visit an expression for its R-Value and return the local variable holding
  // its result
  LocalVar VisitExpressionForRValue(ast::Expr *expr);

  // Visit an expression for its R-Value providing a destination variable where
  // the result should be stored
  void VisitExpressionForRValue(ast::Expr *expr, LocalVar dest);

  enum class TestFallthrough : u8 { None, Then, Else };

  void VisitExpressionForTest(ast::Expr *expr, BytecodeLabel *then_label,
                              BytecodeLabel *else_label,
                              TestFallthrough fallthrough);

  // Visit the body of an iteration statement
  void VisitIterationStatement(ast::IterationStmt *iteration,
                               LoopBuilder *loop_builder);

  // Dispatched from VisitCompareOp for SQL vs. primitive comparisons
  void VisitSqlCompareOpExpr(ast::ComparisonOpExpr *compare);
  void VisitPrimitiveCompareOpExpr(ast::ComparisonOpExpr *compare);

  LocalVar BuildLoadPointer(LocalVar double_ptr, ast::Type *type);

  Bytecode GetIntTypedBytecode(Bytecode bytecode, ast::Type *type);

 public:
  BytecodeEmitter *emitter() { return &emitter_; }

 private:
  ExpressionResultScope *execution_result() { return execution_result_; }
  void set_execution_result(ExpressionResultScope *execution_result) {
    execution_result_ = execution_result;
  }

  FunctionInfo *current_function() { return &functions_.back(); }

  const std::vector<FunctionInfo> &functions() const { return functions_; }

 private:
  BytecodeEmitter emitter_;
  ExpressionResultScope *execution_result_;

  FunctionId func_id_counter_;
  std::vector<FunctionInfo> functions_;
};

}  // namespace tpl::vm