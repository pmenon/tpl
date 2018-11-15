#pragma once

#include "ast/ast.h"
#include "ast/ast_visitor.h"
#include "vm/emitter.h"

namespace tpl::vm {

class Module;
class LoopBuilder;

/**
 * This class takes a TPL program in the form of an AST and compiles it into a
 * TBC bytecode compilation unit.
 */
class BytecodeGenerator : public ast::AstVisitor<BytecodeGenerator> {
 public:
  DISALLOW_COPY_AND_MOVE(BytecodeGenerator);

  // Declare all node visit methods here
#define DECLARE_VISIT_METHOD(type) void Visit##type(ast::type *node);
  AST_NODES(DECLARE_VISIT_METHOD)
#undef DECLARE_VISIT_METHOD

  static std::unique_ptr<Module> Compile(util::Region *region,
                                         ast::AstNode *root);

 private:
  // Private constructor to force users to call Compile()
  explicit BytecodeGenerator(util::Region *region);

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

  // Dispatched from VisitIndexExpr() to distinguish between array and map
  // access.
  void VisitArrayIndexExpr(ast::IndexExpr *node);
  void VisitMapIndexExpr(ast::IndexExpr *node);

  // Visit an expression for its L-Value
  LocalVar VisitExpressionForLValue(ast::Expr *expr);

  // Visit an expression for its R-Value and return the local variable holding
  // its result
  LocalVar VisitExpressionForRValue(ast::Expr *expr);

  // Visit an expression for its R-Value providing a destination variable where
  // the result should be stored
  void VisitExpressionForRValue(ast::Expr *expr, LocalVar dest);

  enum class TestFallthrough : u8 { None, Then, Else };

  void VisitExpressionForTest(ast::Expr *expr, Label *then_label,
                              Label *else_label, TestFallthrough fallthrough);

  // Visit the body of an iteration statement
  void VisitIterationStatement(ast::IterationStmt *iteration,
                               LoopBuilder *loop_builder);

  // Dispatched from VisitCompareOp for SQL vs. primitive comparisons
  void VisitSqlCompareOpExpr(ast::ComparisonOpExpr *compare);
  void VisitPrimitiveCompareOpExpr(ast::ComparisonOpExpr *compare);

  void BuildDeref(LocalVar dest, LocalVar ptr, ast::Type *dest_type);
  LocalVar BuildLoadPointer(LocalVar double_ptr, ast::Type *type);

  Bytecode GetIntTypedBytecode(Bytecode bytecode, ast::Type *type);

 public:
  Emitter *emitter() { return &emitter_; }

 private:
  ExpressionResultScope *execution_result() { return execution_result_; }
  void set_execution_result(ExpressionResultScope *execution_result) {
    execution_result_ = execution_result;
  }

  FunctionInfo *current_function() { return &functions_.back(); }

  util::RegionVector<u8> &bytecode() { return bytecode_; }

  const util::RegionVector<FunctionInfo> &functions() const {
    return functions_;
  }

 private:
  // Region
  util::Region *region_;

  // The bytecode generated during compilation
  util::RegionVector<u8> bytecode_;

  // Information about all generated functions
  util::RegionVector<FunctionInfo> functions_;

  // Emitter to write bytecode ops
  Emitter emitter_;

  // RAII struct to capture semantics of expression evaluation
  ExpressionResultScope *execution_result_;
};

}  // namespace tpl::vm