#pragma once

#include "ast/ast.h"
#include "ast/ast_visitor.h"
#include "vm/bytecode_emitter.h"

namespace tpl::vm {

class BytecodeModule;
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

  static std::unique_ptr<BytecodeModule> Compile(util::Region *region,
                                                 ast::AstNode *root);

 private:
  // Private constructor to force users to call Compile()
  explicit BytecodeGenerator(util::Region *region);

  class ExpressionResultScope;
  class LValueResultScope;
  class RValueResultScope;
  class BytecodePositionScope;

  // Allocate a new function ID
  FunctionInfo *AllocateFunc(const std::string &name);

  // Dispatched from VisitBinaryOpExpr() for handling logical boolean
  // expressions and arithmetic expressions
  void VisitLogicalAndOrExpr(ast::BinaryOpExpr *node);
  void VisitArithmeticExpr(ast::BinaryOpExpr *node);

  // Dispatched from VisitUnaryOp()
  void VisitAddressOfExpr(ast::UnaryOpExpr *op);
  void VisitDerefExpr(ast::UnaryOpExpr *op);
  void VisitArithmeticUnaryExpr(ast::UnaryOpExpr *op);

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

  void VisitExpressionForTest(ast::Expr *expr, BytecodeLabel *then_label,
                              BytecodeLabel *else_label,
                              TestFallthrough fallthrough);

  // Visit the body of an iteration statement
  void VisitIterationStatement(ast::IterationStmt *iteration,
                               LoopBuilder *loop_builder);

  // Dispatched from VisitCompareOp for SQL vs. primitive comparisons
  void VisitSqlCompareOpExpr(ast::ComparisonOpExpr *compare);
  void VisitPrimitiveCompareOpExpr(ast::ComparisonOpExpr *compare);

  void BuildDeref(LocalVar dest, LocalVar ptr, ast::Type *dest_type);
  void BuildAssign(LocalVar dest, LocalVar ptr, ast::Type *dest_type);
  LocalVar BuildLoadPointer(LocalVar double_ptr, ast::Type *type);

  Bytecode GetIntTypedBytecode(Bytecode bytecode, ast::Type *type);

 public:
  BytecodeEmitter *emitter() { return &emitter_; }

 private:
  const FunctionInfo *LookupFuncInfoByName(const std::string &name) const {
    for (const auto &func : functions()) {
      if (func.name() == name) {
        return &func;
      }
    }
    return nullptr;
  }

  // -------------------------------------------------------
  // Accessors
  // -------------------------------------------------------

  ExpressionResultScope *execution_result() { return execution_result_; }

  void set_execution_result(ExpressionResultScope *execution_result) {
    execution_result_ = execution_result;
  }

  FunctionInfo *current_function() { return &functions_.back(); }

  util::RegionVector<u8> &bytecode() { return bytecode_; }

  util::RegionVector<FunctionInfo> &functions() { return functions_; }

  const util::RegionVector<FunctionInfo> &functions() const {
    return functions_;
  }

 private:
  // The bytecode generated during compilation
  util::RegionVector<u8> bytecode_;

  // Information about all generated functions
  util::RegionVector<FunctionInfo> functions_;

  // Emitter to write bytecode ops
  BytecodeEmitter emitter_;

  // RAII struct to capture semantics of expression evaluation
  ExpressionResultScope *execution_result_;
};

}  // namespace tpl::vm