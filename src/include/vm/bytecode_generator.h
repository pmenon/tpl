#pragma once

#include "ast/ast.h"
#include "ast/ast_visitor.h"
#include "vm/bytecode_emitter.h"

namespace tpl::vm {

class BytecodeUnit;

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

  // Allocate a new function ID
  FunctionInfo *AllocateFunc(const std::string &name);

  // Visit an expression and return the register that holds its result
  RegisterId VisitExpressionForValue(ast::Expr *expr);

  // Visit an expression providing a target register where its results should be
  // stored into
  void VisitExpressionWithTarget(ast::Expr *expr, RegisterId reg_id);

 private:
  Bytecode GetIntTypedBytecode(Bytecode bytecode, ast::Type *type);

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Accessors
  ///
  //////////////////////////////////////////////////////////////////////////////

  BytecodeEmitter *emitter() { return &emitter_; }

  ExpressionResultScope *execution_result() { return execution_result_; }
  void set_execution_result(ExpressionResultScope *execution_result) {
    execution_result_ = execution_result;
  }

  FunctionInfo *curr_func() { return &functions_.back(); }

  const std::vector<FunctionInfo> &functions() const { return functions_; }

 private:
  BytecodeEmitter emitter_;
  ExpressionResultScope *execution_result_;

  FunctionId func_id_counter_;
  std::vector<FunctionInfo> functions_;
};

}  // namespace tpl::vm