#pragma once

#include "common/common.h"
#include "sql/codegen/ast_fwd.h"

namespace tpl::sql::codegen {

class CodeGen;

/**
 * Helper class to generate TPL if-then-else statements. Immediately after construction, anything
 * that's appended to the current active function will be inserted into the "then" clause of the
 * generated statement.
 *
 * @code
 * auto cond = codegen->CompareLt(a, b);
 * If a_lt_b(codegen, cond);
 * {
 *   // This code will appear in the "then" block of the statement.
 * }
 * a_lt_b.Else();
 * {
 *   // This code will appear in the "else" block of the statement.
 * }
 * a_lt_b.EndIf();
 * @endcode
 */
class If {
 public:
  /**
   * Create a new if-statement using the provided boolean if-condition.
   * @param codegen The code generator instance.
   * @param condition The boolean condition.
   */
  If(CodeGen *codegen, ast::Expr *condition);

  /**
   * Destructor will complete the statement.
   */
  ~If();

  /**
   * Begin an else clause.
   */
  void Else();

  /**
   * Finish the if-then-else statement. The generated statement will be appended to the current
   * function.
   */
  void EndIf();

 private:
  // The code generator instance.
  CodeGen *codegen_;
  // The start position;
  const SourcePosition position_;
  // Previous function statement list.
  ast::BlockStmt *prev_func_stmt_list_;
  // The condition.
  ast::Expr *condition_;
  // The statements in the "then" clause.
  ast::BlockStmt *then_stmts_;
  // The statements in the "else" clause.
  ast::BlockStmt *else_stmts_;
  // Flag indicating if the if-statement has completed.
  bool completed_;
};

}  // namespace tpl::sql::codegen