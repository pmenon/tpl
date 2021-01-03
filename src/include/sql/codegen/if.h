#pragma once

#include "common/common.h"
#include "sql/codegen/ast_fwd.h"
#include "sql/codegen/edsl/value.h"

namespace tpl::sql::codegen {

class FunctionBuilder;

/**
 * Helper class to generate TPL if-then-else statements. Immediately after construction, anything
 * that's appended to the current active function will be inserted into the "then" clause of the
 * generated statement.
 *
 * @code
 * If a_lt_b(codegen, a < b);
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
   * Instantiate an if-clause using the primitive boolean condition.
   * @param function The function containing the statement.
   * @param condition The boolean condition.
   */
  If(FunctionBuilder *function, const edsl::Value<bool> &condition);

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
  // The function to append this if statement to.
  FunctionBuilder *function_;
  // The start position;
  const SourcePosition position_;
  // Previous function statement list.
  ast::BlockStatement *prev_func_stmt_list_;
  // The condition.
  ast::Expression *condition_;
  // The statements in the "then" clause.
  ast::BlockStatement *then_stmts_;
  // The statements in the "else" clause.
  ast::BlockStatement *else_stmts_;
  // Flag indicating if the if-statement has completed.
  bool completed_;
};

}  // namespace tpl::sql::codegen
