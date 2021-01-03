#pragma once

#include "common/common.h"
#include "sql/codegen/ast_fwd.h"
#include "sql/codegen/edsl/value.h"

namespace tpl::sql::codegen {

class FunctionBuilder;

/**
 * Helper class to generate TPL loops. Immediately after construction, statements appended to the
 * current active function are appended to the loop's body.
 *
 * @code
 * Value<int> a, b;
 * ...
 * Loop loop(codegen, a < b);
 * {
 *   // This code will appear in the "then" block of the statement.
 * }
 * loop.EndLoop();
 * @endcode
 */
class Loop {
 public:
  /**
   * Create a full loop.
   * @param codegen The code generator.
   * @param init The initialization statements.
   * @param condition The loop condition.
   * @param next The next statements.
   */
  explicit Loop(FunctionBuilder *function, const edsl::Value<void> &init,
                const edsl::Value<bool> &condition, const edsl::Value<void> &next);

  /**
   * Create a while-loop.
   * @param codegen The code generator instance.
   * @param condition The loop condition.
   */
  explicit Loop(FunctionBuilder *function, const edsl::Value<bool> &condition);

  /**
   * Destructor.
   */
  ~Loop();

  /**
   * Explicitly mark the end of a loop.
   */
  void EndLoop();

 private:
  // The function this loop is appended to.
  FunctionBuilder *function_;
  // The loop position.
  const SourcePosition position_;
  // The previous list of statements.
  ast::BlockStatement *prev_statements_;
  // The initial statements, loop condition, and next statements.
  ast::Statement *init_;
  ast::Expression *cond_;
  ast::Statement *next_;
  // The loop body.
  ast::BlockStatement *body_;
  // Completion flag.
  bool completed_;
};

}  // namespace tpl::sql::codegen
