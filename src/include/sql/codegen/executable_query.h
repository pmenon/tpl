#pragma once

#include <memory>
#include <vector>

#include "common/common.h"
#include "common/macros.h"
#include "sql/codegen/ast_fwd.h"

namespace tpl::sema {
class ErrorReporter;
}  // namespace tpl::sema

namespace tpl::sql {
class ExecutionContext;
}  // namespace tpl::sql

namespace tpl::sql::planner {
class AbstractPlanNode;
}  // namespace tpl::sql::planner

namespace tpl::sql::codegen {

class CodeContainer;

/**
 * An compiled and executable query object.
 */
class ExecutableQuery {
 public:
  /**
   * Create a query object.
   * @param plan The physical plan.
   */
  explicit ExecutableQuery(const planner::AbstractPlanNode &plan);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(ExecutableQuery);

  /**
   * Destructor.
   */
  ~ExecutableQuery();

  /**
   * Setup the compiled query using the provided fragments.
   * @param fragments
   */
  void Setup(std::vector<std::unique_ptr<CodeContainer>> &&fragments, std::size_t query_state_size);

  /**
   * Execute the query.
   * @param exec_ctx The context in which to execute the query.
   */
  void Run(ExecutionContext *exec_ctx);

  /**
   * @return The AST context.
   */
  ast::Context *GetContext() { return ast_context_.get(); }

 private:
  // The plan.
  const planner::AbstractPlanNode &plan_;
  // The AST error reporter.
  std::unique_ptr<sema::ErrorReporter> errors_;
  // The AST context used to generate the TPL AST.
  std::unique_ptr<ast::Context> ast_context_;
  // The compiled query fragments that make up the query.
  std::vector<std::unique_ptr<CodeContainer>> fragments_;
  // The query state size.
  std::size_t query_state_size_;
};

}  // namespace tpl::sql::codegen
