#pragma once

#include <iosfwd>
#include <memory>
#include <string>
#include <vector>

#include "common/common.h"
#include "common/macros.h"
#include "sql/codegen/ast_fwd.h"
#include "sql/codegen/codegen_defs.h"
#include "sql/codegen/execution_plan.h"
#include "vm/vm_defs.h"

namespace tpl::sema {
class ErrorReporter;
}  // namespace tpl::sema

namespace tpl::sql {
class ExecutionContext;
}  // namespace tpl::sql

namespace tpl::sql::planner {
class AbstractPlanNode;
}  // namespace tpl::sql::planner

namespace tpl::vm {
class Module;
}  // namespace tpl::vm

namespace tpl::sql::codegen {

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
   * Destructor.
   */
  ~ExecutableQuery();

  /**
   * Setup the compiled query using the provided fragments.
   * @param fragments The fragments making up the query. These are provided as a vector in the order
   *                  they're to be executed.
   * @param query_state_size The size of the state structure this query needs. This value is
   *                         represented in bytes.
   */
  void Setup(std::vector<std::unique_ptr<vm::Module>> &&modules, vm::Module *main_module,
             std::string init_fn, std::string tear_down_fn, ExecutionPlan &&execution_plan,
             std::size_t query_state_size);

  /**
   * Execute the query.
   * @param exec_ctx The context in which to execute the query.
   * @param mode The execution mode to use when running the query. By default, its interpreted.
   */
  void Run(ExecutionContext *exec_ctx, vm::ExecutionMode mode = vm::ExecutionMode::Interpret);

  /**
   * @return The physical plan this executable query implements.
   */
  const planner::AbstractPlanNode &GetPlan() const { return plan_; }

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
  std::vector<std::unique_ptr<vm::Module>> modules_;
  // The module holding the query initialization and tear-down logic.
  vm::Module *main_module_;
  // The IDs of the initialization and tear-down functions.
  std::string init_fn_, tear_down_fn_;
  // The execution plan.
  ExecutionPlan execution_plan_;
  // The query state size.
  std::size_t query_state_size_;
};

}  // namespace tpl::sql::codegen
