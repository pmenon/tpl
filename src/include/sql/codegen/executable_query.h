#pragma once

#include <iosfwd>
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
   * A self-contained unit of execution that represents a chunk of a larger query. All executable
   * queries are composed of at least one fragment.
   */
  class Fragment {
   public:
    /**
     * Construct a fragment composed of the given functions from the given module.
     * @param functions The name of the functions to execute, in order.
     * @param module The module that contains the functions.
     */
    Fragment(std::vector<std::string> &&functions, std::unique_ptr<vm::Module> module);

    /**
     * Destructor.
     */
    ~Fragment();

    /**
     * Run this fragment using the provided opaque query state object.
     * @param query_state The query state.
     */
    void Run(byte query_state[]) const;

    /**
     * @return True if this fragment is compiled and executable.
     */
    bool IsCompiled() const { return module_ != nullptr; }

   private:
    // The functions that must be run (in the provided order) to execute this
    // query fragment.
    std::vector<std::string> functions_;
    // The module.
    std::unique_ptr<vm::Module> module_;
  };

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
  void Setup(std::vector<std::unique_ptr<Fragment>> &&fragments, std::size_t query_state_size);

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
  std::vector<std::unique_ptr<Fragment>> fragments_;
  // The query state size.
  std::size_t query_state_size_;
};

}  // namespace tpl::sql::codegen
