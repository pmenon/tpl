#pragma once

#include <functional>
#include <string>
#include <vector>

#include "common/common.h"
#include "sql/codegen/codegen_defs.h"
#include "vm/vm_defs.h"

namespace tpl::vm {
class Module;
}  // namespace tpl::vm

namespace tpl::sql::codegen {

using ExecStepFn = std::function<void(void *)>;

/**
 * A single step in the query processing pipeline. Steps are created and resolved separately.
 */
class ExecutionStep {
 public:
  /**
   * Create a new UNRESOLVED step for the pipeline with the provided ID. The step function is
   * encapsulated in a function with the provided name @em name.
   * @param pipeline_id The ID of the pipeline this steps is a part of.
   * @param func_name The name of the step function.
   */
  explicit ExecutionStep(PipelineId pipeline_id, std::string func_name);

  /**
   * Resolve this step in the provided module.
   * @param module The module.
   */
  void Resolve(vm::Module *module);

  /**
   * Run the this step using the provided state and in the given execution mode.
   * @param query_state The query state to pass to the step.
   * @param mode The mode to run the step (e.g., interpreted, compiled, etc.).
   */
  void Run(byte query_state[], vm::ExecutionMode mode) const;

  /**
   * @return The pipeline ID of this step.
   */
  PipelineId GetPipelineId() const { return pipeline_id_; }

 private:
  // The ID of the pipeline this step belongs to.
  PipelineId pipeline_id_;
  // The name of the function.
  std::string func_name_;
  // The ID of the function in the module. Set only on resolution.
  vm::FunctionId func_id_;
  // The module the steps belongs to. Set only on resolution.
  vm::Module *module_;
};

/**
 * An execution plan for a query.
 */
class ExecutionPlan {
 public:
  /**
   * Empty plan.
   */
  ExecutionPlan() = default;

  /**
   * Create a new execution plan composed of the provided steps.
   * @param steps The steps making up the plan.
   */
  explicit ExecutionPlan(std::vector<ExecutionStep> &&steps);

  /**
   * Run the plan using the provided query state, and using the given execution mode.
   * @param query_state The query state.
   * @param mode The execution mode.
   */
  void Run(byte query_state[], vm::ExecutionMode mode) const;

 private:
  // The steps in the plan.
  std::vector<ExecutionStep> steps_;
};

}  // namespace tpl::sql::codegen
