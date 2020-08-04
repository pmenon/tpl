#pragma once

#include <functional>
#include <vector>

#include "common/common.h"
#include "sql/codegen/codegen_defs.h"
#include "vm/vm_defs.h"

namespace tpl::vm {
class Module;
}  // namespace tpl::vm

namespace tpl::sql::codegen {

using ExecStepFn = std::function<void(void *)>;

class ExecutionStep {
 public:
  ExecutionStep(PipelineId pipeline_id, std::string func_name);

  void Resolve(vm::Module *module);

  void Run(byte query_state[], vm::ExecutionMode mode) const;

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

class ExecutionPlan {
 public:
  ExecutionPlan(std::vector<ExecutionStep> &&steps);

  void Run(byte query_state[], vm::ExecutionMode mode) const;

 private:
  // The steps in the plan.
  std::vector<ExecutionStep> steps_;
};

}  // namespace tpl::sql::codegen
