#include "sql/codegen/execution_plan.h"

#include "common/exception.h"
#include "vm/module.h"

namespace tpl::sql::codegen {

ExecutionStep::ExecutionStep(PipelineId pipeline_id, std::string func_name)
    : pipeline_id_(pipeline_id), func_name_(std::move(func_name)), func_id_(vm::kInvalidFuncId) {}

void ExecutionStep::Resolve(vm::Module *module) {
  TPL_ASSERT(module != nullptr, "Input module cannot be NULL");
  module_ = module;
  // Resolve the function ID.
  const vm::FunctionInfo *func = module_->GetFuncInfoByName(func_name_);
  if (func == nullptr) {
    throw Exception(
        ExceptionType::CodeGen,
        fmt::format("Unable to resolve function '{}'; does not exist in module!", func_name_));
  }
  // Done.
  func_id_ = func->GetId();
}

void ExecutionStep::Run(byte query_state[], vm::ExecutionMode mode) const {
  // Extract and invoke the function.
  ExecStepFn func;
  UNUSED const bool ret = module_->GetFunction(func_name_, mode, func);
  TPL_ASSERT(ret, "Function doesn't exist. Did you forget to resolve the execution step?");
  func(query_state);
}

ExecutionPlan::ExecutionPlan(std::vector<ExecutionStep> &&steps) : steps_(std::move(steps)) {}

void ExecutionPlan::Run(byte query_state[], vm::ExecutionMode mode) const {
  for (const auto &step : steps_) {
    step.Run(query_state, mode);
  }
}

}  // namespace tpl::sql::codegen
