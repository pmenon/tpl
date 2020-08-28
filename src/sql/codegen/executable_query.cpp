#include "sql/codegen/executable_query.h"

#include <algorithm>

#include "ast/context.h"
#include "common/defer.h"
#include "common/exception.h"
#include "logging/logger.h"
#include "sema/error_reporter.h"
#include "sql/codegen/execution_plan.h"
#include "sql/execution_context.h"
#include "vm/module.h"

namespace tpl::sql::codegen {

ExecutableQuery::ExecutableQuery(const planner::AbstractPlanNode &plan)
    : plan_(plan),
      errors_(std::make_unique<sema::ErrorReporter>()),
      ast_context_(std::make_unique<ast::Context>(errors_.get())),
      main_module_(nullptr),
      query_state_size_(0) {}

// Needed because we forward-declare classes used as template types to std::unique_ptr<>
ExecutableQuery::~ExecutableQuery() = default;

void ExecutableQuery::Setup(std::vector<std::unique_ptr<vm::Module>> &&modules,
                            vm::Module *main_module, std::string init_fn, std::string tear_down_fn,
                            ExecutionPlan &&execution_plan, std::size_t query_state_size) {
  TPL_ASSERT(main_module != nullptr, "No main module provided!");
  TPL_ASSERT(query_state_size >= sizeof(void *),
             "Query state must be large enough to store at least an ExecutionContext pointer.");

  if (std::find_if(modules.begin(), modules.end(),
                   [&](auto &m) { return m.get() == main_module; }) == modules.end()) {
    throw Exception(ExceptionType::CodeGen, "Main module isn't owned by modules list.");
  }

  if (auto init = main_module->GetFuncInfoByName(init_fn); init == nullptr) {
    const auto msg = fmt::format("Query init function '{}' does not exist in module", init_fn);
    throw Exception(ExceptionType::CodeGen, msg);
  }

  if (auto tear_down = main_module->GetFuncInfoByName(tear_down_fn); tear_down == nullptr) {
    const auto msg =
        fmt::format("Query tear-down function '{}' does not exist in module", tear_down_fn);
    throw Exception(ExceptionType::CodeGen, msg);
  }

  // Take ownership of parameters.
  modules_ = std::move(modules);
  main_module_ = main_module;
  init_fn_ = std::move(init_fn);
  tear_down_fn_ = std::move(tear_down_fn);
  execution_plan_ = std::move(execution_plan);
  query_state_size_ = query_state_size;
}

void ExecutableQuery::Run(ExecutionContext *exec_ctx, vm::ExecutionMode mode) {
  // First, allocate the query state and move the execution context into it.
  auto query_state = std::make_unique<byte[]>(query_state_size_);
  *reinterpret_cast<ExecutionContext **>(query_state.get()) = exec_ctx;

  // Pull out init and tear-down functions.
  ExecStepFn init, tear_down;
  bool found_init = main_module_->GetFunction(init_fn_, mode, init);
  bool found_destroy = main_module_->GetFunction(tear_down_fn_, mode, tear_down);
  // clang-format off
  (void)found_init; (void)found_destroy; // Force use.
  // clang-format on
  TPL_ASSERT(found_init, "Query initialization function does not exist in module!");
  TPL_ASSERT(found_destroy, "Query tear-down function does not exist in module!");

  // Defer the query tear-down logic so that it is run in case of any error.
  DEFER(tear_down(query_state.get()));

  // Run the query initialization function.
  init(query_state.get());

  // Now, run the main execution plan!
  execution_plan_.Run(query_state.get(), mode);

  // The query tear-down logic is deferred above; it is automatically executed
  // for us if any exceptions occur. Thus, we needn't manually execute it.
}

}  // namespace tpl::sql::codegen
