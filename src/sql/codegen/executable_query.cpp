#include "sql/codegen/executable_query.h"

#include "ast/context.h"
#include "logging/logger.h"
#include "sema/error_reporter.h"
#include "sql/codegen/code_container.h"
#include "sql/execution_context.h"

namespace tpl::sql::codegen {

ExecutableQuery::ExecutableQuery(const planner::AbstractPlanNode &plan)
    : plan_(plan),
      errors_(std::make_unique<sema::ErrorReporter>()),
      ast_context_(std::make_unique<ast::Context>(errors_.get())),
      query_state_size_(0) {}

// Needed because we forward-declare classes used as template types to std::unique_ptr<>
ExecutableQuery::~ExecutableQuery() = default;

void ExecutableQuery::Setup(std::vector<std::unique_ptr<CodeContainer>> &&fragments,
                            const std::size_t query_state_size) {
  TPL_ASSERT(std::all_of(fragments.begin(), fragments.end(),
                         [](const auto &fragment) { return fragment->IsCompiled(); }),
             "All query fragments are not compiled!");
  TPL_ASSERT(
      query_state_size >= sizeof(ExecutionContext *),
      "Query state size must be large enough to store at least a pointer to the ExecutionContext.");

  fragments_ = std::move(fragments);
  query_state_size_ = query_state_size;

  LOG_INFO("Query has {} fragments with {}-byte query state", fragments_.size(), query_state_size_);

  for (auto &fragment : fragments_) {
    fragment->PrettyPrint();
  }
}

void ExecutableQuery::Run(ExecutionContext *exec_ctx) {
  // First, allocate the query state and move the execution context into it.
  auto query_state = std::make_unique<byte[]>(query_state_size_);
  *reinterpret_cast<ExecutionContext **>(query_state.get()) = exec_ctx;

  // Now run through fragments.
  for (const auto &fragment : fragments_) {
    // TODO(pmenon): Complete me.
    (void)fragment;
  }
}

}  // namespace tpl::sql::codegen
