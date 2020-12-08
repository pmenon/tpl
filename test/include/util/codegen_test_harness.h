#pragma once

#include <memory>

#include "util/sql_test_harness.h"

#include "common/settings.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/executable_query.h"
#include "sql/execution_context.h"
#include "sql/planner/plannodes/abstract_plan_node.h"
#include "sql/result_consumer.h"
#include "vm/llvm_engine.h"

// Test
#include "sql/codegen/output_checker.h"

namespace tpl {

class CodegenBasedTest : public TplTest {
 public:
  static void SetUpTestSuite() { vm::LLVMEngine::Initialize(); }
  static void TearDownTestSuite() { vm::LLVMEngine::Shutdown(); }

 protected:
  void ExecuteAndCheckInAllModes(
      const sql::planner::AbstractPlanNode &plan,
      std::function<std::unique_ptr<sql::codegen::OutputChecker>()> checker_maker) {
    auto compile_and_check = [&] {
      // Compile.
      auto query = sql::codegen::CompilationContext::Compile(plan);
      // Test in all modes.
      for (const auto mode : {vm::ExecutionMode::Interpret}) {
        // Create a checker.
        std::unique_ptr<sql::codegen::OutputChecker> checker = checker_maker();
        sql::codegen::OutputCollectorAndChecker store(checker.get(), plan.GetOutputSchema());
        // Setup and run the query.
        sql::MemoryPool memory(nullptr);
        sql::ExecutionContext exec_ctx(&memory, plan.GetOutputSchema(), &store);
        query->Run(&exec_ctx, mode);
        // Check.
        checker->CheckCorrectness();
      }
    };

    // Disable parallel execution.
    Settings::Instance()->Set(Settings::Name::ParallelQueryExecution, false);
    compile_and_check();

    // Enable parallel execution.
    Settings::Instance()->Set(Settings::Name::ParallelQueryExecution, true);
    compile_and_check();
  }
};

}  // namespace tpl
