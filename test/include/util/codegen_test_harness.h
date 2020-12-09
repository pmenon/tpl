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
    for (const auto parallel : {false, true}) {
      // Set parallelization setting.
      Settings::Instance()->Set(Settings::Name::ParallelQueryExecution, parallel);
      // Compile.
      auto query = sql::codegen::CompilationContext::Compile(plan);
      for (const auto mode : {vm::ExecutionMode::Interpret}) {
        // Create a checker.
        std::unique_ptr<sql::codegen::OutputChecker> checker = checker_maker();
        sql::codegen::OutputCollectorAndChecker store(checker.get(), plan.GetOutputSchema());
        // Run the query.
        sql::MemoryPool memory(nullptr);
        sql::ExecutionContext exec_ctx(&memory, plan.GetOutputSchema(), &store);
        query->Run(&exec_ctx, mode);
        // Check.
        checker->CheckCorrectness();
      }
    }
  }
};

}  // namespace tpl
