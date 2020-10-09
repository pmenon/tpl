#pragma once

#include <memory>

#include "util/sql_test_harness.h"

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
      sql::codegen::ExecutableQuery *query,
      std::function<std::unique_ptr<sql::codegen::OutputChecker>()> checker_maker) {
    const sql::planner::OutputSchema *output_schema = query->GetPlan().GetOutputSchema();
    // Test in all modes.
    for (const auto mode : {vm::ExecutionMode::Interpret}) {
      // Create a checker.
      std::unique_ptr<sql::codegen::OutputChecker> checker = checker_maker();
      sql::codegen::OutputCollectorAndChecker store(checker.get(), output_schema);
      // Setup and run the query.
      sql::MemoryPool memory(nullptr);
      sql::ExecutionContext exec_ctx(&memory, output_schema, &store);
      query->Run(&exec_ctx, mode);
      // Check.
      checker->CheckCorrectness();
    }
  }
};

}  // namespace tpl
