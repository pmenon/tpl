#include "util/sql_test_harness.h"

#include <memory>

#include "sql/codegen/compilation_context.h"
#include "sql/execution_context.h"
#include "sql/planner/plannodes/csv_scan_plan_node.h"
#include "sql/printing_consumer.h"
#include "sql/schema.h"
#include "util/file.h"
#include "vm/llvm_engine.h"

// Tests
#include "sql/codegen/output_checker.h"
#include "sql/planner/expression_maker.h"
#include "sql/planner/output_schema_util.h"

namespace tpl::sql::codegen {

using namespace std::chrono_literals;

class CSVScanTranslatorTest : public SqlBasedTest {
 protected:
  // Initialize and tear-down LLVM once.
  static void SetUpTestSuite() { tpl::vm::LLVMEngine::Initialize(); }
  static void TearDownTestSuite() { tpl::vm::LLVMEngine::Shutdown(); }

  // Call up.
  void SetUp() override { SqlBasedTest::SetUp(); }

  // Call up and delete all files.
  void TearDown() override {
    SqlBasedTest::TearDown();
    for (const auto &[path, file] : files_) {
      file->Close();
      // Remove the file, ignoring the error code.
      std::error_code ec;
      std::filesystem::remove(path, ec);
    }
  }

  std::pair<std::string, util::File *> CreateTemporaryTestFile() {
    const auto path = std::filesystem::temp_directory_path() / "tpl-csvtest-tmp.csv";
    const auto flags = util::File::FLAG_CREATE_ALWAYS | util::File::FLAG_WRITE;
    auto &[_, file] = files_.emplace_back(path, std::make_unique<util::File>(path, flags));
    if (!file->IsOpen() || file->HasError()) {
      throw std::runtime_error(util::File::ErrorToString(file->GetErrorIndicator()));
    }
    return std::make_pair(path.string(), file.get());
  }

  std::string CreateTemporaryTestCSV(std::string_view contents) {
    auto [path, file] = CreateTemporaryTestFile();
    file->WriteFull(reinterpret_cast<const byte *>(contents.data()), contents.size());
    file->Flush();
    return path;
  }

 private:
  std::vector<std::pair<std::filesystem::path, std::unique_ptr<util::File>>> files_;
};

TEST_F(CSVScanTranslatorTest, ManyTypesTest) {
  // Temporary CSV.
  auto path = CreateTemporaryTestCSV(
      "1,2,3,4,5.0,\"six\"\n"
      "7,8,9,10,11.12,\"thirteen\"\n");

  planner::ExpressionMaker expr_maker;

  std::unique_ptr<planner::AbstractPlanNode> csv_scan;
  planner::OutputSchemaHelper seq_scan_out(&expr_maker, 0);
  {
    auto col1 = expr_maker.CVE(0, sql::TypeId::TinyInt);
    auto col2 = expr_maker.CVE(1, sql::TypeId::SmallInt);
    auto col3 = expr_maker.CVE(2, sql::TypeId::Integer);
    auto col4 = expr_maker.CVE(3, sql::TypeId::BigInt);
    auto col5 = expr_maker.CVE(4, sql::TypeId::Float);
    auto col6 = expr_maker.CVE(5, sql::TypeId::Varchar);

    seq_scan_out.AddOutput("col1", col1);
    seq_scan_out.AddOutput("col2", col2);
    seq_scan_out.AddOutput("col3", col3);
    seq_scan_out.AddOutput("col4", col4);
    seq_scan_out.AddOutput("col5", col5);
    seq_scan_out.AddOutput("col6", col6);

    auto schema = seq_scan_out.MakeSchema();
    // Build
    csv_scan = planner::CSVScanPlanNode::Builder()
                   .SetOutputSchema(std::move(schema))
                   .SetFileName(path)
                   .SetScanPredicate(nullptr)
                   .Build();
  }

  // Make the checkers:
  // 1. Expect two tuples.
  // 2. The count should be size of the table.
  // 3. The sum should be sum from [1,N] where N=num_tuples.
  TupleCounterChecker counter(2);
  MultiChecker multi_checker({&counter});

  // Compile and Run
  OutputCollectorAndChecker store(&multi_checker, csv_scan->GetOutputSchema());
  PrintingConsumer consumer(std::cout, csv_scan->GetOutputSchema());
  MultiOutputCallback callback({&store});
  sql::MemoryPool memory(nullptr);
  sql::ExecutionContext exec_ctx(&memory, csv_scan->GetOutputSchema(), &callback);

  // Run & Check
  auto query = CompilationContext::Compile(*csv_scan);
  query->Run(&exec_ctx);

  multi_checker.CheckCorrectness();
}

}  // namespace tpl::sql::codegen
