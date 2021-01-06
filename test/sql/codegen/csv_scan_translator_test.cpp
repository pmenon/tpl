#include <memory>

#include "sql/planner/plannodes/csv_scan_plan_node.h"
#include "sql/schema.h"
#include "util/file.h"

// Tests
#include "sql/codegen/output_checker.h"
#include "sql/planner/expression_maker.h"
#include "sql/planner/output_schema_util.h"
#include "util/codegen_test_harness.h"

namespace tpl::sql::codegen {

using namespace std::chrono_literals;

class CSVScanTranslatorTest : public CodegenBasedTest {
 protected:
  void SetUp() override {
    // Call up.
    CodegenBasedTest::SetUp();
    // Setup test file.
    path_ = std::filesystem::temp_directory_path() / "tpl-csvtest-tmp.csv";
    file_.Open(path_, util::File::FLAG_CREATE_ALWAYS | util::File::FLAG_WRITE);
  }

  void TearDown() override {
    // Call up.
    CodegenBasedTest::TearDown();
    // Delete test file.
    file_.Close();
    // Remove the file, ignoring the error code.
    std::error_code ec;
    std::filesystem::remove(path_, ec);
  }

  void CreateTemporaryTestCSV(std::string_view contents) {
    file_.Seek(util::File::Whence::FROM_BEGIN, 0);
    file_.WriteFull(reinterpret_cast<const byte *>(contents.data()), contents.size());
    file_.Flush();
  }

  std::string GetTestCSVPath() const { return path_.string(); }

 private:
  // The path to the test file.
  std::filesystem::path path_;
  // The open file handle.
  util::File file_;
};

TEST_F(CSVScanTranslatorTest, ManyTypesTest) {
  // Temporary CSV.
  CreateTemporaryTestCSV(
      "1,2,3,4,5.0,\"six\"\n"
      "7,8,9,10,11.12,\"thirteen\"\n");

  planner::ExpressionMaker expr_maker;

  std::unique_ptr<planner::AbstractPlanNode> csv_scan;
  planner::OutputSchemaHelper seq_scan_out(&expr_maker, 0);
  {
    auto col1 = expr_maker.CVE(0, Type::TinyIntType(false));
    auto col2 = expr_maker.CVE(1, Type::SmallIntType(false));
    auto col3 = expr_maker.CVE(2, Type::IntegerType(false));
    auto col4 = expr_maker.CVE(3, Type::BigIntType(false));
    auto col5 = expr_maker.CVE(4, Type::RealType(false));
    auto col6 = expr_maker.CVE(5, Type::VarcharType(false, 20));

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
                   .SetFileName(GetTestCSVPath())
                   .SetScanPredicate(nullptr)
                   .Build();
  }

  // Run and check.
  ExecuteAndCheckInAllModes(*csv_scan, []() {
    // Expect two output tuples.
    // TODO(pmenon): Check attribute values.
    return std::make_unique<TupleCounterChecker>(2);
  });
}

}  // namespace tpl::sql::codegen
