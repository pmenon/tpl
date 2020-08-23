#include <memory>

#include "sql/catalog.h"
#include "sql/codegen/compilation_context.h"
#include "sql/planner/plannodes/order_by_plan_node.h"
#include "sql/planner/plannodes/seq_scan_plan_node.h"
#include "sql/printing_consumer.h"
#include "sql/schema.h"
#include "sql/table.h"

// Tests
#include "sql/codegen/output_checker.h"
#include "sql/planner/expression_maker.h"
#include "sql/planner/output_schema_util.h"
#include "util/codegen_test_harness.h"

namespace tpl::sql::codegen {

using namespace std::chrono_literals;

class SortTranslatorTest : public CodegenBasedTest {
 protected:
  void TestSortWithLimitAndOrOffset(uint64_t off, uint64_t lim) {
    // SELECT col1, col2 FROM test_1 ORDER BY col2 ASC OFFSET off LIMIT lim;

    // Get accessor
    auto accessor = sql::Catalog::Instance();
    planner::ExpressionMaker expr_maker;
    sql::Table *table = accessor->LookupTableByName("small_1");
    const auto &table_schema = table->GetSchema();

    // Scan
    std::unique_ptr<planner::AbstractPlanNode> seq_scan;
    planner::OutputSchemaHelper seq_scan_out{&expr_maker, 0};
    {
      // Get Table columns
      auto col1 = expr_maker.CVE(table_schema.GetColumnInfo("col1").oid, sql::TypeId::Integer);
      auto col2 = expr_maker.CVE(table_schema.GetColumnInfo("col2").oid, sql::TypeId::Integer);
      seq_scan_out.AddOutput("col1", col1);
      seq_scan_out.AddOutput("col2", col2);
      auto schema = seq_scan_out.MakeSchema();
      // Build
      planner::SeqScanPlanNode::Builder builder;
      seq_scan = builder.SetOutputSchema(std::move(schema)).SetTableOid(table->GetId()).Build();
    }

    // Order By
    std::unique_ptr<planner::AbstractPlanNode> order_by;
    planner::OutputSchemaHelper order_by_out(&expr_maker, 0);
    {
      // Output columns col1, col2
      auto col1 = seq_scan_out.GetOutput("col1");
      auto col2 = seq_scan_out.GetOutput("col2");
      order_by_out.AddOutput("col1", col1);
      order_by_out.AddOutput("col2", col2);
      auto schema = order_by_out.MakeSchema();
      // Build
      planner::OrderByPlanNode::Builder builder;
      builder.SetOutputSchema(std::move(schema))
          .AddChild(std::move(seq_scan))
          .AddSortKey(col2, planner::OrderByOrderingType::ASC);
      if (off != 0) builder.SetOffset(off);
      if (lim != 0) builder.SetLimit(lim);
      order_by = builder.Build();
    }

    uint32_t expected_tuple_count = 0;
    if (lim == 0) {
      expected_tuple_count = table->GetTupleCount() > off ? table->GetTupleCount() - off : 0;
    } else {
      expected_tuple_count =
          table->GetTupleCount() > off ? std::min(lim, table->GetTupleCount() - off) : 0;
    }

    // Compile.
    auto query = CompilationContext::Compile(*order_by);

    // Run and check.
    ExecuteAndCheckInAllModes(query.get(), [&]() {
      // Checkers:
      // 1. col2 should contain rows in range [offset, offset+lim].
      // 2. col2 should be sorted by col2 ASC.
      // 3. The total number of rows should depend on offset and limit.
      std::vector<std::unique_ptr<OutputChecker>> checks;
      checks.push_back(std::make_unique<TupleCounterChecker>(expected_tuple_count));
      checks.push_back(std::make_unique<SingleIntSortChecker>(1));
      checks.push_back(std::make_unique<GenericChecker>(
          [&](const std::vector<const sql::Val *> &row) {
            const auto col2 = static_cast<const sql::Integer *>(row[1]);
            EXPECT_GE(col2->val, off);
            if (lim != 0) {
              ASSERT_LT(col2->val, off + lim);
            }
          },
          nullptr));
      return std::make_unique<MultiChecker>(std::move(checks));
    });
  }
};

TEST_F(SortTranslatorTest, SimpleSortTest) {
  // SELECT col1, col2 FROM test_1 WHERE col1 < 500 ORDER BY col2 ASC

  // Get accessor
  auto accessor = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;
  sql::Table *table = accessor->LookupTableByName("test_1");
  const auto &table_schema = table->GetSchema();
  std::unique_ptr<planner::AbstractPlanNode> seq_scan;
  planner::OutputSchemaHelper seq_scan_out{&expr_maker, 0};
  {
    // Get Table columns
    auto col1 = expr_maker.CVE(table_schema.GetColumnInfo("colA").oid, sql::TypeId::Integer);
    auto col2 = expr_maker.CVE(table_schema.GetColumnInfo("colB").oid, sql::TypeId::Integer);
    seq_scan_out.AddOutput("col1", col1);
    seq_scan_out.AddOutput("col2", col2);
    auto schema = seq_scan_out.MakeSchema();
    // Make predicate
    auto predicate = expr_maker.CompareLt(col1, expr_maker.Constant(500));
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(std::move(schema))
                   .SetScanPredicate(predicate)
                   .SetTableOid(table->GetId())
                   .Build();
  }
  // Order By
  std::unique_ptr<planner::AbstractPlanNode> order_by;
  planner::OutputSchemaHelper order_by_out{&expr_maker, 0};
  {
    // Output Colums col1, col2
    auto col1 = seq_scan_out.GetOutput("col1");
    auto col2 = seq_scan_out.GetOutput("col2");
    order_by_out.AddOutput("col1", col1);
    order_by_out.AddOutput("col2", col2);
    auto schema = order_by_out.MakeSchema();
    // Build
    planner::OrderByPlanNode::Builder builder;
    order_by = builder.SetOutputSchema(std::move(schema))
                   .AddChild(std::move(seq_scan))
                   .AddSortKey(col2, planner::OrderByOrderingType::ASC)
                   .Build();
  }

  // Compile.
  auto query = CompilationContext::Compile(*order_by);

  // Run and check.
  ExecuteAndCheckInAllModes(query.get(), [&]() {
    // Checkers:
    // 1. All 'col1' should be less than 500 due to the filter.
    // 2. The output should be sorted by col2 ASC.
    std::vector<std::unique_ptr<OutputChecker>> checks;
    checks.push_back(std::make_unique<SingleColumnValueChecker<Integer>>(std::less<>(), 0, 500));
    checks.push_back(std::make_unique<SingleIntSortChecker>(1));
    return std::make_unique<MultiChecker>(std::move(checks));
  });
}

TEST_F(SortTranslatorTest, TwoColumnSortTest) {
  // SELECT col1, col2, col1 + col2 FROM test_1 WHERE col1 < 500 ORDER BY col2 ASC, col1 - col2 DESC

  // Get accessor
  auto accessor = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;
  sql::Table *table = accessor->LookupTableByName("test_1");
  const auto &table_schema = table->GetSchema();
  std::unique_ptr<planner::AbstractPlanNode> seq_scan;
  planner::OutputSchemaHelper seq_scan_out{&expr_maker, 0};
  {
    // Get Table columns
    auto col1 = expr_maker.CVE(table_schema.GetColumnInfo("colA").oid, sql::TypeId::Integer);
    auto col2 = expr_maker.CVE(table_schema.GetColumnInfo("colB").oid, sql::TypeId::Integer);
    seq_scan_out.AddOutput("col1", col1);
    seq_scan_out.AddOutput("col2", col2);
    auto schema = seq_scan_out.MakeSchema();
    // Make predicate
    auto predicate = expr_maker.CompareLt(col1, expr_maker.Constant(500));
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(std::move(schema))
                   .SetScanPredicate(predicate)
                   .SetTableOid(table->GetId())
                   .Build();
  }
  // Order By
  std::unique_ptr<planner::AbstractPlanNode> order_by;
  planner::OutputSchemaHelper order_by_out{&expr_maker, 0};
  {
    // Output Colums col1, col2, col1 + col2
    auto col1 = seq_scan_out.GetOutput("col1");
    auto col2 = seq_scan_out.GetOutput("col2");
    auto sum = expr_maker.OpSum(col1, col2);
    order_by_out.AddOutput("col1", col1);
    order_by_out.AddOutput("col2", col2);
    order_by_out.AddOutput("sum", sum);
    auto schema = order_by_out.MakeSchema();
    // Order By Clause
    planner::SortKey clause1{col2, planner::OrderByOrderingType::ASC};
    auto diff = expr_maker.OpMin(col1, col2);
    planner::SortKey clause2{diff, planner::OrderByOrderingType::DESC};
    // Build
    planner::OrderByPlanNode::Builder builder;
    order_by = builder.SetOutputSchema(std::move(schema))
                   .AddChild(std::move(seq_scan))
                   .AddSortKey(clause1.first, clause1.second)
                   .AddSortKey(clause2.first, clause2.second)
                   .Build();
  }

  // Compile.
  auto query = CompilationContext::Compile(*order_by);

  // Run and check.
  ExecuteAndCheckInAllModes(query.get(), [&]() {
    // Checkers:
    // There should be 500 output rows, where col1 < 500.
    // The output should be sorted by col2 ASC
    std::vector<std::unique_ptr<OutputChecker>> checks;
    checks.push_back(std::make_unique<TupleCounterChecker>(500));
    checks.push_back(std::make_unique<SingleColumnValueChecker<Integer>>(std::less<>(), 0, 500));
    checks.push_back(std::make_unique<GenericChecker>(
        [curr_col1 = std::numeric_limits<int64_t>::max(),
         curr_col2 = std::numeric_limits<int64_t>::min()](
            const std::vector<const sql::Val *> &vals) mutable {
          auto col1 = static_cast<const sql::Integer *>(vals[0]);
          auto col2 = static_cast<const sql::Integer *>(vals[1]);
          // Neither output is NULL.
          ASSERT_FALSE(col1->is_null);
          ASSERT_FALSE(col2->is_null);
          // Check sort order: col2 ASC, col1 DESC
          ASSERT_LE(curr_col2, col2->val);
          if (curr_col2 == col2->val) {
            ASSERT_GE(curr_col1 - curr_col2, col1->val - col2->val);
          }
          curr_col1 = col1->val;
          curr_col2 = col2->val;
        },
        nullptr));
    return std::make_unique<MultiChecker>(std::move(checks));
  });
}

TEST_F(SortTranslatorTest, SortWithLimitAndOffsetTest) {
  TestSortWithLimitAndOrOffset(0, 1);
  TestSortWithLimitAndOrOffset(0, 10);
  TestSortWithLimitAndOrOffset(10, 0);
  TestSortWithLimitAndOrOffset(100, 0);
  TestSortWithLimitAndOrOffset(50000000, 0);
  TestSortWithLimitAndOrOffset(50000000, 50000000);
}

}  // namespace tpl::sql::codegen
