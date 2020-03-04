#include "util/sql_test_harness.h"

#include <sql/planner/plannodes/limit_plan_node.h>
#include <memory>

#include "tbb/tbb.h"

#include "sql/catalog.h"
#include "sql/codegen/compilation_context.h"
#include "sql/execution_context.h"
#include "sql/planner/plannodes/aggregate_plan_node.h"
#include "sql/planner/plannodes/hash_join_plan_node.h"
#include "sql/planner/plannodes/nested_loop_join_plan_node.h"
#include "sql/planner/plannodes/order_by_plan_node.h"
#include "sql/planner/plannodes/seq_scan_plan_node.h"
#include "sql/printing_consumer.h"
#include "sql/schema.h"
#include "sql/table.h"

#include "vm/llvm_engine.h"

// Tests
#include "sql/codegen/output_checker.h"
#include "sql/planner/expression_maker.h"
#include "sql/planner/output_schema_util.h"

namespace tpl::sql::codegen {

using namespace std::chrono_literals;

class SeqScanTranslatorTest : public SqlBasedTest {
 protected:
  void SetUp() override { SqlBasedTest::SetUp(); }
  static void SetUpTestSuite() { tpl::vm::LLVMEngine::Initialize(); }
  static void TearDownTestSuite() { tpl::vm::LLVMEngine::Shutdown(); }

 private:
  tbb::task_scheduler_init anonymous_;
};

TEST_F(SeqScanTranslatorTest, SimpleSeqScanTest) {
  // SELECT col1, col2, col1 * col2, col1 >= 100*col2 FROM test_1 WHERE col1 < 500 AND col2 >= 5;
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
    // Make New Column
    auto col3 = expr_maker.OpMul(col1, col2);
    auto col4 = expr_maker.CompareGe(col1, expr_maker.OpMul(expr_maker.Constant(100), col2));
    seq_scan_out.AddOutput("col1", col1);
    seq_scan_out.AddOutput("col2", col2);
    seq_scan_out.AddOutput("col3", col3);
    seq_scan_out.AddOutput("col4", col4);
    auto schema = seq_scan_out.MakeSchema();
    // Make predicate
    auto comp1 = expr_maker.CompareLt(col1, expr_maker.Constant(500));
    auto comp2 = expr_maker.CompareGe(col2, expr_maker.Constant(5));
    auto predicate = expr_maker.ConjunctionAnd(comp1, comp2);
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(std::move(schema))
                   .SetScanPredicate(predicate)
                   .SetTableOid(table->GetId())
                   .Build();
  }

  auto last = seq_scan.get();

  // Make the output checkers
  SingleIntComparisonChecker col1_checker(std::less<int64_t>(), 0, 500);
  SingleIntComparisonChecker col2_checker(std::greater_equal<int64_t>(), 1, 5);
  MultiChecker multi_checker({&col1_checker, &col2_checker});

  // Create the execution context
  OutputCollectorAndChecker store(&multi_checker, last->GetOutputSchema());
  MultiOutputCallback callback({&store});
  sql::MemoryPool memory(nullptr);
  sql::ExecutionContext exec_ctx(&memory, last->GetOutputSchema(), &callback);
  // Run & Check
  auto query = CompilationContext::Compile(*last);
  query->Run(&exec_ctx);

  multi_checker.CheckCorrectness();
}

TEST_F(SeqScanTranslatorTest, NonVecFilterTest) {
  // SELECT col1, col2, col1 * col2, col1 >= 100*col2 FROM test_1
  // WHERE (col1 < 500 AND col2 >= 5) OR (500 <= col1 <= 1000 AND (col2 = 3 OR col2 = 7));
  // The filter is not in DNF form and can't be vectorized.
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
    // Make New Column
    auto col3 = expr_maker.OpMul(col1, col2);
    auto col4 = expr_maker.CompareGe(col1, expr_maker.OpMul(expr_maker.Constant(100), col2));
    seq_scan_out.AddOutput("col1", col1);
    seq_scan_out.AddOutput("col2", col2);
    seq_scan_out.AddOutput("col3", col3);
    seq_scan_out.AddOutput("col4", col4);
    auto schema = seq_scan_out.MakeSchema();
    // Make predicate
    auto comp1 = expr_maker.CompareLt(col1, expr_maker.Constant(500));
    auto comp2 = expr_maker.CompareGe(col2, expr_maker.Constant(5));
    auto clause1 = expr_maker.ConjunctionAnd(comp1, comp2);
    auto comp3 = expr_maker.CompareGe(col1, expr_maker.Constant(500));
    auto comp4 = expr_maker.CompareLt(col1, expr_maker.Constant(1000));
    auto comp5 = expr_maker.CompareEq(col2, expr_maker.Constant(3));
    auto comp6 = expr_maker.CompareEq(col2, expr_maker.Constant(7));
    auto clause2 = expr_maker.ConjunctionAnd(expr_maker.ConjunctionAnd(comp3, comp4),
                                             expr_maker.ConjunctionOr(comp5, comp6));
    auto predicate = expr_maker.ConjunctionOr(clause1, clause2);
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(std::move(schema))
                   .SetScanPredicate(predicate)
                   .SetTableOid(table->GetId())
                   .Build();
  }
  auto last = seq_scan.get();
  // Make the output checkers
  // Make the checkers
  uint32_t num_output_rows = 0;
  RowChecker row_checker = [&num_output_rows](const std::vector<const sql::Val *> vals) {
    // Read cols
    auto col1 = static_cast<const sql::Integer *>(vals[0]);
    auto col2 = static_cast<const sql::Integer *>(vals[1]);
    ASSERT_FALSE(col1->is_null || col2->is_null);
    // Check predicate
    ASSERT_TRUE((col1->val < 500 && col2->val >= 5) ||
                (col1->val >= 500 && col1->val < 1000 && (col2->val == 7 || col2->val == 3)));
    num_output_rows++;
  };
  GenericChecker checker(row_checker, {});

  // Create the execution context
  OutputCollectorAndChecker store(&checker, last->GetOutputSchema());
  MultiOutputCallback callback({&store});
  sql::MemoryPool memory(nullptr);
  sql::ExecutionContext exec_ctx(&memory, last->GetOutputSchema(), &callback);

  // Run & Check
  auto query = CompilationContext::Compile(*last);
  query->Run(&exec_ctx);

  checker.CheckCorrectness();
}

TEST_F(SeqScanTranslatorTest, SimpleAggregateTest) {
  // SELECT col2, SUM(col1) FROM test_1 WHERE col1 < 1000 GROUP BY col2;
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
    auto predicate = expr_maker.CompareLt(col1, expr_maker.Constant(1000));
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(std::move(schema))
                   .SetScanPredicate(predicate)
                   .SetTableOid(table->GetId())
                   .Build();
  }
  // Make the aggregate
  std::unique_ptr<planner::AbstractPlanNode> agg;
  planner::OutputSchemaHelper agg_out{&expr_maker, 0};
  {
    // Read previous output
    auto col1 = seq_scan_out.GetOutput("col1");
    auto col2 = seq_scan_out.GetOutput("col2");
    // Add group by term
    agg_out.AddGroupByTerm("col2", col2);
    // Add aggregates
    agg_out.AddAggTerm("sum_col1", expr_maker.AggSum(col1));
    // Make the output expressions
    agg_out.AddOutput("col2", agg_out.GetGroupByTermForOutput("col2"));
    agg_out.AddOutput("sum_col1", agg_out.GetAggTermForOutput("sum_col1"));
    auto schema = agg_out.MakeSchema();
    // Build
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(std::move(schema))
              .AddGroupByTerm(agg_out.GetGroupByTerm("col2"))
              .AddAggregateTerm(agg_out.GetAggTerm("col1"))
              .AddChild(std::move(seq_scan))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }
  auto last = agg.get();
  // Make the checkers
  TupleCounterChecker num_checker(10);
  SingleIntSumChecker sum_checker(1, (1000 * 999) / 2);
  MultiChecker multi_checker({&num_checker, &sum_checker});

  // Compile and Run
  OutputCollectorAndChecker store(&multi_checker, agg->GetOutputSchema());
  MultiOutputCallback callback({&store});
  sql::MemoryPool memory(nullptr);
  sql::ExecutionContext exec_ctx(&memory, agg->GetOutputSchema(), &callback);

  // Run & Check
  auto query = CompilationContext::Compile(*last);
  query->Run(&exec_ctx);

  multi_checker.CheckCorrectness();
}

TEST_F(SeqScanTranslatorTest, SimpleSortTest) {
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
  auto last = order_by.get();

  // Checkers:
  // There should be 500 output rows, where col1 < 500.
  // The output should be sorted by col2 ASC
  SingleIntComparisonChecker col1_checker([](auto a, auto b) { return a < b; }, 0, 500);
  SingleIntSortChecker col2_sort_checker(1);
  MultiChecker multi_checker({&col2_sort_checker, &col1_checker});

  OutputCollectorAndChecker store(&multi_checker, order_by->GetOutputSchema());
  MultiOutputCallback callback({&store});
  sql::MemoryPool memory(nullptr);
  sql::ExecutionContext exec_ctx(&memory, order_by->GetOutputSchema(), &callback);

  // Run & Check
  auto query = CompilationContext::Compile(*last);
  query->Run(&exec_ctx);

  multi_checker.CheckCorrectness();
}

TEST_F(SeqScanTranslatorTest, TwoColumnSortTest) {
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
  auto last = order_by.get();
  // Checkers:
  // There should be 500 output rows, where col1 < 500.
  // The output should be sorted by col2 ASC, then col1 DESC.
  uint32_t num_output_rows{0};
  uint32_t num_expected_rows{500};
  int64_t curr_col1{std::numeric_limits<int64_t>::max()};
  int64_t curr_col2{std::numeric_limits<int64_t>::min()};
  RowChecker row_checker = [&num_output_rows, &curr_col1, &curr_col2,
                            num_expected_rows](const std::vector<const sql::Val *> vals) {
    // Read cols
    auto col1 = static_cast<const sql::Integer *>(vals[0]);
    auto col2 = static_cast<const sql::Integer *>(vals[1]);
    ASSERT_FALSE(col1->is_null || col2->is_null);
    // Check col1 and number of outputs
    ASSERT_LT(col1->val, 500);
    num_output_rows++;
    ASSERT_LE(num_output_rows, num_expected_rows);
    // Check that output is sorted by col2 ASC, then col1 DESC
    ASSERT_LE(curr_col2, col2->val);
    if (curr_col2 == col2->val) {
      ASSERT_GE(curr_col1, col1->val);
    }
    curr_col1 = col1->val;
    curr_col2 = col2->val;
  };
  CorrectnessFn correcteness_fn = [&num_output_rows, num_expected_rows]() {
    ASSERT_EQ(num_output_rows, num_expected_rows);
  };
  GenericChecker checker(row_checker, correcteness_fn);
  OutputCollectorAndChecker store(&checker, order_by->GetOutputSchema());
  MultiOutputCallback callback({&store});
  sql::MemoryPool memory(nullptr);
  sql::ExecutionContext exec_ctx(&memory, order_by->GetOutputSchema(), &callback);

  // Run & Check
  auto query = CompilationContext::Compile(*last);
  query->Run(&exec_ctx);

  checker.CheckCorrectness();
}

namespace {

void TestLimitAndOrOffset(uint64_t off, uint64_t lim) {
  // SELECT col1, col2 FROM small_1 OFFSET off LIMIT lim;
  // small_1.col2 is serial, we use that to check offset/limit.

  auto accessor = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;
  sql::Table *table = accessor->LookupTableByName("small_1");
  const auto &table_schema = table->GetSchema();

  // Scan.
  std::unique_ptr<planner::AbstractPlanNode> seq_scan;
  planner::OutputSchemaHelper seq_scan_out(&expr_maker, 0);
  {
    auto col1 = expr_maker.CVE(table_schema.GetColumnInfo("col1").oid, sql::TypeId::Integer);
    auto col2 = expr_maker.CVE(table_schema.GetColumnInfo("col2").oid, sql::TypeId::Integer);
    seq_scan_out.AddOutput("col1", col1);
    seq_scan_out.AddOutput("col2", col2);
    auto schema = seq_scan_out.MakeSchema();
    planner::SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(std::move(schema))
                   .SetScanPredicate(nullptr)
                   .SetTableOid(table->GetId())
                   .Build();
  }

  // Limit.
  std::unique_ptr<planner::AbstractPlanNode> limit;
  planner::OutputSchemaHelper limit_out(&expr_maker, 0);
  {
    // Read previous output
    auto col1 = seq_scan_out.GetOutput("col1");
    auto col2 = seq_scan_out.GetOutput("col2");
    // Make the output expressions
    limit_out.AddOutput("col1", col1);
    limit_out.AddOutput("col2", col2);
    auto schema = limit_out.MakeSchema();
    // Build
    planner::LimitPlanNode::Builder builder;
    limit = planner::LimitPlanNode::Builder()
                .SetLimit(lim)
                .SetOffset(off)
                .SetOutputSchema(std::move(schema))
                .AddChild(std::move(seq_scan))
                .Build();
  }

  // Make the checkers
  GenericChecker row_check(
      [&](const std::vector<const sql::Val *> &row) {
        // col2 is monotonically increasing from 0. So, the values we get back
        // should match: off <= col2 < off+lim.
        const auto col2 = static_cast<const sql::Integer *>(row[1]);
        EXPECT_GE(col2->val, off);
        EXPECT_LT(col2->val, off + lim);
      },
      nullptr);
  TupleCounterChecker num_checker(std::min(static_cast<uint64_t>(table->GetTupleCount()), lim));
  MultiChecker multi_checker({&num_checker, &row_check});

  // Compile and Run
  OutputCollectorAndChecker store(&multi_checker, limit->GetOutputSchema());
  MultiOutputCallback callback({&store});
  sql::MemoryPool memory(nullptr);
  sql::ExecutionContext exec_ctx(&memory, limit->GetOutputSchema(), &callback);

  // Run & Check
  auto query = CompilationContext::Compile(*limit);
  query->Run(&exec_ctx);

  multi_checker.CheckCorrectness();
}

}  // namespace

TEST_F(SeqScanTranslatorTest, LimitAndOffsetTest) {
  // We don't test zero-limits because those should've been optimized out before
  // ever getting to query execution.
  TestLimitAndOrOffset(0, 1);
  TestLimitAndOrOffset(0, 10);
  TestLimitAndOrOffset(10, 20);
  TestLimitAndOrOffset(10, 1);
  TestLimitAndOrOffset(50, 20);
}

}  // namespace tpl::sql::codegen
