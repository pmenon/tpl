#include "util/sql_test_harness.h"

#include <memory>

#include "sql/catalog.h"
#include "sql/planner/plannodes/aggregate_plan_node.h"
#include "sql/planner/plannodes/projection_plan_node.h"
#include "sql/planner/plannodes/seq_scan_plan_node.h"
#include "sql/printing_consumer.h"
#include "sql/schema.h"
#include "sql/table.h"

// Tests
#include "sql/planner/expression_maker.h"
#include "sql/planner/output_schema_util.h"
#include "util/codegen_test_harness.h"

namespace tpl::sql::codegen {

using namespace std::chrono_literals;

class StaticAggregationTranslatorTest : public CodegenBasedTest {};

TEST_F(StaticAggregationTranslatorTest, SimpleTest) {
  // SELECT COUNT(*), SUM(col2) FROM small_1;

  auto accessor = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;
  auto table = accessor->LookupTableByName("small_1");
  auto &table_schema = table->GetSchema();
  std::unique_ptr<planner::AbstractPlanNode> seq_scan;
  planner::OutputSchemaHelper seq_scan_out(&expr_maker, 0);
  {
    auto col2 = expr_maker.CVE(table_schema.GetColumnInfo("col2"));
    seq_scan_out.AddOutput("col2", col2);
    auto schema = seq_scan_out.MakeSchema();
    // Build
    seq_scan = planner::SeqScanPlanNode::Builder()
                   .SetOutputSchema(std::move(schema))
                   .SetScanPredicate(nullptr)
                   .SetTableOid(table->GetId())
                   .Build();
  }

  // Make the aggregate
  std::unique_ptr<planner::AbstractPlanNode> agg;
  planner::OutputSchemaHelper agg_out(&expr_maker, 0);
  {
    // Read previous output
    auto col2 = seq_scan_out.GetOutput("col2");
    // Add aggregates
    agg_out.AddAggTerm("count_star", expr_maker.AggCountStar());
    agg_out.AddAggTerm("sum_col2", expr_maker.AggSum(col2));
    // Make the output expressions
    agg_out.AddOutput("count_star", agg_out.GetAggTermForOutput("count_star"));
    agg_out.AddOutput("sum_col2", agg_out.GetAggTermForOutput("sum_col2"));
    auto schema = agg_out.MakeSchema();
    // Build
    agg = planner::AggregatePlanNode::Builder()
              .SetOutputSchema(std::move(schema))
              .AddAggregateTerm(agg_out.GetAggTerm("count_star"))
              .AddAggregateTerm(agg_out.GetAggTerm("sum_col2"))
              .AddChild(std::move(seq_scan))
              .SetAggregateStrategyType(planner::AggregateStrategyType::PLAIN)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }

  // Run and check.
  ExecuteAndCheckInAllModes(*agg, [&]() {
    const uint64_t ntuples = table->GetTupleCount();
    // Checks:
    // 1. There should only be one output.
    // 2. The count should be equal to the number of tuples in the table
    //    since all values are non-NULL.
    // 3. The sum should be equal to sum(1,N) where N=num_tuples since
    //    col2 is a monotonically increasing column.
    std::vector<std::unique_ptr<OutputChecker>> checks;
    checks.emplace_back(std::make_unique<TupleCounterChecker>(1));
    checks.emplace_back(
        std::make_unique<SingleColumnValueChecker<Integer>>(std::equal_to<>(), 0, ntuples));
    checks.emplace_back(std::make_unique<SingleIntSumChecker>(1, ntuples * (ntuples - 1) / 2));
    return std::make_unique<MultiChecker>(std::move(checks));
  });
}

TEST_F(StaticAggregationTranslatorTest, StaticAggregateWithHavingTest) {
  // SELECT COUNT(*) FROM small_1 HAVING COUNT(*) < 0;

  // Make the sequential scan.
  auto accessor = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;
  auto table = accessor->LookupTableByName("small_1");
  std::unique_ptr<planner::AbstractPlanNode> seq_scan;
  planner::OutputSchemaHelper seq_scan_out(&expr_maker, 0);
  {
    auto schema = seq_scan_out.MakeSchema();
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(std::move(schema))
                   .SetScanPredicate(nullptr)
                   .SetTableOid(table->GetId())
                   .Build();
  }

  // Make the aggregate.
  std::unique_ptr<planner::AbstractPlanNode> agg;
  planner::OutputSchemaHelper agg_out(&expr_maker, 0);
  {
    // Add aggregates.
    agg_out.AddAggTerm("count_star", expr_maker.AggCountStar());
    // Make the output expressions.
    agg_out.AddOutput("count_star", agg_out.GetAggTermForOutput("count_star"));
    // Having predicate.
    auto having =
        expr_maker.CompareLt(agg_out.GetAggTermForOutput("count_star"), expr_maker.Constant(0));
    // Build
    auto schema = agg_out.MakeSchema();
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(std::move(schema))
              .AddAggregateTerm(agg_out.GetAggTerm("count_star"))
              .AddChild(std::move(seq_scan))
              .SetAggregateStrategyType(planner::AggregateStrategyType::PLAIN)
              .SetHavingClausePredicate(having)
              .Build();
  }

  // Run and check.
  ExecuteAndCheckInAllModes(*agg, [&]() {
    // Checks:
    // 1. Should not output anything since the count is greater than 0.
    std::vector<std::unique_ptr<OutputChecker>> checks;
    checks.emplace_back(std::make_unique<TupleCounterChecker>(0));
    return std::make_unique<MultiChecker>(std::move(checks));
  });
}

}  // namespace tpl::sql::codegen
