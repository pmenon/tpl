#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"

#include "compiler/compiler.h"
#include "sema/sema.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/executable_query.h"
#include "sql/codegen/output_checker.h"
#include "sql/execution_context.h"
#include "sql/planner/expression_maker.h"
#include "sql/planner/output_schema_util.h"
#include "sql/planner/plannodes/aggregate_plan_node.h"
#include "sql/planner/plannodes/hash_join_plan_node.h"
#include "sql/planner/plannodes/nested_loop_join_plan_node.h"
#include "sql/planner/plannodes/order_by_plan_node.h"
#include "sql/planner/plannodes/output_schema.h"
#include "sql/planner/plannodes/projection_plan_node.h"
#include "sql/planner/plannodes/seq_scan_plan_node.h"
#include "sql/printing_consumer.h"
#include "sql/table.h"
#include "sql/tablegen/table_generator.h"
#include "vm/bytecode_generator.h"
#include "vm/bytecode_module.h"
#include "vm/llvm_engine.h"
#include "vm/module.h"

namespace tpl::sql::codegen {

namespace {

// The execution mode.
constexpr auto kExecutionMode = vm::ExecutionMode::Interpret;

// Change this path to where your TPC-H data is.
constexpr char kTpchDataDir[] = "/home/pmenon/tools/TPC-H/data/sf-0.1";

// Flag used to ensure the TPCH database is only loaded once.
std::once_flag kLoadTpchDatabaseOnce{};

}  // namespace

class TpchBenchmark : public benchmark::Fixture {
 public:
  void SetUp(benchmark::State &st) override {
    Fixture::SetUp(st);
    std::call_once(kLoadTpchDatabaseOnce, []() {
      tablegen::TableGenerator::GenerateTPCHTables(Catalog::Instance(), kTpchDataDir);
    });
  }
};

BENCHMARK_DEFINE_F(TpchBenchmark, Q1)(benchmark::State &state) {
  auto accessor = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;
  sql::Table *l_table = accessor->LookupTableByName("tpch.lineitem");
  const auto &l_schema = l_table->GetSchema();
  // Scan the table
  std::unique_ptr<planner::AbstractPlanNode> l_seq_scan;
  planner::OutputSchemaHelper l_seq_scan_out{&expr_maker, 0};
  {
    // Read all needed columns
    auto l_returnflag = expr_maker.CVE(l_schema.GetColumnInfo("l_returnflag"));
    auto l_linestatus = expr_maker.CVE(l_schema.GetColumnInfo("l_linestatus"));
    auto l_extendedprice = expr_maker.CVE(l_schema.GetColumnInfo("l_extendedprice"));
    auto l_discount = expr_maker.CVE(l_schema.GetColumnInfo("l_discount"));
    auto l_tax = expr_maker.CVE(l_schema.GetColumnInfo("l_tax"));
    auto l_quantity = expr_maker.CVE(l_schema.GetColumnInfo("l_quantity"));
    auto l_shipdate = expr_maker.CVE(l_schema.GetColumnInfo("l_shipdate"));
    // Make the output schema
    l_seq_scan_out.AddOutput("l_returnflag", l_returnflag);
    l_seq_scan_out.AddOutput("l_linestatus", l_linestatus);
    l_seq_scan_out.AddOutput("l_extendedprice", l_extendedprice);
    l_seq_scan_out.AddOutput("l_discount", l_discount);
    l_seq_scan_out.AddOutput("l_tax", l_tax);
    l_seq_scan_out.AddOutput("l_quantity", l_quantity);
    auto schema = l_seq_scan_out.MakeSchema();
    // Make the predicate
    l_seq_scan_out.AddOutput("l_shipdate", l_shipdate);
    auto date_const = expr_maker.Constant(1998, 9, 2);
    auto predicate = expr_maker.CompareLt(l_shipdate, date_const);
    // Build
    planner::SeqScanPlanNode::Builder builder;
    l_seq_scan = builder.SetOutputSchema(std::move(schema))
                     .SetScanPredicate(predicate)
                     .SetTableOid(l_table->GetId())
                     .Build();
  }
  // Make the aggregate
  std::unique_ptr<planner::AbstractPlanNode> agg;
  planner::OutputSchemaHelper agg_out{&expr_maker, 0};
  {
    // Read previous layer's output
    auto l_returnflag = l_seq_scan_out.GetOutput("l_returnflag");
    auto l_linestatus = l_seq_scan_out.GetOutput("l_linestatus");
    auto l_quantity = l_seq_scan_out.GetOutput("l_quantity");
    auto l_extendedprice = l_seq_scan_out.GetOutput("l_extendedprice");
    auto l_discount = l_seq_scan_out.GetOutput("l_discount");
    auto l_tax = l_seq_scan_out.GetOutput("l_tax");
    // Make the aggregate expressions
    auto sum_qty = expr_maker.AggSum(l_quantity);
    auto sum_base_price = expr_maker.AggSum(l_extendedprice);
    auto one_const = expr_maker.Constant(1.0f);
    auto disc_price = expr_maker.OpMul(l_extendedprice, expr_maker.OpMin(one_const, l_discount));
    auto sum_disc_price = expr_maker.AggSum(disc_price);
    auto charge = expr_maker.OpMul(disc_price, expr_maker.OpSum(one_const, l_tax));
    auto sum_charge = expr_maker.AggSum(charge);
    auto avg_qty = expr_maker.AggAvg(l_quantity);
    auto avg_price = expr_maker.AggAvg(l_extendedprice);
    auto avg_disc = expr_maker.AggAvg(l_discount);
    auto count_order = expr_maker.AggCount(expr_maker.Constant(1));  // Works as Count(*)
    // Add them to the helper.
    agg_out.AddGroupByTerm("l_returnflag", l_returnflag);
    agg_out.AddGroupByTerm("l_linestatus", l_linestatus);
    agg_out.AddAggTerm("sum_qty", sum_qty);
    agg_out.AddAggTerm("sum_base_price", sum_base_price);
    agg_out.AddAggTerm("sum_disc_price", sum_disc_price);
    agg_out.AddAggTerm("sum_charge", sum_charge);
    agg_out.AddAggTerm("avg_qty", avg_qty);
    agg_out.AddAggTerm("avg_price", avg_price);
    agg_out.AddAggTerm("avg_disc", avg_disc);
    agg_out.AddAggTerm("count_order", count_order);
    // Make the output schema
    agg_out.AddOutput("l_returnflag", agg_out.GetGroupByTermForOutput("l_returnflag"));
    agg_out.AddOutput("l_linestatus", agg_out.GetGroupByTermForOutput("l_linestatus"));
    agg_out.AddOutput("sum_qty", agg_out.GetAggTermForOutput("sum_qty"));
    agg_out.AddOutput("sum_base_price", agg_out.GetAggTermForOutput("sum_base_price"));
    agg_out.AddOutput("sum_disc_price", agg_out.GetAggTermForOutput("sum_disc_price"));
    agg_out.AddOutput("sum_charge", agg_out.GetAggTermForOutput("sum_charge"));
    agg_out.AddOutput("avg_qty", agg_out.GetAggTermForOutput("avg_qty"));
    agg_out.AddOutput("avg_price", agg_out.GetAggTermForOutput("avg_price"));
    agg_out.AddOutput("avg_disc", agg_out.GetAggTermForOutput("avg_disc"));
    agg_out.AddOutput("count_order", agg_out.GetAggTermForOutput("count_order"));
    auto schema = agg_out.MakeSchema();
    // Build
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(std::move(schema))
              .AddGroupByTerm(l_returnflag)
              .AddGroupByTerm(l_linestatus)
              .AddAggregateTerm(sum_qty)
              .AddAggregateTerm(sum_base_price)
              .AddAggregateTerm(sum_disc_price)
              .AddAggregateTerm(sum_charge)
              .AddAggregateTerm(avg_qty)
              .AddAggregateTerm(avg_price)
              .AddAggregateTerm(avg_disc)
              .AddAggregateTerm(count_order)
              .AddChild(std::move(l_seq_scan))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }
  // Order By
  std::unique_ptr<planner::AbstractPlanNode> order_by;
  planner::OutputSchemaHelper order_by_out{&expr_maker, 0};
  {
    // Output Colums col1, col2, col1 + col2
    auto l_returnflag = agg_out.GetOutput("l_returnflag");
    auto l_linestatus = agg_out.GetOutput("l_linestatus");
    auto sum_qty = agg_out.GetOutput("sum_qty");
    auto sum_base_price = agg_out.GetOutput("sum_base_price");
    auto sum_disc_price = agg_out.GetOutput("sum_disc_price");
    auto sum_charge = agg_out.GetOutput("sum_charge");
    auto avg_qty = agg_out.GetOutput("avg_qty");
    auto avg_price = agg_out.GetOutput("avg_price");
    auto avg_disc = agg_out.GetOutput("avg_disc");
    auto count_order = agg_out.GetOutput("count_order");
    order_by_out.AddOutput("l_returnflag", l_returnflag);
    order_by_out.AddOutput("l_linestatus", l_linestatus);
    order_by_out.AddOutput("sum_qty", sum_qty);
    order_by_out.AddOutput("sum_base_price", sum_base_price);
    order_by_out.AddOutput("sum_disc_price", sum_disc_price);
    order_by_out.AddOutput("sum_charge", sum_charge);
    order_by_out.AddOutput("avg_qty", avg_qty);
    order_by_out.AddOutput("avg_price", avg_price);
    order_by_out.AddOutput("avg_disc", avg_disc);
    order_by_out.AddOutput("count_order", count_order);
    auto schema = order_by_out.MakeSchema();
    // Order By Clause
    planner::SortKey clause1{l_returnflag, planner::OrderByOrderingType::ASC};
    planner::SortKey clause2{l_linestatus, planner::OrderByOrderingType::ASC};
    // Build
    planner::OrderByPlanNode::Builder builder;
    order_by = builder.SetOutputSchema(std::move(schema))
                   .AddChild(std::move(agg))
                   .AddSortKey(clause1.first, clause1.second)
                   .AddSortKey(clause2.first, clause2.second)
                   .Build();
  }
  // Compile plan
  auto last_op = order_by.get();
  NoOpResultConsumer consumer;
  sql::MemoryPool memory(nullptr);
  sql::ExecutionContext exec_ctx(&memory, last_op->GetOutputSchema(), &consumer);
  auto query = CompilationContext::Compile(*last_op);
  // Run Once to force compilation
  query->Run(&exec_ctx, kExecutionMode);

  // Only time execution
  for (auto _ : state) {
    query->Run(&exec_ctx, kExecutionMode);
  }
}

BENCHMARK_DEFINE_F(TpchBenchmark, Q3)(benchmark::State &state) {
  auto accessor = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;

  // Scan customer.
  std::unique_ptr<planner::AbstractScanPlanNode> cust_seq_scan;
  planner::OutputSchemaHelper cust_seq_scan_out(&expr_maker, 0);
  {
    sql::Table *table = accessor->LookupTableByName("tpch.customer");
    const auto &schema = table->GetSchema();

    // Read all needed columns.
    auto c_custkey = expr_maker.CVE(schema.GetColumnInfo("c_custkey"));
    auto c_mktsegment = expr_maker.CVE(schema.GetColumnInfo("c_mktsegment"));
    // Make the output schema.
    cust_seq_scan_out.AddOutput("c_custkey", c_custkey);
    // Predicate.
    auto predicate = expr_maker.CompareEq(c_mktsegment, expr_maker.Constant("BUILDING"));
    // Build.
    cust_seq_scan = planner::SeqScanPlanNode::Builder{}
                        .SetOutputSchema(cust_seq_scan_out.MakeSchema())
                        .SetScanPredicate(predicate)
                        .SetTableOid(table->GetId())
                        .Build();
  }

  // Scan orders.
  std::unique_ptr<planner::AbstractScanPlanNode> o_seq_scan;
  planner::OutputSchemaHelper o_seq_scan_out(&expr_maker, 1);
  {
    sql::Table *table = accessor->LookupTableByName("tpch.orders");
    const auto &schema = table->GetSchema();

    // Read all needed columns.
    auto o_custkey = expr_maker.CVE(schema.GetColumnInfo("o_custkey"));
    auto o_orderkey = expr_maker.CVE(schema.GetColumnInfo("o_orderkey"));
    auto o_orderdate = expr_maker.CVE(schema.GetColumnInfo("o_orderdate"));
    auto o_shippriority = expr_maker.CVE(schema.GetColumnInfo("o_shippriority"));
    // Make the output schema.
    o_seq_scan_out.AddOutput("o_custkey", o_custkey);
    o_seq_scan_out.AddOutput("o_orderkey", o_orderkey);
    o_seq_scan_out.AddOutput("o_orderdate", o_orderdate);
    o_seq_scan_out.AddOutput("o_shippriority", o_shippriority);
    // Predicate.
    auto predicate = expr_maker.CompareLt(o_orderdate, expr_maker.Constant(1995, 03, 15));
    // Build.
    o_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(o_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(table->GetId())
                     .Build();
  }

  // Scan lineitem.
  std::unique_ptr<planner::AbstractScanPlanNode> l_seq_scan;
  planner::OutputSchemaHelper l_seq_scan_out(&expr_maker, 1);
  {
    sql::Table *table = accessor->LookupTableByName("tpch.lineitem");
    const auto &schema = table->GetSchema();

    // Read all needed columns.
    auto l_orderkey = expr_maker.CVE(schema.GetColumnInfo("l_orderkey"));
    auto l_shipdate = expr_maker.CVE(schema.GetColumnInfo("l_shipdate"));
    auto l_extendedprice = expr_maker.CVE(schema.GetColumnInfo("l_extendedprice"));
    auto l_discount = expr_maker.CVE(schema.GetColumnInfo("l_discount"));
    // Make the output schema.
    l_seq_scan_out.AddOutput("l_orderkey", l_orderkey);
    l_seq_scan_out.AddOutput("l_extendedprice", l_extendedprice);
    l_seq_scan_out.AddOutput("l_discount", l_discount);
    // Predicate.
    auto predicate = expr_maker.CompareGt(l_shipdate, expr_maker.Constant(1995, 03, 15));
    // Build.
    l_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(l_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(table->GetId())
                     .Build();
  }

  // Make HJ1: customer x orders
  std::unique_ptr<planner::AbstractPlanNode> hash_join1;
  planner::OutputSchemaHelper hash_join_out1(&expr_maker, 0);
  {
    // Left columns.
    auto c_custkey = cust_seq_scan_out.GetOutput("c_custkey");
    // Right columns.
    auto o_custkey = o_seq_scan_out.GetOutput("o_custkey");
    auto o_orderkey = o_seq_scan_out.GetOutput("o_orderkey");
    auto o_orderdate = o_seq_scan_out.GetOutput("o_orderdate");
    auto o_shippriority = o_seq_scan_out.GetOutput("o_shippriority");
    // Output Schema.
    hash_join_out1.AddOutput("o_orderkey", o_orderkey);
    hash_join_out1.AddOutput("o_orderdate", o_orderdate);
    hash_join_out1.AddOutput("o_shippriority", o_shippriority);
    // Predicate.
    auto predicate = expr_maker.CompareEq(c_custkey, o_custkey);
    // Build.
    hash_join1 = planner::HashJoinPlanNode::Builder{}
                     .AddChild(std::move(cust_seq_scan))
                     .AddChild(std::move(o_seq_scan))
                     .SetOutputSchema(hash_join_out1.MakeSchema())
                     .AddLeftHashKey(c_custkey)
                     .AddRightHashKey(o_custkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .Build();
  }

  // Make HJ2: HJ1 x lineitem
  std::unique_ptr<planner::AbstractPlanNode> hash_join2;
  planner::OutputSchemaHelper hash_join_out2(&expr_maker, 0);
  {
    // Left columns.
    auto o_orderkey = hash_join_out1.GetOutput("o_orderkey");
    auto o_orderdate = hash_join_out1.GetOutput("o_orderdate");
    auto o_shippriority = hash_join_out1.GetOutput("o_shippriority");
    // Right columns.
    auto l_orderkey = l_seq_scan_out.GetOutput("l_orderkey");
    auto l_extendedprice = l_seq_scan_out.GetOutput("l_extendedprice");
    auto l_discount = l_seq_scan_out.GetOutput("l_discount");
    // Output Schema.
    hash_join_out2.AddOutput("o_orderdate", o_orderdate);
    hash_join_out2.AddOutput("o_shippriority", o_shippriority);
    hash_join_out2.AddOutput("l_orderkey", l_orderkey);
    hash_join_out2.AddOutput("l_extendedprice", l_extendedprice);
    hash_join_out2.AddOutput("l_discount", l_discount);
    // Predicate.
    auto predicate = expr_maker.CompareEq(o_orderkey, l_orderkey);
    // Build.
    hash_join2 = planner::HashJoinPlanNode::Builder{}
                     .AddChild(std::move(hash_join1))
                     .AddChild(std::move(l_seq_scan))
                     .SetOutputSchema(hash_join_out2.MakeSchema())
                     .AddLeftHashKey(o_orderkey)
                     .AddRightHashKey(l_orderkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .Build();
  }

  // Make the aggregate
  std::unique_ptr<planner::AbstractPlanNode> agg;
  planner::OutputSchemaHelper agg_out(&expr_maker, 0);
  {
    // Read previous layer's output
    auto o_orderdate = hash_join_out2.GetOutput("o_orderdate");
    auto o_shippriority = hash_join_out2.GetOutput("o_shippriority");
    auto l_orderkey = hash_join_out2.GetOutput("l_orderkey");
    auto l_extendedprice = hash_join_out2.GetOutput("l_extendedprice");
    auto l_discount = hash_join_out2.GetOutput("l_discount");
    // Make the aggregate expressions
    auto revenue = expr_maker.AggSum(
        expr_maker.OpMul(l_extendedprice, expr_maker.OpMin(expr_maker.Constant(1.0f), l_discount)));
    // Add them to the helper.
    agg_out.AddGroupByTerm("l_orderkey", l_orderkey);
    agg_out.AddGroupByTerm("o_orderdate", o_orderdate);
    agg_out.AddGroupByTerm("o_shippriority", o_shippriority);
    agg_out.AddAggTerm("revenue", revenue);
    // Make the output schema
    agg_out.AddOutput("l_orderkey", agg_out.GetGroupByTermForOutput("l_orderkey"));
    agg_out.AddOutput("o_orderdate", agg_out.GetGroupByTermForOutput("o_orderdate"));
    agg_out.AddOutput("o_shippriority", agg_out.GetGroupByTermForOutput("o_shippriority"));
    agg_out.AddOutput("revenue", agg_out.GetAggTermForOutput("revenue"));
    // Build
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(agg_out.MakeSchema())
              .AddGroupByTerm(l_orderkey)
              .AddGroupByTerm(o_orderdate)
              .AddGroupByTerm(o_shippriority)
              .AddAggregateTerm(revenue)
              .AddChild(std::move(hash_join2))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }

  // Make final sort
  std::unique_ptr<planner::AbstractPlanNode> order_by;
  planner::OutputSchemaHelper order_by_out(&expr_maker, 0);
  {
    // Output colums
    auto l_orderkey = agg_out.GetOutput("l_orderkey");
    auto o_orderdate = agg_out.GetOutput("o_orderdate");
    auto o_shippriority = agg_out.GetOutput("o_shippriority");
    auto revenue = agg_out.GetOutput("revenue");
    order_by_out.AddOutput("l_orderkey", l_orderkey);
    order_by_out.AddOutput("revenue", revenue);
    order_by_out.AddOutput("o_orderdate", o_orderdate);
    order_by_out.AddOutput("o_shippriority", o_shippriority);
    // Build
    planner::OrderByPlanNode::Builder builder;
    order_by = builder.SetOutputSchema(order_by_out.MakeSchema())
                   .AddChild(std::move(agg))
                   .AddSortKey(revenue, planner::OrderByOrderingType::DESC)
                   .AddSortKey(o_orderdate, planner::OrderByOrderingType::ASC)
                   .SetLimit(10)
                   .Build();
  }

  // Compile plan
  auto last_op = order_by.get();
  NoOpResultConsumer consumer;
  sql::MemoryPool memory(nullptr);
  sql::ExecutionContext exec_ctx(&memory, last_op->GetOutputSchema(), &consumer);
  auto query = CompilationContext::Compile(*last_op);
  // Run Once to force compilation
  query->Run(&exec_ctx, kExecutionMode);

  // Only time execution
  for (auto _ : state) {
    query->Run(&exec_ctx, kExecutionMode);
  }
}

BENCHMARK_DEFINE_F(TpchBenchmark, Q4)(benchmark::State &state) {
  auto accessor = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;
  // Orders.
  sql::Table *o_table = accessor->LookupTableByName("tpch.orders");
  const auto &o_schema = o_table->GetSchema();
  // Lineitem.
  sql::Table *l_table = accessor->LookupTableByName("tpch.lineitem");
  const auto &l_schema = l_table->GetSchema();
  // Scan orders
  std::unique_ptr<planner::AbstractPlanNode> o_seq_scan;
  planner::OutputSchemaHelper o_seq_scan_out{&expr_maker, 0};
  {
    // Read all needed columns
    auto o_orderkey = expr_maker.CVE(o_schema.GetColumnInfo("o_orderkey"));
    auto o_orderpriority = expr_maker.CVE(o_schema.GetColumnInfo("o_orderpriority"));
    auto o_orderdate = expr_maker.CVE(o_schema.GetColumnInfo("o_orderdate"));
    // Make the output schema
    o_seq_scan_out.AddOutput("o_orderkey", o_orderkey);
    o_seq_scan_out.AddOutput("o_orderpriority", o_orderpriority);
    auto schema = o_seq_scan_out.MakeSchema();
    // Make predicate
    auto lo_date = expr_maker.Constant(1993, 7, 1);
    auto hi_date = expr_maker.Constant(1993, 10, 1);
    auto lo_comp = expr_maker.CompareGe(o_orderdate, lo_date);
    auto hi_comp = expr_maker.CompareLt(o_orderdate, hi_date);
    auto predicate = expr_maker.ConjunctionAnd(lo_comp, hi_comp);
    // Build
    planner::SeqScanPlanNode::Builder builder;
    o_seq_scan = builder.SetOutputSchema(std::move(schema))
                     .SetScanPredicate(predicate)
                     .SetTableOid(o_table->GetId())
                     .Build();
  }
  // Scan lineitem
  std::unique_ptr<planner::AbstractPlanNode> l_seq_scan;
  planner::OutputSchemaHelper l_seq_scan_out{&expr_maker, 1};
  {
    // Read all needed columns
    auto l_orderkey = expr_maker.CVE(l_schema.GetColumnInfo("l_orderkey"));
    auto l_commitdate = expr_maker.CVE(l_schema.GetColumnInfo("l_commitdate"));
    auto l_receiptdate = expr_maker.CVE(l_schema.GetColumnInfo("l_receiptdate"));
    // Make the output schema
    l_seq_scan_out.AddOutput("l_orderkey", l_orderkey);
    auto schema = l_seq_scan_out.MakeSchema();
    auto predicate = expr_maker.CompareLt(l_commitdate, l_receiptdate);
    // Build
    planner::SeqScanPlanNode::Builder builder;
    l_seq_scan = builder.SetOutputSchema(std::move(schema))
                     .SetScanPredicate(predicate)
                     .SetTableOid(l_table->GetId())
                     .Build();
  }
  // Semi Join
  std::unique_ptr<planner::AbstractPlanNode> semi_join;
  planner::OutputSchemaHelper semi_join_out{&expr_maker, 0};
  {
    // Read all needed columns
    // Left
    auto o_orderkey = o_seq_scan_out.GetOutput("o_orderkey");
    auto o_orderpriority = o_seq_scan_out.GetOutput("o_orderpriority");
    // Right
    auto l_orderkey = l_seq_scan_out.GetOutput("l_orderkey");
    // Make output schema
    semi_join_out.AddOutput("o_orderpriority", o_orderpriority);
    auto schema = semi_join_out.MakeSchema();
    auto predicate = expr_maker.CompareEq(o_orderkey, l_orderkey);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    semi_join = builder.SetOutputSchema(std::move(schema))
                    .SetJoinPredicate(predicate)
                    .AddChild(std::move(o_seq_scan))
                    .AddChild(std::move(l_seq_scan))
                    .AddLeftHashKey(o_orderkey)
                    .AddRightHashKey(l_orderkey)
                    .SetJoinType(planner::LogicalJoinType::LEFT_SEMI)
                    .Build();
  }
  // Make the aggregate
  std::unique_ptr<planner::AbstractPlanNode> agg;
  planner::OutputSchemaHelper agg_out{&expr_maker, 0};
  {
    // Read previous layer's output
    auto o_orderpriority = semi_join_out.GetOutput("o_orderpriority");
    // Make the aggregate expressions
    auto one_const = expr_maker.Constant(1);
    auto order_count = expr_maker.AggCount(one_const);
    // Add them to the helper.
    agg_out.AddGroupByTerm("o_orderpriority", o_orderpriority);
    agg_out.AddAggTerm("order_count", order_count);
    // Make the output schema
    agg_out.AddOutput("o_orderpriority", agg_out.GetGroupByTermForOutput("o_orderpriority"));
    agg_out.AddOutput("order_count", agg_out.GetAggTermForOutput("order_count"));
    auto schema = agg_out.MakeSchema();
    // Build
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(std::move(schema))
              .AddGroupByTerm(o_orderpriority)
              .AddAggregateTerm(order_count)
              .AddChild(std::move(semi_join))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }
  // Order By
  std::unique_ptr<planner::AbstractPlanNode> order_by;
  planner::OutputSchemaHelper order_by_out{&expr_maker, 0};
  {
    auto o_orderpriority = agg_out.GetOutput("o_orderpriority");
    auto order_count = agg_out.GetOutput("order_count");
    order_by_out.AddOutput("o_orderpriority", o_orderpriority);
    order_by_out.AddOutput("order_count", order_count);
    auto schema = order_by_out.MakeSchema();
    // Order By Clause
    planner::SortKey clause{o_orderpriority, planner::OrderByOrderingType::ASC};
    // Build
    planner::OrderByPlanNode::Builder builder;
    order_by = builder.SetOutputSchema(std::move(schema))
                   .AddChild(std::move(agg))
                   .AddSortKey(clause.first, clause.second)
                   .Build();
  }

  // Compile plan
  auto last_op = order_by.get();
  NoOpResultConsumer consumer;
  sql::MemoryPool memory(nullptr);
  sql::ExecutionContext exec_ctx(&memory, last_op->GetOutputSchema(), &consumer);
  auto query = CompilationContext::Compile(*last_op);
  // Run Once to force compilation
  query->Run(&exec_ctx, kExecutionMode);

  // Only time execution
  for (auto _ : state) {
    query->Run(&exec_ctx, kExecutionMode);
  }
}

BENCHMARK_DEFINE_F(TpchBenchmark, Q5)(benchmark::State &state) {
  auto accessor = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;
  // Region.
  sql::Table *r_table = accessor->LookupTableByName("tpch.region");
  const auto &r_schema = r_table->GetSchema();
  // Nation.
  sql::Table *n_table = accessor->LookupTableByName("tpch.nation");
  const auto &n_schema = n_table->GetSchema();
  // Customer.
  sql::Table *c_table = accessor->LookupTableByName("tpch.customer");
  const auto &c_schema = c_table->GetSchema();
  // Orders.
  sql::Table *o_table = accessor->LookupTableByName("tpch.orders");
  const auto &o_schema = o_table->GetSchema();
  // Lineitem.
  sql::Table *l_table = accessor->LookupTableByName("tpch.lineitem");
  const auto &l_schema = l_table->GetSchema();
  // Supplier.
  sql::Table *s_table = accessor->LookupTableByName("tpch.supplier");
  const auto &s_schema = s_table->GetSchema();
  // Scan region
  std::unique_ptr<planner::AbstractPlanNode> r_seq_scan;
  planner::OutputSchemaHelper r_seq_scan_out{&expr_maker, 0};
  {
    // Read all needed columns
    auto r_name = expr_maker.CVE(r_schema.GetColumnInfo("r_name"));
    auto r_regionkey = expr_maker.CVE(r_schema.GetColumnInfo("r_regionkey"));
    // Make the output schema
    r_seq_scan_out.AddOutput("r_regionkey", r_regionkey);
    auto schema = r_seq_scan_out.MakeSchema();
    // Make the predicate
    auto asia = expr_maker.Constant("ASIA");
    auto predicate = expr_maker.CompareEq(r_name, asia);
    // Build
    planner::SeqScanPlanNode::Builder builder;
    r_seq_scan = builder.SetOutputSchema(std::move(schema))
                     .SetScanPredicate(predicate)
                     .SetTableOid(r_table->GetId())
                     .Build();
  }
  // Scan nation
  std::unique_ptr<planner::AbstractPlanNode> n_seq_scan;
  planner::OutputSchemaHelper n_seq_scan_out{&expr_maker, 1};
  {
    // Read all needed columns
    auto n_name = expr_maker.CVE(n_schema.GetColumnInfo("n_name"));
    auto n_nationkey = expr_maker.CVE(n_schema.GetColumnInfo("n_nationkey"));
    auto n_regionkey = expr_maker.CVE(n_schema.GetColumnInfo("n_regionkey"));
    // Make the output schema
    n_seq_scan_out.AddOutput("n_name", n_name);
    n_seq_scan_out.AddOutput("n_nationkey", n_nationkey);
    n_seq_scan_out.AddOutput("n_regionkey", n_regionkey);
    auto schema = n_seq_scan_out.MakeSchema();
    // Build
    planner::SeqScanPlanNode::Builder builder;
    n_seq_scan = builder.SetOutputSchema(std::move(schema))
                     .SetScanPredicate(nullptr)
                     .SetTableOid(n_table->GetId())
                     .Build();
  }
  // Scan customer
  std::unique_ptr<planner::AbstractPlanNode> c_seq_scan;
  planner::OutputSchemaHelper c_seq_scan_out{&expr_maker, 1};
  {
    // Read all needed columns
    auto c_custkey = expr_maker.CVE(c_schema.GetColumnInfo("c_custkey"));
    auto c_nationkey = expr_maker.CVE(c_schema.GetColumnInfo("c_nationkey"));
    // Make the output schema
    c_seq_scan_out.AddOutput("c_custkey", c_custkey);
    c_seq_scan_out.AddOutput("c_nationkey", c_nationkey);
    auto schema = c_seq_scan_out.MakeSchema();
    // Build
    planner::SeqScanPlanNode::Builder builder;
    c_seq_scan = builder.SetOutputSchema(std::move(schema))
                     .SetScanPredicate(nullptr)
                     .SetTableOid(c_table->GetId())
                     .Build();
  }
  // Scan orders
  std::unique_ptr<planner::AbstractPlanNode> o_seq_scan;
  planner::OutputSchemaHelper o_seq_scan_out{&expr_maker, 1};
  {
    // Read all needed columns
    auto o_orderkey = expr_maker.CVE(o_schema.GetColumnInfo("o_orderkey"));
    auto o_custkey = expr_maker.CVE(o_schema.GetColumnInfo("o_custkey"));
    auto o_orderdate = expr_maker.CVE(o_schema.GetColumnInfo("o_orderdate"));
    // Make the output schema
    o_seq_scan_out.AddOutput("o_orderkey", o_orderkey);
    o_seq_scan_out.AddOutput("o_custkey", o_custkey);
    auto schema = o_seq_scan_out.MakeSchema();
    // Make predicate
    auto lo_date = expr_maker.Constant(1994, 1, 1);
    auto hi_date = expr_maker.Constant(1995, 1, 1);
    auto lo_comp = expr_maker.CompareGe(o_orderdate, lo_date);
    auto hi_comp = expr_maker.CompareLe(o_orderdate, hi_date);
    auto predicate = expr_maker.ConjunctionAnd(lo_comp, hi_comp);
    // Build
    planner::SeqScanPlanNode::Builder builder;
    o_seq_scan = builder.SetOutputSchema(std::move(schema))
                     .SetScanPredicate(predicate)
                     .SetTableOid(o_table->GetId())
                     .Build();
  }
  // Scan lineitem
  std::unique_ptr<planner::AbstractPlanNode> l_seq_scan;
  planner::OutputSchemaHelper l_seq_scan_out{&expr_maker, 1};
  {
    // Read all needed columns
    auto l_extendedprice = expr_maker.CVE(l_schema.GetColumnInfo("l_extendedprice"));
    auto l_discount = expr_maker.CVE(l_schema.GetColumnInfo("l_discount"));
    auto l_orderkey = expr_maker.CVE(l_schema.GetColumnInfo("l_orderkey"));
    auto l_suppkey = expr_maker.CVE(l_schema.GetColumnInfo("l_suppkey"));
    // Make the output schema
    l_seq_scan_out.AddOutput("l_extendedprice", l_extendedprice);
    l_seq_scan_out.AddOutput("l_discount", l_discount);
    l_seq_scan_out.AddOutput("l_orderkey", l_orderkey);
    l_seq_scan_out.AddOutput("l_suppkey", l_suppkey);
    auto schema = l_seq_scan_out.MakeSchema();
    // Build
    planner::SeqScanPlanNode::Builder builder;
    l_seq_scan = builder.SetOutputSchema(std::move(schema))
                     .SetScanPredicate(nullptr)
                     .SetTableOid(l_table->GetId())
                     .Build();
  }
  // Scan supplier
  std::unique_ptr<planner::AbstractPlanNode> s_seq_scan;
  planner::OutputSchemaHelper s_seq_scan_out{&expr_maker, 0};
  {
    // Read all needed columns
    auto s_suppkey = expr_maker.CVE(s_schema.GetColumnInfo("s_suppkey"));
    auto s_nationkey = expr_maker.CVE(s_schema.GetColumnInfo("s_nationkey"));
    // Make the output schema
    s_seq_scan_out.AddOutput("s_suppkey", s_suppkey);
    s_seq_scan_out.AddOutput("s_nationkey", s_nationkey);
    auto schema = s_seq_scan_out.MakeSchema();
    // Build
    planner::SeqScanPlanNode::Builder builder;
    s_seq_scan = builder.SetOutputSchema(std::move(schema))
                     .SetScanPredicate(nullptr)
                     .SetTableOid(s_table->GetId())
                     .Build();
  }
  // Make first hash join
  std::unique_ptr<planner::AbstractPlanNode> hash_join1;
  planner::OutputSchemaHelper hash_join_out1{&expr_maker, 0};
  {
    // Left columns
    auto r_regionkey = r_seq_scan_out.GetOutput("r_regionkey");
    // Right columns
    auto n_name = n_seq_scan_out.GetOutput("n_name");
    auto n_nationkey = n_seq_scan_out.GetOutput("n_nationkey");
    auto n_regionkey = n_seq_scan_out.GetOutput("n_regionkey");
    // Output Schema
    hash_join_out1.AddOutput("n_nationkey", n_nationkey);
    hash_join_out1.AddOutput("n_name", n_name);
    auto schema = hash_join_out1.MakeSchema();
    // Predicate
    auto predicate = expr_maker.CompareEq(r_regionkey, n_regionkey);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join1 = builder.AddChild(std::move(r_seq_scan))
                     .AddChild(std::move(n_seq_scan))
                     .SetOutputSchema(std::move(schema))
                     .AddLeftHashKey(r_regionkey)
                     .AddRightHashKey(n_regionkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .Build();
  }
  // Make second hash join
  std::unique_ptr<planner::AbstractPlanNode> hash_join2;
  planner::OutputSchemaHelper hash_join_out2{&expr_maker, 0};
  {
    // Left columns
    auto n_nationkey = hash_join_out1.GetOutput("n_nationkey");
    auto n_name = hash_join_out1.GetOutput("n_name");
    // Right columns
    auto c_custkey = c_seq_scan_out.GetOutput("c_custkey");
    auto c_nationkey = c_seq_scan_out.GetOutput("c_nationkey");
    // Output Schema
    hash_join_out2.AddOutput("n_nationkey", n_nationkey);
    hash_join_out2.AddOutput("n_name", n_name);
    hash_join_out2.AddOutput("c_custkey", c_custkey);
    auto schema = hash_join_out2.MakeSchema();
    // Predicate
    auto predicate = expr_maker.CompareEq(n_nationkey, c_nationkey);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join2 = builder.AddChild(std::move(hash_join1))
                     .AddChild(std::move(c_seq_scan))
                     .SetOutputSchema(std::move(schema))
                     .AddLeftHashKey(n_nationkey)
                     .AddRightHashKey(c_nationkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .Build();
  }
  // Make third hash join
  std::unique_ptr<planner::AbstractPlanNode> hash_join3;
  planner::OutputSchemaHelper hash_join_out3{&expr_maker, 0};
  {
    // Left columns
    auto n_nationkey = hash_join_out2.GetOutput("n_nationkey");
    auto n_name = hash_join_out2.GetOutput("n_name");
    auto c_custkey = hash_join_out2.GetOutput("c_custkey");
    // Right columns
    auto o_custkey = o_seq_scan_out.GetOutput("o_custkey");
    auto o_orderkey = o_seq_scan_out.GetOutput("o_orderkey");
    // Output Schema
    hash_join_out3.AddOutput("n_nationkey", n_nationkey);
    hash_join_out3.AddOutput("n_name", n_name);
    hash_join_out3.AddOutput("o_orderkey", o_orderkey);
    auto schema = hash_join_out3.MakeSchema();
    // Predicate
    auto predicate = expr_maker.CompareEq(c_custkey, o_custkey);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join3 = builder.AddChild(std::move(hash_join2))
                     .AddChild(std::move(o_seq_scan))
                     .SetOutputSchema(std::move(schema))
                     .AddLeftHashKey(c_custkey)
                     .AddRightHashKey(o_custkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .Build();
  }
  // Make fourth hash join
  std::unique_ptr<planner::AbstractPlanNode> hash_join4;
  planner::OutputSchemaHelper hash_join_out4{&expr_maker, 1};
  {
    // Left columns
    auto n_nationkey = hash_join_out3.GetOutput("n_nationkey");
    auto n_name = hash_join_out3.GetOutput("n_name");
    auto o_orderkey = hash_join_out3.GetOutput("o_orderkey");
    // Right columns
    auto l_extendedprice = l_seq_scan_out.GetOutput("l_extendedprice");
    auto l_discount = l_seq_scan_out.GetOutput("l_discount");
    auto l_orderkey = l_seq_scan_out.GetOutput("l_orderkey");
    auto l_suppkey = l_seq_scan_out.GetOutput("l_suppkey");
    // Output Schema
    hash_join_out4.AddOutput("l_extendedprice", l_extendedprice);
    hash_join_out4.AddOutput("l_discount", l_discount);
    hash_join_out4.AddOutput("l_suppkey", l_suppkey);
    hash_join_out4.AddOutput("n_nationkey", n_nationkey);
    hash_join_out4.AddOutput("n_name", n_name);
    auto schema = hash_join_out4.MakeSchema();
    // Predicate
    auto predicate = expr_maker.CompareEq(o_orderkey, l_orderkey);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join4 = builder.AddChild(std::move(hash_join3))
                     .AddChild(std::move(l_seq_scan))
                     .SetOutputSchema(std::move(schema))
                     .AddLeftHashKey(o_orderkey)
                     .AddRightHashKey(l_orderkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .Build();
  }
  // Make fifth hash join
  std::unique_ptr<planner::AbstractPlanNode> hash_join5;
  planner::OutputSchemaHelper hash_join_out5{&expr_maker, 0};
  {
    // Left columns
    auto s_suppkey = s_seq_scan_out.GetOutput("s_suppkey");
    auto s_nationkey = s_seq_scan_out.GetOutput("s_nationkey");
    // Right columns
    auto n_name = hash_join_out4.GetOutput("n_name");
    auto n_nationkey = hash_join_out4.GetOutput("n_nationkey");
    auto l_extendedprice = hash_join_out4.GetOutput("l_extendedprice");
    auto l_discount = hash_join_out4.GetOutput("l_discount");
    auto l_suppkey = hash_join_out4.GetOutput("l_suppkey");
    // Output Schema
    hash_join_out5.AddOutput("l_extendedprice", l_extendedprice);
    hash_join_out5.AddOutput("l_discount", l_discount);
    hash_join_out5.AddOutput("n_name", n_name);
    auto schema = hash_join_out5.MakeSchema();
    // Predicate
    auto comp1 = expr_maker.CompareEq(n_nationkey, s_nationkey);
    auto comp2 = expr_maker.CompareEq(l_suppkey, s_suppkey);
    auto predicate = expr_maker.ConjunctionAnd(comp1, comp2);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join5 = builder.AddChild(std::move(s_seq_scan))
                     .AddChild(std::move(hash_join4))
                     .SetOutputSchema(std::move(schema))
                     .AddLeftHashKey(s_nationkey)
                     .AddLeftHashKey(s_suppkey)
                     .AddRightHashKey(n_nationkey)
                     .AddRightHashKey(l_suppkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .Build();
  }
  // Make the aggregate
  std::unique_ptr<planner::AbstractPlanNode> agg;
  planner::OutputSchemaHelper agg_out{&expr_maker, 0};
  {
    // Read previous layer's output
    auto l_extendedprice = hash_join_out5.GetOutput("l_extendedprice");
    auto l_discount = hash_join_out5.GetOutput("l_discount");
    auto n_name = hash_join_out5.GetOutput("n_name");
    // Make the aggregate expressions
    auto one_const = expr_maker.Constant(1.0f);
    auto revenue = expr_maker.AggSum(
        expr_maker.OpMul(l_extendedprice, expr_maker.OpMin(one_const, l_discount)));
    // Add them to the helper.
    agg_out.AddGroupByTerm("n_name", n_name);
    agg_out.AddAggTerm("revenue", revenue);
    // Make the output schema
    agg_out.AddOutput("n_name", agg_out.GetGroupByTermForOutput("n_name"));
    agg_out.AddOutput("revenue", agg_out.GetAggTermForOutput("revenue"));
    auto schema = agg_out.MakeSchema();
    // Build
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(std::move(schema))
              .AddGroupByTerm(n_name)
              .AddAggregateTerm(revenue)
              .AddChild(std::move(hash_join5))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }
  // Order By
  std::unique_ptr<planner::AbstractPlanNode> order_by;
  planner::OutputSchemaHelper order_by_out{&expr_maker, 0};
  {
    // Output Colums col1, col2, col1 + col2
    auto n_name = agg_out.GetOutput("n_name");
    auto revenue = agg_out.GetOutput("revenue");
    order_by_out.AddOutput("n_name", n_name);
    order_by_out.AddOutput("revenue", revenue);
    auto schema = order_by_out.MakeSchema();
    // Order By Clause
    planner::SortKey clause{revenue, planner::OrderByOrderingType::DESC};
    // Build
    planner::OrderByPlanNode::Builder builder;
    order_by = builder.SetOutputSchema(std::move(schema))
                   .AddChild(std::move(agg))
                   .AddSortKey(clause.first, clause.second)
                   .Build();
  }

  // Compile plan
  auto last_op = order_by.get();
  NoOpResultConsumer consumer;
  sql::MemoryPool memory(nullptr);
  sql::ExecutionContext exec_ctx(&memory, last_op->GetOutputSchema(), &consumer);
  auto query = CompilationContext::Compile(*last_op);
  // Run Once to force compilation
  query->Run(&exec_ctx, kExecutionMode);

  // Only time execution
  for (auto _ : state) {
    query->Run(&exec_ctx, kExecutionMode);
  }
}

BENCHMARK_DEFINE_F(TpchBenchmark, Q6)(benchmark::State &state) {
  auto accessor = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;
  // Lineitem.
  sql::Table *l_table = accessor->LookupTableByName("tpch.lineitem");
  const auto &l_schema = l_table->GetSchema();
  // Scan lineitem
  std::unique_ptr<planner::AbstractPlanNode> l_seq_scan;
  planner::OutputSchemaHelper l_seq_scan_out{&expr_maker, 0};
  {
    // Read all needed columns
    auto l_extendedprice = expr_maker.CVE(l_schema.GetColumnInfo("l_extendedprice"));
    auto l_discount = expr_maker.CVE(l_schema.GetColumnInfo("l_discount"));
    auto l_shipdate = expr_maker.CVE(l_schema.GetColumnInfo("l_shipdate"));
    auto l_quantity = expr_maker.CVE(l_schema.GetColumnInfo("l_quantity"));
    // Make the output schema
    l_seq_scan_out.AddOutput("l_extendedprice", l_extendedprice);
    l_seq_scan_out.AddOutput("l_discount", l_discount);
    auto schema = l_seq_scan_out.MakeSchema();
    // Predicate
    auto lo_date = expr_maker.Constant(1994, 1, 1);
    auto hi_date = expr_maker.Constant(1995, 1, 1);
    auto lo_discount = expr_maker.Constant(0.05f);
    auto hi_discount = expr_maker.Constant(0.07f);
    auto lo_qty = expr_maker.Constant(24.0f);
    auto lo_date_comp = expr_maker.CompareGe(l_shipdate, lo_date);
    auto hi_date_comp = expr_maker.CompareLt(l_shipdate, hi_date);
    auto lo_discount_comp = expr_maker.CompareGe(l_discount, lo_discount);
    auto hi_discount_comp = expr_maker.CompareLe(l_discount, hi_discount);
    auto lo_qty_comp = expr_maker.CompareLt(l_quantity, lo_qty);
    auto date_comp = expr_maker.ConjunctionAnd(lo_date_comp, hi_date_comp);
    auto discount_comp = expr_maker.ConjunctionAnd(lo_discount_comp, hi_discount_comp);
    auto predicate =
        expr_maker.ConjunctionAnd(date_comp, expr_maker.ConjunctionAnd(discount_comp, lo_qty_comp));
    // Build
    planner::SeqScanPlanNode::Builder builder;
    l_seq_scan = builder.SetOutputSchema(std::move(schema))
                     .SetScanPredicate(predicate)
                     .SetTableOid(l_table->GetId())
                     .Build();
  }
  // Make the aggregate
  std::unique_ptr<planner::AbstractPlanNode> agg;
  planner::OutputSchemaHelper agg_out{&expr_maker, 0};
  {
    // Read previous layer's output
    auto l_extendedprice = l_seq_scan_out.GetOutput("l_extendedprice");
    auto l_discount = l_seq_scan_out.GetOutput("l_discount");
    // Make the aggregate expressions
    auto revenue = expr_maker.AggSum(expr_maker.OpMul(l_extendedprice, l_discount));
    // Add them to the helper.
    agg_out.AddAggTerm("revenue", revenue);
    // Make the output schema
    agg_out.AddOutput("revenue", agg_out.GetAggTermForOutput("revenue"));
    auto schema = agg_out.MakeSchema();
    // Build
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(std::move(schema))
              .AddAggregateTerm(revenue)
              .AddChild(std::move(l_seq_scan))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }

  // Compile plan
  auto last_op = agg.get();
  NoOpResultConsumer consumer;
  sql::MemoryPool memory(nullptr);
  sql::ExecutionContext exec_ctx(&memory, last_op->GetOutputSchema(), &consumer);
  auto query = CompilationContext::Compile(*last_op);
  // Run Once to force compilation
  query->Run(&exec_ctx, kExecutionMode);

  // Only time execution
  for (auto _ : state) {
    query->Run(&exec_ctx, kExecutionMode);
  }
}

BENCHMARK_DEFINE_F(TpchBenchmark, Q7)(benchmark::State &state) {
  auto accessor = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;
  // Nation.
  sql::Table *n_table = accessor->LookupTableByName("tpch.nation");
  const auto &n_schema = n_table->GetSchema();
  // Customer.
  sql::Table *c_table = accessor->LookupTableByName("tpch.customer");
  const auto &c_schema = c_table->GetSchema();
  // Orders.
  sql::Table *o_table = accessor->LookupTableByName("tpch.orders");
  const auto &o_schema = o_table->GetSchema();
  // Lineitem.
  sql::Table *l_table = accessor->LookupTableByName("tpch.lineitem");
  const auto &l_schema = l_table->GetSchema();
  // Supplier.
  sql::Table *s_table = accessor->LookupTableByName("tpch.supplier");
  const auto &s_schema = s_table->GetSchema();
  // Scan nation1
  std::unique_ptr<planner::AbstractPlanNode> n1_seq_scan;
  planner::OutputSchemaHelper n1_seq_scan_out{&expr_maker, 0};
  {
    // Read all needed columns
    auto n1_name = expr_maker.CVE(n_schema.GetColumnInfo("n_name"));
    auto n1_nationkey = expr_maker.CVE(n_schema.GetColumnInfo("n_nationkey"));
    // Make the output schema
    n1_seq_scan_out.AddOutput("n1_name", n1_name);
    n1_seq_scan_out.AddOutput("n1_nationkey", n1_nationkey);
    // Predicate
    auto schema = n1_seq_scan_out.MakeSchema();
    auto france_comp = expr_maker.CompareEq(n1_name, expr_maker.Constant("FRANCE"));
    auto germany_comp = expr_maker.CompareEq(n1_name, expr_maker.Constant("GERMANY"));
    auto predicate = expr_maker.ConjunctionOr(france_comp, germany_comp);
    // Build
    planner::SeqScanPlanNode::Builder builder;
    n1_seq_scan = builder.SetOutputSchema(std::move(schema))
                      .SetScanPredicate(predicate)
                      .SetTableOid(n_table->GetId())
                      .Build();
  }

  // Scan nation2
  std::unique_ptr<planner::AbstractPlanNode> n2_seq_scan;
  planner::OutputSchemaHelper n2_seq_scan_out{&expr_maker, 1};
  {
    // Read all needed columns
    auto n2_name = expr_maker.CVE(n_schema.GetColumnInfo("n_name"));
    auto n2_nationkey = expr_maker.CVE(n_schema.GetColumnInfo("n_nationkey"));
    // Make the output schema
    n2_seq_scan_out.AddOutput("n2_name", n2_name);
    n2_seq_scan_out.AddOutput("n2_nationkey", n2_nationkey);
    auto schema = n2_seq_scan_out.MakeSchema();
    // Predicate
    auto france_comp = expr_maker.CompareEq(n2_name, expr_maker.Constant("FRANCE"));
    auto germany_comp = expr_maker.CompareEq(n2_name, expr_maker.Constant("GERMANY"));
    auto predicate = expr_maker.ConjunctionOr(france_comp, germany_comp);
    // Build
    planner::SeqScanPlanNode::Builder builder;
    n2_seq_scan = builder.SetOutputSchema(std::move(schema))
                      .SetScanPredicate(predicate)
                      .SetTableOid(n_table->GetId())
                      .Build();
  }

  // BNL
  std::unique_ptr<planner::AbstractPlanNode> bnl;
  planner::OutputSchemaHelper bnl_out{&expr_maker, 0};
  {
    // Left
    auto n1_name = n1_seq_scan_out.GetOutput("n1_name");
    auto n1_nationkey = n1_seq_scan_out.GetOutput("n1_nationkey");
    // Right
    auto n2_name = n2_seq_scan_out.GetOutput("n2_name");
    auto n2_nationkey = n2_seq_scan_out.GetOutput("n2_nationkey");
    // Make the output schema
    bnl_out.AddOutput("n1_name", n1_name);
    bnl_out.AddOutput("n2_name", n2_name);
    bnl_out.AddOutput("n1_nationkey", n1_nationkey);
    bnl_out.AddOutput("n2_nationkey", n2_nationkey);
    auto schema = bnl_out.MakeSchema();
    // Predicate
    auto france_comp1 = expr_maker.CompareEq(n1_name, expr_maker.Constant("FRANCE"));
    auto france_comp2 = expr_maker.CompareEq(n2_name, expr_maker.Constant("FRANCE"));
    auto germany_comp1 = expr_maker.CompareEq(n1_name, expr_maker.Constant("GERMANY"));
    auto germany_comp2 = expr_maker.CompareEq(n2_name, expr_maker.Constant("GERMANY"));
    auto eq1 = expr_maker.ConjunctionAnd(france_comp1, germany_comp2);
    auto eq2 = expr_maker.ConjunctionAnd(france_comp2, germany_comp1);
    auto predicate = expr_maker.ConjunctionOr(eq1, eq2);
    // Build
    planner::NestedLoopJoinPlanNode::Builder builder;
    bnl = builder.AddChild(std::move(n1_seq_scan))
              .AddChild(std::move(n2_seq_scan))
              .SetOutputSchema(std::move(schema))
              .SetJoinType(planner::LogicalJoinType::INNER)
              .SetJoinPredicate(predicate)
              .Build();
  }

  // Scan customer
  std::unique_ptr<planner::AbstractPlanNode> c_seq_scan;
  planner::OutputSchemaHelper c_seq_scan_out{&expr_maker, 1};
  {
    // Read all needed columns
    auto c_custkey = expr_maker.CVE(c_schema.GetColumnInfo("c_custkey"));
    auto c_nationkey = expr_maker.CVE(c_schema.GetColumnInfo("c_nationkey"));
    // Make the output schema
    c_seq_scan_out.AddOutput("c_custkey", c_custkey);
    c_seq_scan_out.AddOutput("c_nationkey", c_nationkey);
    auto schema = c_seq_scan_out.MakeSchema();

    // Build
    planner::SeqScanPlanNode::Builder builder;
    c_seq_scan = builder.SetOutputSchema(std::move(schema))
                     .SetScanPredicate(nullptr)
                     .SetTableOid(c_table->GetId())
                     .Build();
  }

  // Hash Join 1
  std::unique_ptr<planner::AbstractPlanNode> hash_join1;
  planner::OutputSchemaHelper hash_join_out1{&expr_maker, 0};
  {
    // Left columns
    auto n1_name = bnl_out.GetOutput("n1_name");
    auto n2_name = bnl_out.GetOutput("n2_name");
    auto n1_nationkey = bnl_out.GetOutput("n1_nationkey");
    auto n2_nationkey = bnl_out.GetOutput("n2_nationkey");
    // Right columns
    auto c_custkey = c_seq_scan_out.GetOutput("c_custkey");
    auto c_nationkey = c_seq_scan_out.GetOutput("c_nationkey");
    // Output Schema
    hash_join_out1.AddOutput("n1_name", n1_name);
    hash_join_out1.AddOutput("n2_name", n2_name);
    hash_join_out1.AddOutput("n1_nationkey", n1_nationkey);
    hash_join_out1.AddOutput("c_custkey", c_custkey);
    auto schema = hash_join_out1.MakeSchema();
    // Predicate
    auto predicate = expr_maker.CompareEq(n2_nationkey, c_nationkey);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join1 = builder.AddChild(std::move(bnl))
                     .AddChild(std::move(c_seq_scan))
                     .SetOutputSchema(std::move(schema))
                     .AddLeftHashKey(n2_nationkey)
                     .AddRightHashKey(c_nationkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .Build();
  }

  // Scan orders
  std::unique_ptr<planner::AbstractPlanNode> o_seq_scan;
  planner::OutputSchemaHelper o_seq_scan_out{&expr_maker, 1};
  {
    // Read all needed columns
    auto o_orderkey = expr_maker.CVE(o_schema.GetColumnInfo("o_orderkey"));
    auto o_custkey = expr_maker.CVE(o_schema.GetColumnInfo("o_custkey"));
    // Make the output schema
    o_seq_scan_out.AddOutput("o_orderkey", o_orderkey);
    o_seq_scan_out.AddOutput("o_custkey", o_custkey);
    auto schema = o_seq_scan_out.MakeSchema();

    // Build
    planner::SeqScanPlanNode::Builder builder;
    o_seq_scan = builder.SetOutputSchema(std::move(schema))
                     .SetScanPredicate(nullptr)
                     .SetTableOid(o_table->GetId())
                     .Build();
  }

  // Make second hash join
  std::unique_ptr<planner::AbstractPlanNode> hash_join2;
  planner::OutputSchemaHelper hash_join_out2{&expr_maker, 0};
  {
    // Left columns
    auto n1_name = hash_join_out1.GetOutput("n1_name");
    auto n2_name = hash_join_out1.GetOutput("n2_name");
    auto n1_nationkey = hash_join_out1.GetOutput("n1_nationkey");
    auto c_custkey = hash_join_out1.GetOutput("c_custkey");
    // Right columns
    auto o_orderkey = o_seq_scan_out.GetOutput("o_orderkey");
    auto o_custkey = o_seq_scan_out.GetOutput("o_custkey");
    // Output Schema
    hash_join_out2.AddOutput("n1_name", n1_name);
    hash_join_out2.AddOutput("n2_name", n2_name);
    hash_join_out2.AddOutput("n1_nationkey", n1_nationkey);
    hash_join_out2.AddOutput("o_orderkey", o_orderkey);
    auto schema = hash_join_out2.MakeSchema();
    // Predicate
    auto predicate = expr_maker.CompareEq(c_custkey, o_custkey);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join2 = builder.AddChild(std::move(hash_join1))
                     .AddChild(std::move(o_seq_scan))
                     .SetOutputSchema(std::move(schema))
                     .AddLeftHashKey(c_custkey)
                     .AddRightHashKey(o_custkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .Build();
  }

  // Scan lineitem
  std::unique_ptr<planner::AbstractPlanNode> l_seq_scan;
  planner::OutputSchemaHelper l_seq_scan_out{&expr_maker, 1};
  {
    // Read all needed columns
    auto l_extendedprice = expr_maker.CVE(l_schema.GetColumnInfo("l_extendedprice"));
    auto l_discount = expr_maker.CVE(l_schema.GetColumnInfo("l_discount"));
    auto l_orderkey = expr_maker.CVE(l_schema.GetColumnInfo("l_orderkey"));
    auto l_suppkey = expr_maker.CVE(l_schema.GetColumnInfo("l_suppkey"));
    auto l_shipdate = expr_maker.CVE(l_schema.GetColumnInfo("l_shipdate"));
    // Make the output schema
    auto const_one = expr_maker.Constant(1.0f);
    auto volume = expr_maker.OpMul(l_extendedprice, expr_maker.OpMin(const_one, l_discount));
    l_seq_scan_out.AddOutput("volume", volume);
    l_seq_scan_out.AddOutput("l_orderkey", l_orderkey);
    l_seq_scan_out.AddOutput("l_suppkey", l_suppkey);
    auto extract_year =
        expr_maker.UnaryOperator(KnownOperator::ExtractYear, Type::IntegerType(false), l_shipdate);
    l_seq_scan_out.AddOutput("l_year", extract_year);
    auto schema = l_seq_scan_out.MakeSchema();
    // Predicate
    auto lo_date = expr_maker.Constant(1995, 1, 1);
    auto hi_date = expr_maker.Constant(1996, 12, 31);
    auto lo_date_comp = expr_maker.CompareGe(l_shipdate, lo_date);
    auto hi_date_comp = expr_maker.CompareLt(l_shipdate, hi_date);
    auto predicate = expr_maker.ConjunctionAnd(lo_date_comp, hi_date_comp);
    // Build
    planner::SeqScanPlanNode::Builder builder;
    l_seq_scan = builder.SetOutputSchema(std::move(schema))
                     .SetScanPredicate(predicate)
                     .SetTableOid(l_table->GetId())
                     .Build();
  }

  // Make third hash join
  std::unique_ptr<planner::AbstractPlanNode> hash_join3;
  planner::OutputSchemaHelper hash_join_out3{&expr_maker, 1};
  {
    // Left columns
    auto n1_name = hash_join_out2.GetOutput("n1_name");
    auto n2_name = hash_join_out2.GetOutput("n2_name");
    auto n1_nationkey = hash_join_out2.GetOutput("n1_nationkey");
    auto o_orderkey = hash_join_out2.GetOutput("o_orderkey");

    // Right columns
    auto volume = l_seq_scan_out.GetOutput("volume");
    auto l_orderkey = l_seq_scan_out.GetOutput("l_orderkey");
    auto l_suppkey = l_seq_scan_out.GetOutput("l_suppkey");
    auto l_year = l_seq_scan_out.GetOutput("l_year");
    // Output Schema
    hash_join_out3.AddOutput("n1_name", n1_name);
    hash_join_out3.AddOutput("n2_name", n2_name);
    hash_join_out3.AddOutput("n1_nationkey", n1_nationkey);
    hash_join_out3.AddOutput("volume", volume);
    hash_join_out3.AddOutput("l_suppkey", l_suppkey);
    hash_join_out3.AddOutput("l_year", l_year);
    auto schema = hash_join_out3.MakeSchema();
    // Predicate
    auto predicate = expr_maker.CompareEq(o_orderkey, l_orderkey);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join3 = builder.AddChild(std::move(hash_join2))
                     .AddChild(std::move(l_seq_scan))
                     .SetOutputSchema(std::move(schema))
                     .AddLeftHashKey(o_orderkey)
                     .AddRightHashKey(l_orderkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .Build();
  }

  // Scan supplier
  std::unique_ptr<planner::AbstractPlanNode> s_seq_scan;
  planner::OutputSchemaHelper s_seq_scan_out{&expr_maker, 0};
  {
    // Read all needed columns
    auto s_suppkey = expr_maker.CVE(s_schema.GetColumnInfo("s_suppkey"));
    auto s_nationkey = expr_maker.CVE(s_schema.GetColumnInfo("s_nationkey"));
    // Make the output schema
    s_seq_scan_out.AddOutput("s_suppkey", s_suppkey);
    s_seq_scan_out.AddOutput("s_nationkey", s_nationkey);
    auto schema = s_seq_scan_out.MakeSchema();

    // Build
    planner::SeqScanPlanNode::Builder builder;
    s_seq_scan = builder.SetOutputSchema(std::move(schema))
                     .SetScanPredicate(nullptr)
                     .SetTableOid(s_table->GetId())
                     .Build();
  }

  // Make fourth hash join
  std::unique_ptr<planner::AbstractPlanNode> hash_join4;
  planner::OutputSchemaHelper hash_join_out4{&expr_maker, 0};
  {
    // Left columns
    auto s_suppkey = s_seq_scan_out.GetOutput("s_suppkey");
    auto s_nationkey = s_seq_scan_out.GetOutput("s_nationkey");
    // Right columns
    auto n1_name = hash_join_out3.GetOutput("n1_name");
    auto n2_name = hash_join_out3.GetOutput("n2_name");
    auto n1_nationkey = hash_join_out3.GetOutput("n1_nationkey");
    auto volume = hash_join_out3.GetOutput("volume");
    auto l_suppkey = hash_join_out3.GetOutput("l_suppkey");
    auto l_year = hash_join_out3.GetOutput("l_year");

    // Output Schema
    hash_join_out4.AddOutput("supp_nation", n1_name);
    hash_join_out4.AddOutput("cust_nation", n2_name);
    hash_join_out4.AddOutput("volume", volume);
    hash_join_out4.AddOutput("l_year", l_year);
    auto schema = hash_join_out4.MakeSchema();
    // Predicate
    auto comp1 = expr_maker.CompareEq(s_suppkey, l_suppkey);
    auto comp2 = expr_maker.CompareEq(s_nationkey, n1_nationkey);
    auto predicate = expr_maker.ConjunctionAnd(comp1, comp2);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join4 = builder.AddChild(std::move(s_seq_scan))
                     .AddChild(std::move(hash_join3))
                     .SetOutputSchema(std::move(schema))
                     .AddLeftHashKey(s_suppkey)
                     .AddLeftHashKey(s_nationkey)
                     .AddRightHashKey(l_suppkey)
                     .AddRightHashKey(n1_nationkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .Build();
  }

  // Make the aggregate
  std::unique_ptr<planner::AbstractPlanNode> agg;
  planner::OutputSchemaHelper agg_out{&expr_maker, 0};
  {
    // Read previous layer's output
    auto supp_nation = hash_join_out4.GetOutput("supp_nation");
    auto cust_nation = hash_join_out4.GetOutput("cust_nation");
    auto volume = hash_join_out4.GetOutput("volume");
    auto l_year = hash_join_out4.GetOutput("l_year");
    // Make the aggregate expressions
    auto volume_sum = expr_maker.AggSum(volume);
    // Add them to the helper.
    agg_out.AddGroupByTerm("supp_nation", supp_nation);
    agg_out.AddGroupByTerm("cust_nation", cust_nation);
    agg_out.AddGroupByTerm("l_year", l_year);
    agg_out.AddAggTerm("volume", volume_sum);
    // Make the output schema
    agg_out.AddOutput("supp_nation", agg_out.GetGroupByTermForOutput("supp_nation"));
    agg_out.AddOutput("cust_nation", agg_out.GetGroupByTermForOutput("cust_nation"));
    agg_out.AddOutput("l_year", agg_out.GetGroupByTermForOutput("l_year"));
    agg_out.AddOutput("volume", agg_out.GetAggTermForOutput("volume"));
    auto schema = agg_out.MakeSchema();
    // Build
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(std::move(schema))
              .AddGroupByTerm(supp_nation)
              .AddGroupByTerm(cust_nation)
              .AddGroupByTerm(l_year)
              .AddAggregateTerm(volume_sum)
              .AddChild(std::move(hash_join4))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }

  // Order By
  std::unique_ptr<planner::AbstractPlanNode> order_by;
  planner::OutputSchemaHelper order_by_out{&expr_maker, 0};
  {
    // Read previous layer
    auto supp_nation = agg_out.GetOutput("supp_nation");
    auto cust_nation = agg_out.GetOutput("cust_nation");
    auto l_year = agg_out.GetOutput("l_year");
    auto volume = agg_out.GetOutput("volume");

    order_by_out.AddOutput("supp_nation", supp_nation);
    order_by_out.AddOutput("cust_nation", cust_nation);
    order_by_out.AddOutput("l_year", l_year);
    order_by_out.AddOutput("volume", volume);
    auto schema = order_by_out.MakeSchema();
    // Order By Clause
    planner::SortKey clause1{supp_nation, planner::OrderByOrderingType::ASC};
    planner::SortKey clause2{cust_nation, planner::OrderByOrderingType::ASC};
    planner::SortKey clause3{l_year, planner::OrderByOrderingType::ASC};
    // Build
    planner::OrderByPlanNode::Builder builder;
    order_by = builder.SetOutputSchema(std::move(schema))
                   .AddChild(std::move(agg))
                   .AddSortKey(clause1.first, clause1.second)
                   .AddSortKey(clause2.first, clause2.second)
                   .AddSortKey(clause3.first, clause3.second)
                   .Build();
  }

  // Compile plan
  auto last_op = order_by.get();
  NoOpResultConsumer consumer;
  sql::MemoryPool memory(nullptr);
  sql::ExecutionContext exec_ctx(&memory, last_op->GetOutputSchema(), &consumer);
  auto query = CompilationContext::Compile(*last_op);
  // Run Once to force compilation
  query->Run(&exec_ctx, kExecutionMode);

  // Only time execution
  for (auto _ : state) {
    query->Run(&exec_ctx, kExecutionMode);
  }
}

BENCHMARK_DEFINE_F(TpchBenchmark, Q9)(benchmark::State &state) {
  auto accessor = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;

  // Scan nation.
  std::unique_ptr<planner::AbstractScanPlanNode> n_seq_scan;
  planner::OutputSchemaHelper n_seq_scan_out(&expr_maker, 0);
  {
    sql::Table *n_table = accessor->LookupTableByName("tpch.nation");
    const auto &n_schema = n_table->GetSchema();

    // Read all needed columns.
    auto n_nationkey = expr_maker.CVE(n_schema.GetColumnInfo("n_nationkey"));
    auto n_name = expr_maker.CVE(n_schema.GetColumnInfo("n_name"));
    // Make the output schema.
    n_seq_scan_out.AddOutput("n_nationkey", n_nationkey);
    n_seq_scan_out.AddOutput("n_name", n_name);
    // Build.
    n_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(n_seq_scan_out.MakeSchema())
                     .SetScanPredicate(nullptr)
                     .SetTableOid(n_table->GetId())
                     .Build();
  }

  // Scan supplier.
  std::unique_ptr<planner::AbstractPlanNode> s_seq_scan;
  planner::OutputSchemaHelper s_seq_scan_out(&expr_maker, 1);
  {
    sql::Table *s_table = accessor->LookupTableByName("tpch.supplier");
    const auto &s_schema = s_table->GetSchema();

    // Read all needed columns
    auto s_suppkey = expr_maker.CVE(s_schema.GetColumnInfo("s_suppkey"));
    auto s_nationkey = expr_maker.CVE(s_schema.GetColumnInfo("s_nationkey"));
    // Make the output schema.
    s_seq_scan_out.AddOutput("s_suppkey", s_suppkey);
    s_seq_scan_out.AddOutput("s_nationkey", s_nationkey);
    // Build.
    s_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(s_seq_scan_out.MakeSchema())
                     .SetScanPredicate(nullptr)
                     .SetTableOid(s_table->GetId())
                     .Build();
  }

  // Scan part.
  std::unique_ptr<planner::AbstractPlanNode> p_seq_scan;
  planner::OutputSchemaHelper p_seq_scan_out(&expr_maker, 0);
  {
    sql::Table *p_table = accessor->LookupTableByName("tpch.part");
    const auto &p_schema = p_table->GetSchema();

    // Read all needed columns.
    auto p_partkey = expr_maker.CVE(p_schema.GetColumnInfo("p_partkey"));
    auto p_name = expr_maker.CVE(p_schema.GetColumnInfo("p_name"));
    // Make the output schema.
    p_seq_scan_out.AddOutput("p_partkey", p_partkey);
    // Predicate.
    auto predicate = expr_maker.CompareLike(p_name, expr_maker.Constant("%green%"));
    // Build.
    p_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(p_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(p_table->GetId())
                     .Build();
  }

  // Scan partsupp.
  std::unique_ptr<planner::AbstractPlanNode> ps_seq_scan;
  planner::OutputSchemaHelper ps_seq_scan_out(&expr_maker, 1);
  {
    sql::Table *ps_table = accessor->LookupTableByName("tpch.partsupp");
    const auto &ps_schema = ps_table->GetSchema();

    // Read all needed columns.
    auto ps_partkey = expr_maker.CVE(ps_schema.GetColumnInfo("ps_partkey"));
    auto ps_suppkey = expr_maker.CVE(ps_schema.GetColumnInfo("ps_suppkey"));
    auto ps_supplycost = expr_maker.CVE(ps_schema.GetColumnInfo("ps_supplycost"));
    // Make the output schema.
    ps_seq_scan_out.AddOutput("ps_partkey", ps_partkey);
    ps_seq_scan_out.AddOutput("ps_suppkey", ps_suppkey);
    ps_seq_scan_out.AddOutput("ps_supplycost", ps_supplycost);
    // Build.
    ps_seq_scan = planner::SeqScanPlanNode::Builder{}
                      .SetOutputSchema(ps_seq_scan_out.MakeSchema())
                      .SetScanPredicate(nullptr)
                      .SetTableOid(ps_table->GetId())
                      .Build();
  }

  // Scan lineitem.
  std::unique_ptr<planner::AbstractPlanNode> l_seq_scan;
  planner::OutputSchemaHelper l_seq_scan_out(&expr_maker, 1);
  {
    sql::Table *l_table = accessor->LookupTableByName("tpch.lineitem");
    const auto &l_schema = l_table->GetSchema();

    // Read all needed columns.
    auto l_extendedprice = expr_maker.CVE(l_schema.GetColumnInfo("l_extendedprice"));
    auto l_discount = expr_maker.CVE(l_schema.GetColumnInfo("l_discount"));
    auto l_quantity = expr_maker.CVE(l_schema.GetColumnInfo("l_quantity"));
    auto l_suppkey = expr_maker.CVE(l_schema.GetColumnInfo("l_suppkey"));
    auto l_partkey = expr_maker.CVE(l_schema.GetColumnInfo("l_partkey"));
    auto l_orderkey = expr_maker.CVE(l_schema.GetColumnInfo("l_orderkey"));
    // Make the output schema.
    l_seq_scan_out.AddOutput("l_extendedprice", l_extendedprice);
    l_seq_scan_out.AddOutput("l_discount", l_discount);
    l_seq_scan_out.AddOutput("l_quantity", l_quantity);
    l_seq_scan_out.AddOutput("l_suppkey", l_suppkey);
    l_seq_scan_out.AddOutput("l_partkey", l_partkey);
    l_seq_scan_out.AddOutput("l_orderkey", l_orderkey);
    // Build.
    l_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(l_seq_scan_out.MakeSchema())
                     .SetScanPredicate(nullptr)
                     .SetTableOid(l_table->GetId())
                     .Build();
  }

  // Scan orders.
  std::unique_ptr<planner::AbstractPlanNode> o_seq_scan;
  planner::OutputSchemaHelper o_seq_scan_out(&expr_maker, 1);
  {
    sql::Table *o_table = accessor->LookupTableByName("tpch.orders");
    const auto &o_schema = o_table->GetSchema();

    // Read all needed columns.
    auto o_orderkey = expr_maker.CVE(o_schema.GetColumnInfo("o_orderkey"));
    auto o_orderdate = expr_maker.CVE(o_schema.GetColumnInfo("o_orderdate"));
    // Make the output schema.
    o_seq_scan_out.AddOutput("o_orderkey", o_orderkey);
    o_seq_scan_out.AddOutput("o_orderdate", o_orderdate);
    // Build.
    o_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(o_seq_scan_out.MakeSchema())
                     .SetScanPredicate(nullptr)
                     .SetTableOid(o_table->GetId())
                     .Build();
  }

  // Make HJ1: nation x supplier
  std::unique_ptr<planner::AbstractPlanNode> hash_join1;
  planner::OutputSchemaHelper hash_join_out1(&expr_maker, 0);
  {
    // Left columns.
    auto n_nationkey = n_seq_scan_out.GetOutput("n_nationkey");
    UNUSED auto n_name = n_seq_scan_out.GetOutput("n_name");
    // Right columns.
    auto s_suppkey = s_seq_scan_out.GetOutput("s_suppkey");
    auto s_nationkey = s_seq_scan_out.GetOutput("s_nationkey");
    // Output Schema.
    hash_join_out1.AddOutput("s_suppkey", s_suppkey);
    hash_join_out1.AddOutput("n_name", n_name);
    // Predicate.
    auto predicate = expr_maker.CompareEq(n_nationkey, s_nationkey);
    // Build.
    hash_join1 = planner::HashJoinPlanNode::Builder{}
                     .AddChild(std::move(n_seq_scan))
                     .AddChild(std::move(s_seq_scan))
                     .SetOutputSchema(hash_join_out1.MakeSchema())
                     .AddLeftHashKey(n_nationkey)
                     .AddRightHashKey(s_nationkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .Build();
  }

  // Make HJ2: part x partsupp
  std::unique_ptr<planner::AbstractPlanNode> hash_join2;
  planner::OutputSchemaHelper hash_join_out2(&expr_maker, 1);
  {
    // Left columns.
    auto p_partkey = p_seq_scan_out.GetOutput("p_partkey");
    // Right columns.
    auto ps_partkey = ps_seq_scan_out.GetOutput("ps_partkey");
    auto ps_suppkey = ps_seq_scan_out.GetOutput("ps_suppkey");
    auto ps_supplycost = ps_seq_scan_out.GetOutput("ps_supplycost");
    // Output Schema.
    hash_join_out2.AddOutput("p_partkey", p_partkey);
    hash_join_out2.AddOutput("ps_suppkey", ps_suppkey);
    hash_join_out2.AddOutput("ps_supplycost", ps_supplycost);
    // Predicate.
    auto predicate = expr_maker.CompareEq(p_partkey, ps_partkey);
    // Build.
    hash_join2 = planner::HashJoinPlanNode::Builder{}
                     .AddChild(std::move(p_seq_scan))
                     .AddChild(std::move(ps_seq_scan))
                     .SetOutputSchema(hash_join_out2.MakeSchema())
                     .AddLeftHashKey(p_partkey)
                     .AddRightHashKey(ps_partkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .Build();
  }

  // Make HJ3: HJ1 x HJ2
  std::unique_ptr<planner::AbstractPlanNode> hash_join3;
  planner::OutputSchemaHelper hash_join_out3(&expr_maker, 0);
  {
    // Left columns.
    auto n_name = hash_join_out1.GetOutput("n_name");
    auto s_suppkey = hash_join_out1.GetOutput("s_suppkey");
    // Right columns.
    auto p_partkey = hash_join_out2.GetOutput("p_partkey");
    auto ps_suppkey = hash_join_out2.GetOutput("ps_suppkey");
    auto ps_supplycost = hash_join_out2.GetOutput("ps_supplycost");
    // Output Schema.
    hash_join_out3.AddOutput("n_name", n_name);
    hash_join_out3.AddOutput("s_suppkey", s_suppkey);
    hash_join_out3.AddOutput("p_partkey", p_partkey);
    hash_join_out3.AddOutput("ps_supplycost", ps_supplycost);
    // Predicate
    auto predicate = expr_maker.CompareEq(s_suppkey, ps_suppkey);
    // Build
    hash_join3 = planner::HashJoinPlanNode::Builder{}
                     .AddChild(std::move(hash_join1))
                     .AddChild(std::move(hash_join2))
                     .SetOutputSchema(hash_join_out3.MakeSchema())
                     .AddLeftHashKey(s_suppkey)
                     .AddRightHashKey(ps_suppkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .Build();
  }

  // Make HJ4: HJ3 x lineitem
  std::unique_ptr<planner::AbstractPlanNode> hash_join4;
  planner::OutputSchemaHelper hash_join_out4(&expr_maker, 0);
  {
    // Left columns.
    auto n_name = hash_join_out3.GetOutput("n_name");
    auto s_suppkey = hash_join_out3.GetOutput("s_suppkey");
    auto p_partkey = hash_join_out3.GetOutput("p_partkey");
    auto ps_supplycost = hash_join_out3.GetOutput("ps_supplycost");
    // Right columns.
    auto l_extendedprice = l_seq_scan_out.GetOutput("l_extendedprice");
    auto l_discount = l_seq_scan_out.GetOutput("l_discount");
    auto l_quantity = l_seq_scan_out.GetOutput("l_quantity");
    auto l_suppkey = l_seq_scan_out.GetOutput("l_suppkey");
    auto l_partkey = l_seq_scan_out.GetOutput("l_partkey");
    auto l_orderkey = l_seq_scan_out.GetOutput("l_orderkey");
    // Output Schema.
    hash_join_out4.AddOutput("n_name", n_name);
    hash_join_out4.AddOutput("ps_supplycost", ps_supplycost);
    hash_join_out4.AddOutput("l_extendedprice", l_extendedprice);
    hash_join_out4.AddOutput("l_discount", l_discount);
    hash_join_out4.AddOutput("l_quantity", l_quantity);
    hash_join_out4.AddOutput("l_orderkey", l_orderkey);
    // Predicate.
    auto predicate = expr_maker.ConjunctionAnd(expr_maker.CompareEq(p_partkey, l_partkey),
                                               expr_maker.CompareEq(s_suppkey, l_suppkey));
    // Build.
    hash_join4 = planner::HashJoinPlanNode::Builder{}
                     .AddChild(std::move(hash_join3))
                     .AddChild(std::move(l_seq_scan))
                     .SetOutputSchema(hash_join_out4.MakeSchema())
                     .AddLeftHashKey(p_partkey)
                     .AddLeftHashKey(s_suppkey)
                     .AddRightHashKey(l_partkey)
                     .AddRightHashKey(l_suppkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .Build();
  }

  // Make HJ5: HJ4 x orders
  std::unique_ptr<planner::AbstractPlanNode> hash_join5;
  planner::OutputSchemaHelper hash_join_out5(&expr_maker, 0);
  {
    // Left columns.
    auto n_name = hash_join_out4.GetOutput("n_name");
    auto ps_supplycost = hash_join_out4.GetOutput("ps_supplycost");
    auto l_extendedprice = hash_join_out4.GetOutput("l_extendedprice");
    auto l_discount = hash_join_out4.GetOutput("l_discount");
    auto l_quantity = hash_join_out4.GetOutput("l_quantity");
    auto l_orderkey = hash_join_out4.GetOutput("l_orderkey");
    // Right columns.
    auto o_orderkey = o_seq_scan_out.GetOutput("o_orderkey");
    auto o_orderdate = o_seq_scan_out.GetOutput("o_orderdate");
    // Output Schema.
    hash_join_out5.AddOutput("n_name", n_name);
    hash_join_out5.AddOutput("ps_supplycost", ps_supplycost);
    hash_join_out5.AddOutput("l_extendedprice", l_extendedprice);
    hash_join_out5.AddOutput("l_discount", l_discount);
    hash_join_out5.AddOutput("l_quantity", l_quantity);
    hash_join_out5.AddOutput("o_orderdate", o_orderdate);
    // Predicate.
    auto predicate = expr_maker.CompareEq(l_orderkey, o_orderkey);
    // Build
    hash_join5 = planner::HashJoinPlanNode::Builder{}
                     .AddChild(std::move(hash_join4))
                     .AddChild(std::move(o_seq_scan))
                     .SetOutputSchema(hash_join_out5.MakeSchema())
                     .AddLeftHashKey(l_orderkey)
                     .AddRightHashKey(o_orderkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .Build();
  }

  // Make projection.
  std::unique_ptr<planner::AbstractPlanNode> proj;
  planner::OutputSchemaHelper proj_out(&expr_maker, 0);
  {
    // Inputs columns.
    auto n_name = hash_join_out5.GetOutput("n_name");
    auto ps_supplycost = hash_join_out5.GetOutput("ps_supplycost");
    auto l_extendedprice = hash_join_out5.GetOutput("l_extendedprice");
    auto l_discount = hash_join_out5.GetOutput("l_discount");
    auto l_quantity = hash_join_out5.GetOutput("l_quantity");
    auto o_orderdate = hash_join_out5.GetOutput("o_orderdate");
    // Output Schema.
    proj_out.AddOutput("n_name", n_name);
    proj_out.AddOutput("o_year", expr_maker.UnaryOperator(KnownOperator::ExtractYear,
                                                          Type::IntegerType(false), o_orderdate));
    proj_out.AddOutput(
        "amount", expr_maker.OpMin(
                      // l_extendedprice * (1.0 - l_discount)
                      expr_maker.OpMul(l_extendedprice,
                                       expr_maker.OpMin(expr_maker.Constant(1.0F), l_discount)),
                      // ps_supplycost * l_quantity
                      expr_maker.OpMul(ps_supplycost, l_quantity))

    );
    // Build.
    proj = planner::ProjectionPlanNode::Builder{}
               .AddChild(std::move(hash_join5))
               .SetOutputSchema(proj_out.MakeSchema())
               .Build();
  }

  // Make the aggregate.
  std::unique_ptr<planner::AbstractPlanNode> agg;
  planner::OutputSchemaHelper agg_out(&expr_maker, 0);
  {
    // Read previous layer's output
    auto n_name = proj_out.GetOutput("n_name");
    auto o_year = proj_out.GetOutput("o_year");
    auto amount = proj_out.GetOutput("amount");
    // Make the aggregate expressions.
    auto sum_profit = expr_maker.AggSum(amount);
    // Add them to the helper.
    agg_out.AddGroupByTerm("n_name", n_name);
    agg_out.AddGroupByTerm("o_year", o_year);
    agg_out.AddAggTerm("sum_profit", sum_profit);
    // Make the output schema.
    agg_out.AddOutput("n_name", agg_out.GetGroupByTermForOutput("n_name"));
    agg_out.AddOutput("o_year", agg_out.GetGroupByTermForOutput("o_year"));
    agg_out.AddOutput("sum_profit", agg_out.GetAggTermForOutput("sum_profit"));
    // Build
    agg = planner::AggregatePlanNode::Builder{}
              .SetOutputSchema(agg_out.MakeSchema())
              .AddGroupByTerm(n_name)
              .AddGroupByTerm(o_year)
              .AddAggregateTerm(sum_profit)
              .AddChild(std::move(proj))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }

  // Make order-by.
  std::unique_ptr<planner::AbstractPlanNode> order_by;
  planner::OutputSchemaHelper order_by_out(&expr_maker, 0);
  {
    auto n_name = agg_out.GetOutput("n_name");
    auto o_year = agg_out.GetOutput("o_year");
    auto sum_profit = agg_out.GetOutput("sum_profit");
    // Outputs.
    order_by_out.AddOutput("n_name", n_name);
    order_by_out.AddOutput("o_year", o_year);
    order_by_out.AddOutput("sum_profit", sum_profit);
    // Ordering clauses.
    planner::SortKey clause_1{n_name, planner::OrderByOrderingType::ASC};
    planner::SortKey clause_2{o_year, planner::OrderByOrderingType::DESC};
    // Build.
    order_by = planner::OrderByPlanNode::Builder{}
                   .SetOutputSchema(order_by_out.MakeSchema())
                   .AddChild(std::move(agg))
                   .AddSortKey(clause_1.first, clause_1.second)
                   .AddSortKey(clause_2.first, clause_2.second)
                   .Build();
  }

  // Compile plan
  auto last_op = order_by.get();
  NoOpResultConsumer consumer;
  sql::MemoryPool memory(nullptr);
  sql::ExecutionContext exec_ctx(&memory, last_op->GetOutputSchema(), &consumer);
  auto query = CompilationContext::Compile(*last_op);
  // Run Once to force compilation
  query->Run(&exec_ctx, kExecutionMode);

  // Only time execution
  for (auto _ : state) {
    query->Run(&exec_ctx, kExecutionMode);
  }
}

BENCHMARK_DEFINE_F(TpchBenchmark, Q10)(benchmark::State &state) {
  auto accessor = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;

  // Scan orders.
  std::unique_ptr<planner::AbstractScanPlanNode> o_seq_scan;
  planner::OutputSchemaHelper o_seq_scan_out(&expr_maker, 0);
  {
    sql::Table *table = accessor->LookupTableByName("tpch.orders");
    const auto &schema = table->GetSchema();

    // Read all needed columns.
    auto o_custkey = expr_maker.CVE(schema.GetColumnInfo("o_custkey"));
    auto o_orderkey = expr_maker.CVE(schema.GetColumnInfo("o_orderkey"));
    auto o_orderdate = expr_maker.CVE(schema.GetColumnInfo("o_orderdate"));
    // Make the output schema.
    o_seq_scan_out.AddOutput("o_custkey", o_custkey);
    o_seq_scan_out.AddOutput("o_orderkey", o_orderkey);
    // Predicate.
    auto predicate = expr_maker.ConjunctionAnd(
        expr_maker.CompareGe(o_orderdate, expr_maker.Constant(1993, 10, 01)),
        expr_maker.CompareLt(o_orderdate, expr_maker.Constant(1994, 01, 01)));
    // Build.
    o_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(o_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(table->GetId())
                     .Build();
  }

  // Scan customer.
  std::unique_ptr<planner::AbstractScanPlanNode> cust_seq_scan;
  planner::OutputSchemaHelper cust_seq_scan_out(&expr_maker, 1);
  {
    sql::Table *table = accessor->LookupTableByName("tpch.customer");
    const auto &schema = table->GetSchema();

    // Read all needed columns.
    auto c_custkey = expr_maker.CVE(schema.GetColumnInfo("c_custkey"));
    auto c_nationkey = expr_maker.CVE(schema.GetColumnInfo("c_nationkey"));
    auto c_name = expr_maker.CVE(schema.GetColumnInfo("c_name"));
    auto c_acctbal = expr_maker.CVE(schema.GetColumnInfo("c_acctbal"));
    auto c_phone = expr_maker.CVE(schema.GetColumnInfo("c_phone"));
    auto c_address = expr_maker.CVE(schema.GetColumnInfo("c_address"));
    auto c_comment = expr_maker.CVE(schema.GetColumnInfo("c_comment"));
    // Make the output schema.
    cust_seq_scan_out.AddOutput("c_custkey", c_custkey);
    cust_seq_scan_out.AddOutput("c_nationkey", c_nationkey);
    cust_seq_scan_out.AddOutput("c_name", c_name);
    cust_seq_scan_out.AddOutput("c_acctbal", c_acctbal);
    cust_seq_scan_out.AddOutput("c_phone", c_phone);
    cust_seq_scan_out.AddOutput("c_address", c_address);
    cust_seq_scan_out.AddOutput("c_comment", c_comment);
    // Build.
    cust_seq_scan = planner::SeqScanPlanNode::Builder{}
                        .SetOutputSchema(cust_seq_scan_out.MakeSchema())
                        .SetScanPredicate(nullptr)
                        .SetTableOid(table->GetId())
                        .Build();
  }

  // Scan nation.
  std::unique_ptr<planner::AbstractScanPlanNode> n_seq_scan;
  planner::OutputSchemaHelper n_seq_scan_out(&expr_maker, 0);
  {
    sql::Table *table = accessor->LookupTableByName("tpch.nation");
    const auto &schema = table->GetSchema();

    // Read all needed columns.
    auto n_nationkey = expr_maker.CVE(schema.GetColumnInfo("n_nationkey"));
    auto n_name = expr_maker.CVE(schema.GetColumnInfo("n_name"));
    // Make the output schema.
    n_seq_scan_out.AddOutput("n_nationkey", n_nationkey);
    n_seq_scan_out.AddOutput("n_name", n_name);
    // Build.
    n_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(n_seq_scan_out.MakeSchema())
                     .SetScanPredicate(nullptr)
                     .SetTableOid(table->GetId())
                     .Build();
  }

  // Scan lineitem.
  std::unique_ptr<planner::AbstractScanPlanNode> l_seq_scan;
  planner::OutputSchemaHelper l_seq_scan_out(&expr_maker, 1);
  {
    sql::Table *table = accessor->LookupTableByName("tpch.lineitem");
    const auto &schema = table->GetSchema();

    // Read all needed columns.
    auto l_orderkey = expr_maker.CVE(schema.GetColumnInfo("l_orderkey"));
    auto l_returnflag = expr_maker.CVE(schema.GetColumnInfo("l_returnflag"));
    auto l_extendedprice = expr_maker.CVE(schema.GetColumnInfo("l_extendedprice"));
    auto l_discount = expr_maker.CVE(schema.GetColumnInfo("l_discount"));
    // Make the output schema.
    l_seq_scan_out.AddOutput("l_orderkey", l_orderkey);
    l_seq_scan_out.AddOutput("l_extendedprice", l_extendedprice);
    l_seq_scan_out.AddOutput("l_discount", l_discount);
    // Predicate.
    auto predicate = expr_maker.CompareEq(l_returnflag, expr_maker.Constant("R"));
    // Build.
    l_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(l_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(table->GetId())
                     .Build();
  }

  // Make HJ1: orders x customer
  std::unique_ptr<planner::AbstractPlanNode> hash_join1;
  planner::OutputSchemaHelper hash_join_out1(&expr_maker, 1);
  {
    // Left columns.
    auto o_custkey = o_seq_scan_out.GetOutput("o_custkey");
    auto o_orderkey = o_seq_scan_out.GetOutput("o_orderkey");
    // Right columns.
    auto c_custkey = cust_seq_scan_out.GetOutput("c_custkey");
    auto c_nationkey = cust_seq_scan_out.GetOutput("c_nationkey");
    auto c_name = cust_seq_scan_out.GetOutput("c_name");
    auto c_acctbal = cust_seq_scan_out.GetOutput("c_acctbal");
    auto c_phone = cust_seq_scan_out.GetOutput("c_phone");
    auto c_address = cust_seq_scan_out.GetOutput("c_address");
    auto c_comment = cust_seq_scan_out.GetOutput("c_comment");
    // Output Schema.
    hash_join_out1.AddOutput("o_orderkey", o_orderkey);
    hash_join_out1.AddOutput("c_custkey", c_custkey);
    hash_join_out1.AddOutput("c_nationkey", c_nationkey);
    hash_join_out1.AddOutput("c_name", c_name);
    hash_join_out1.AddOutput("c_acctbal", c_acctbal);
    hash_join_out1.AddOutput("c_phone", c_phone);
    hash_join_out1.AddOutput("c_address", c_address);
    hash_join_out1.AddOutput("c_comment", c_comment);
    // Predicate.
    auto predicate = expr_maker.CompareEq(o_custkey, c_custkey);
    // Build.
    hash_join1 = planner::HashJoinPlanNode::Builder{}
                     .AddChild(std::move(o_seq_scan))
                     .AddChild(std::move(cust_seq_scan))
                     .SetOutputSchema(hash_join_out1.MakeSchema())
                     .AddLeftHashKey(o_custkey)
                     .AddRightHashKey(c_custkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .Build();
  }

  // Make HJ2: nation x HJ1
  std::unique_ptr<planner::AbstractPlanNode> hash_join2;
  planner::OutputSchemaHelper hash_join_out2(&expr_maker, 0);
  {
    // Left columns.
    auto n_nationkey = n_seq_scan_out.GetOutput("n_nationkey");
    auto n_name = n_seq_scan_out.GetOutput("n_name");
    // Right columns.
    auto o_orderkey = hash_join_out1.GetOutput("o_orderkey");
    auto c_nationkey = hash_join_out1.GetOutput("c_nationkey");
    auto c_custkey = hash_join_out1.GetOutput("c_custkey");
    auto c_name = hash_join_out1.GetOutput("c_name");
    auto c_acctbal = hash_join_out1.GetOutput("c_acctbal");
    auto c_phone = hash_join_out1.GetOutput("c_phone");
    auto c_address = hash_join_out1.GetOutput("c_address");
    auto c_comment = hash_join_out1.GetOutput("c_comment");
    // Output Schema.
    hash_join_out2.AddOutput("n_name", n_name);
    hash_join_out2.AddOutput("o_orderkey", o_orderkey);
    hash_join_out2.AddOutput("c_custkey", c_custkey);
    hash_join_out2.AddOutput("c_name", c_name);
    hash_join_out2.AddOutput("c_acctbal", c_acctbal);
    hash_join_out2.AddOutput("c_phone", c_phone);
    hash_join_out2.AddOutput("c_address", c_address);
    hash_join_out2.AddOutput("c_comment", c_comment);
    // Predicate.
    auto predicate = expr_maker.CompareEq(n_nationkey, c_nationkey);
    // Build.
    hash_join2 = planner::HashJoinPlanNode::Builder{}
                     .AddChild(std::move(n_seq_scan))
                     .AddChild(std::move(hash_join1))
                     .SetOutputSchema(hash_join_out2.MakeSchema())
                     .AddLeftHashKey(n_nationkey)
                     .AddRightHashKey(c_nationkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .Build();
  }

  // Make HJ3: HJ2 x lineitem
  std::unique_ptr<planner::AbstractPlanNode> hash_join3;
  planner::OutputSchemaHelper hash_join_out3(&expr_maker, 0);
  {
    // Left columns.
    auto n_name = hash_join_out2.GetOutput("n_name");
    auto o_orderkey = hash_join_out2.GetOutput("o_orderkey");
    auto c_custkey = hash_join_out2.GetOutput("c_custkey");
    auto c_name = hash_join_out2.GetOutput("c_name");
    auto c_acctbal = hash_join_out2.GetOutput("c_acctbal");
    auto c_phone = hash_join_out2.GetOutput("c_phone");
    auto c_address = hash_join_out2.GetOutput("c_address");
    auto c_comment = hash_join_out2.GetOutput("c_comment");
    // Right columns.
    auto l_orderkey = l_seq_scan_out.GetOutput("l_orderkey");
    auto l_extendedprice = l_seq_scan_out.GetOutput("l_extendedprice");
    auto l_discount = l_seq_scan_out.GetOutput("l_discount");
    // Output Schema.
    hash_join_out3.AddOutput("n_name", n_name);
    hash_join_out3.AddOutput("c_custkey", c_custkey);
    hash_join_out3.AddOutput("c_name", c_name);
    hash_join_out3.AddOutput("c_acctbal", c_acctbal);
    hash_join_out3.AddOutput("c_phone", c_phone);
    hash_join_out3.AddOutput("c_address", c_address);
    hash_join_out3.AddOutput("c_comment", c_comment);
    hash_join_out3.AddOutput("l_extendedprice", l_extendedprice);
    hash_join_out3.AddOutput("l_discount", l_discount);
    // Predicate.
    auto predicate = expr_maker.CompareEq(o_orderkey, l_orderkey);
    // Build.
    hash_join3 = planner::HashJoinPlanNode::Builder{}
                     .AddChild(std::move(hash_join2))
                     .AddChild(std::move(l_seq_scan))
                     .SetOutputSchema(hash_join_out3.MakeSchema())
                     .AddLeftHashKey(o_orderkey)
                     .AddRightHashKey(l_orderkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .Build();
  }

  // Make the aggregate
  std::unique_ptr<planner::AbstractPlanNode> agg;
  planner::OutputSchemaHelper agg_out(&expr_maker, 0);
  {
    // Read previous layer's output
    auto n_name = hash_join_out3.GetOutput("n_name");
    auto c_custkey = hash_join_out3.GetOutput("c_custkey");
    auto c_name = hash_join_out3.GetOutput("c_name");
    auto c_acctbal = hash_join_out3.GetOutput("c_acctbal");
    auto c_phone = hash_join_out3.GetOutput("c_phone");
    auto c_address = hash_join_out3.GetOutput("c_address");
    auto c_comment = hash_join_out3.GetOutput("c_comment");
    auto l_extendedprice = hash_join_out3.GetOutput("l_extendedprice");
    auto l_discount = hash_join_out3.GetOutput("l_discount");
    // Make the aggregate expressions
    auto revenue = expr_maker.AggSum(
        expr_maker.OpMul(l_extendedprice, expr_maker.OpMin(expr_maker.Constant(1.0f), l_discount)));
    // Add them to the helper.
    agg_out.AddGroupByTerm("c_custkey", c_custkey);
    agg_out.AddGroupByTerm("c_name", c_name);
    agg_out.AddGroupByTerm("c_acctbal", c_acctbal);
    agg_out.AddGroupByTerm("c_phone", c_phone);
    agg_out.AddGroupByTerm("n_name", n_name);
    agg_out.AddGroupByTerm("c_address", c_address);
    agg_out.AddGroupByTerm("c_comment", c_comment);
    agg_out.AddAggTerm("revenue", revenue);
    // Make the output schema
    agg_out.AddOutput("c_custkey", agg_out.GetGroupByTermForOutput("c_custkey"));
    agg_out.AddOutput("c_name", agg_out.GetGroupByTermForOutput("c_name"));
    agg_out.AddOutput("c_acctbal", agg_out.GetGroupByTermForOutput("c_acctbal"));
    agg_out.AddOutput("c_phone", agg_out.GetGroupByTermForOutput("c_phone"));
    agg_out.AddOutput("n_name", agg_out.GetGroupByTermForOutput("n_name"));
    agg_out.AddOutput("c_address", agg_out.GetGroupByTermForOutput("c_address"));
    agg_out.AddOutput("c_comment", agg_out.GetGroupByTermForOutput("c_comment"));
    agg_out.AddOutput("revenue", agg_out.GetAggTermForOutput("revenue"));
    // Build
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(agg_out.MakeSchema())
              .AddGroupByTerm(c_custkey)
              .AddGroupByTerm(c_name)
              .AddGroupByTerm(c_acctbal)
              .AddGroupByTerm(c_phone)
              .AddGroupByTerm(n_name)
              .AddGroupByTerm(c_address)
              .AddGroupByTerm(c_comment)
              .AddAggregateTerm(revenue)
              .AddChild(std::move(hash_join3))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }

  // Make final sort
  std::unique_ptr<planner::AbstractPlanNode> order_by;
  planner::OutputSchemaHelper order_by_out(&expr_maker, 0);
  {
    // Output colums
    auto c_custkey = agg_out.GetOutput("c_custkey");
    auto c_name = agg_out.GetOutput("c_name");
    auto c_acctbal = agg_out.GetOutput("c_acctbal");
    auto c_phone = agg_out.GetOutput("c_phone");
    auto n_name = agg_out.GetOutput("n_name");
    auto c_address = agg_out.GetOutput("c_address");
    auto c_comment = agg_out.GetOutput("c_comment");
    auto revenue = agg_out.GetOutput("revenue");
    order_by_out.AddOutput("c_custkey", c_custkey);
    order_by_out.AddOutput("c_name", c_name);
    order_by_out.AddOutput("revenue", revenue);
    order_by_out.AddOutput("c_acctbal", c_acctbal);
    order_by_out.AddOutput("n_name", n_name);
    order_by_out.AddOutput("c_address", c_address);
    order_by_out.AddOutput("c_phone", c_phone);
    order_by_out.AddOutput("c_comment", c_comment);
    // Build
    planner::OrderByPlanNode::Builder builder;
    order_by = builder.SetOutputSchema(order_by_out.MakeSchema())
                   .AddChild(std::move(agg))
                   .AddSortKey(revenue, planner::OrderByOrderingType::DESC)
                   .SetLimit(20)
                   .Build();
  }

  // Compile plan
  auto last_op = order_by.get();
  NoOpResultConsumer consumer;
  sql::MemoryPool memory(nullptr);
  sql::ExecutionContext exec_ctx(&memory, last_op->GetOutputSchema(), &consumer);
  auto query = CompilationContext::Compile(*last_op);
  // Run Once to force compilation
  query->Run(&exec_ctx, kExecutionMode);

  // Only time execution
  for (auto _ : state) {
    query->Run(&exec_ctx, kExecutionMode);
  }
}

BENCHMARK_DEFINE_F(TpchBenchmark, Q11)(benchmark::State &state) {
  auto accessor = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;
  // Supplier.
  sql::Table *s_table = accessor->LookupTableByName("tpch.supplier");
  const auto &s_schema = s_table->GetSchema();
  // Partsupp.
  sql::Table *ps_table = accessor->LookupTableByName("tpch.partsupp");
  const auto &ps_schema = ps_table->GetSchema();
  // Nation.
  sql::Table *n_table = accessor->LookupTableByName("tpch.nation");
  const auto &n_schema = n_table->GetSchema();

  std::unique_ptr<planner::AbstractPlanNode> n_seq_scan1;
  planner::OutputSchemaHelper n_seq_scan_out1{&expr_maker, 0};
  {
    // Read all needed columns
    auto n_name = expr_maker.CVE(n_schema.GetColumnInfo("n_name"));
    auto n_nationkey = expr_maker.CVE(n_schema.GetColumnInfo("n_nationkey"));
    // Make the output schema
    n_seq_scan_out1.AddOutput("n_nationkey", n_nationkey);
    auto schema = n_seq_scan_out1.MakeSchema();
    // Predicate
    auto germany = expr_maker.Constant("GERMANY");
    auto predicate = expr_maker.CompareEq(n_name, germany);
    // Build
    planner::SeqScanPlanNode::Builder builder;
    n_seq_scan1 = builder.SetOutputSchema(std::move(schema))
                      .SetScanPredicate(predicate)
                      .SetTableOid(n_table->GetId())
                      .Build();
  }

  std::unique_ptr<planner::AbstractPlanNode> n_seq_scan2;
  planner::OutputSchemaHelper n_seq_scan_out2{&expr_maker, 0};
  {
    // Read all needed columns
    auto n_name = expr_maker.CVE(n_schema.GetColumnInfo("n_name"));
    auto n_nationkey = expr_maker.CVE(n_schema.GetColumnInfo("n_nationkey"));
    // Make the output schema
    n_seq_scan_out2.AddOutput("n_nationkey", n_nationkey);
    auto schema = n_seq_scan_out2.MakeSchema();
    // Predicate
    auto germany = expr_maker.Constant("GERMANY");
    auto predicate = expr_maker.CompareEq(n_name, germany);
    // Build
    planner::SeqScanPlanNode::Builder builder;
    n_seq_scan2 = builder.SetOutputSchema(std::move(schema))
                      .SetScanPredicate(predicate)
                      .SetTableOid(n_table->GetId())
                      .Build();
  }

  // Scan supplier
  std::unique_ptr<planner::AbstractPlanNode> s_seq_scan1;
  planner::OutputSchemaHelper s_seq_scan_out1{&expr_maker, 1};
  {
    // Read all needed columns
    auto s_suppkey = expr_maker.CVE(s_schema.GetColumnInfo("s_suppkey"));
    auto s_nationkey = expr_maker.CVE(s_schema.GetColumnInfo("s_nationkey"));
    // Make the output schema
    s_seq_scan_out1.AddOutput("s_suppkey", s_suppkey);
    s_seq_scan_out1.AddOutput("s_nationkey", s_nationkey);
    auto schema = s_seq_scan_out1.MakeSchema();

    // Build
    planner::SeqScanPlanNode::Builder builder;
    s_seq_scan1 = builder.SetOutputSchema(std::move(schema))
                      .SetScanPredicate(nullptr)
                      .SetTableOid(s_table->GetId())
                      .Build();
  }

  std::unique_ptr<planner::AbstractPlanNode> s_seq_scan2;
  planner::OutputSchemaHelper s_seq_scan_out2{&expr_maker, 1};
  {
    // Read all needed columns
    auto s_suppkey = expr_maker.CVE(s_schema.GetColumnInfo("s_suppkey"));
    auto s_nationkey = expr_maker.CVE(s_schema.GetColumnInfo("s_nationkey"));
    // Make the output schema
    s_seq_scan_out2.AddOutput("s_suppkey", s_suppkey);
    s_seq_scan_out2.AddOutput("s_nationkey", s_nationkey);
    auto schema = s_seq_scan_out2.MakeSchema();

    // Build
    planner::SeqScanPlanNode::Builder builder;
    s_seq_scan2 = builder.SetOutputSchema(std::move(schema))
                      .SetScanPredicate(nullptr)
                      .SetTableOid(s_table->GetId())
                      .Build();
  }

  // Scan partsupp
  std::unique_ptr<planner::AbstractPlanNode> ps_seq_scan1;
  planner::OutputSchemaHelper ps_seq_scan_out1{&expr_maker, 1};
  {
    // Read all needed columns
    auto ps_suppkey = expr_maker.CVE(ps_schema.GetColumnInfo("ps_suppkey"));
    auto ps_partkey = expr_maker.CVE(ps_schema.GetColumnInfo("ps_partkey"));
    auto ps_supplycost = expr_maker.CVE(ps_schema.GetColumnInfo("ps_supplycost"));
    auto ps_availqty = expr_maker.CVE(ps_schema.GetColumnInfo("ps_availqty"));

    // Make the output schema
    ps_seq_scan_out1.AddOutput("ps_suppkey", ps_suppkey);
    ps_seq_scan_out1.AddOutput("ps_partkey", ps_partkey);
    ps_seq_scan_out1.AddOutput("ps_supplycost", ps_supplycost);
    ps_seq_scan_out1.AddOutput("ps_availqty", ps_availqty);
    auto schema = ps_seq_scan_out1.MakeSchema();
    // Build
    planner::SeqScanPlanNode::Builder builder;
    ps_seq_scan1 = builder.SetOutputSchema(std::move(schema))
                       .SetScanPredicate(nullptr)
                       .SetTableOid(ps_table->GetId())
                       .Build();
  }

  std::unique_ptr<planner::AbstractPlanNode> ps_seq_scan2;
  planner::OutputSchemaHelper ps_seq_scan_out2{&expr_maker, 1};
  {
    // Read all needed columns
    auto ps_suppkey = expr_maker.CVE(ps_schema.GetColumnInfo("ps_suppkey"));
    auto ps_partkey = expr_maker.CVE(ps_schema.GetColumnInfo("ps_partkey"));
    auto ps_supplycost = expr_maker.CVE(ps_schema.GetColumnInfo("ps_supplycost"));
    auto ps_availqty = expr_maker.CVE(ps_schema.GetColumnInfo("ps_availqty"));

    // Make the output schema
    ps_seq_scan_out2.AddOutput("ps_suppkey", ps_suppkey);
    ps_seq_scan_out2.AddOutput("ps_partkey", ps_partkey);
    ps_seq_scan_out2.AddOutput("ps_supplycost", ps_supplycost);
    ps_seq_scan_out2.AddOutput("ps_availqty", ps_availqty);
    auto schema = ps_seq_scan_out2.MakeSchema();
    // Build
    planner::SeqScanPlanNode::Builder builder;
    ps_seq_scan2 = builder.SetOutputSchema(std::move(schema))
                       .SetScanPredicate(nullptr)
                       .SetTableOid(ps_table->GetId())
                       .Build();
  }

  // Hash Join 1
  std::unique_ptr<planner::AbstractPlanNode> hash_join1_1;
  planner::OutputSchemaHelper hash_join_out1_1{&expr_maker, 0};
  {
    // Read left columns
    auto n_nationkey = n_seq_scan_out1.GetOutput("n_nationkey");
    // Read right columns
    auto s_suppkey = s_seq_scan_out1.GetOutput("s_suppkey");
    auto s_nationkey = s_seq_scan_out1.GetOutput("s_nationkey");
    // Make Schema
    hash_join_out1_1.AddOutput("s_suppkey", s_suppkey);
    auto schema = hash_join_out1_1.MakeSchema();
    // Make predicate
    auto predicate = expr_maker.CompareEq(n_nationkey, s_nationkey);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join1_1 = builder.SetOutputSchema(std::move(schema))
                       .AddChild(std::move(n_seq_scan1))
                       .AddChild(std::move(s_seq_scan1))
                       .SetJoinType(planner::LogicalJoinType::INNER)
                       .SetJoinPredicate(predicate)
                       .AddLeftHashKey(n_nationkey)
                       .AddRightHashKey(s_nationkey)
                       .Build();
  }

  std::unique_ptr<planner::AbstractPlanNode> hash_join1_2;
  planner::OutputSchemaHelper hash_join_out1_2{&expr_maker, 0};
  {
    // Read left columns
    auto n_nationkey = n_seq_scan_out2.GetOutput("n_nationkey");
    // Read right columns
    auto s_suppkey = s_seq_scan_out2.GetOutput("s_suppkey");
    auto s_nationkey = s_seq_scan_out2.GetOutput("s_nationkey");
    // Make Schema
    hash_join_out1_2.AddOutput("s_suppkey", s_suppkey);
    auto schema = hash_join_out1_2.MakeSchema();
    // Make predicate
    auto predicate = expr_maker.CompareEq(n_nationkey, s_nationkey);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join1_2 = builder.SetOutputSchema(std::move(schema))
                       .AddChild(std::move(n_seq_scan2))
                       .AddChild(std::move(s_seq_scan2))
                       .SetJoinType(planner::LogicalJoinType::INNER)
                       .SetJoinPredicate(predicate)
                       .AddLeftHashKey(n_nationkey)
                       .AddRightHashKey(s_nationkey)
                       .Build();
  }

  // Hash Join 2
  std::unique_ptr<planner::AbstractPlanNode> hash_join2_1;
  planner::OutputSchemaHelper hash_join_out2_1{&expr_maker, 0};
  {
    // Read left columns
    auto s_suppkey = hash_join_out1_1.GetOutput("s_suppkey");
    // Read right columns
    auto ps_suppkey = ps_seq_scan_out1.GetOutput("ps_suppkey");
    // auto ps_partkey = ps_seq_scan_out1.GetOutput("ps_partkey");
    auto ps_supplycost = ps_seq_scan_out1.GetOutput("ps_supplycost");
    auto ps_availqty = ps_seq_scan_out1.GetOutput("ps_availqty");
    // Make Schema
    hash_join_out2_1.AddOutput("ps_supplycost", ps_supplycost);
    hash_join_out2_1.AddOutput("ps_availqty", ps_availqty);
    auto schema = hash_join_out2_1.MakeSchema();
    // Make predicate
    auto predicate = expr_maker.CompareEq(s_suppkey, ps_suppkey);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join2_1 = builder.SetOutputSchema(std::move(schema))
                       .AddChild(std::move(hash_join1_1))
                       .AddChild(std::move(ps_seq_scan1))
                       .SetJoinType(planner::LogicalJoinType::INNER)
                       .SetJoinPredicate(predicate)
                       .AddLeftHashKey(s_suppkey)
                       .AddRightHashKey(ps_suppkey)
                       .Build();
  }

  std::unique_ptr<planner::AbstractPlanNode> hash_join2_2;
  planner::OutputSchemaHelper hash_join_out2_2{&expr_maker, 0};
  {
    // Read left columns
    auto s_suppkey = hash_join_out1_2.GetOutput("s_suppkey");
    // Read right columns
    auto ps_suppkey = ps_seq_scan_out2.GetOutput("ps_suppkey");
    auto ps_partkey = ps_seq_scan_out2.GetOutput("ps_partkey");
    auto ps_supplycost = ps_seq_scan_out2.GetOutput("ps_supplycost");
    auto ps_availqty = ps_seq_scan_out2.GetOutput("ps_availqty");
    // Make Schema
    hash_join_out2_2.AddOutput("ps_supplycost", ps_supplycost);
    hash_join_out2_2.AddOutput("ps_availqty", ps_availqty);
    hash_join_out2_2.AddOutput("ps_partkey", ps_partkey);
    auto schema = hash_join_out2_2.MakeSchema();
    // Make predicate
    auto predicate = expr_maker.CompareEq(s_suppkey, ps_suppkey);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join2_2 = builder.SetOutputSchema(std::move(schema))
                       .AddChild(std::move(hash_join1_2))
                       .AddChild(std::move(ps_seq_scan2))
                       .SetJoinType(planner::LogicalJoinType::INNER)
                       .SetJoinPredicate(predicate)
                       .AddLeftHashKey(s_suppkey)
                       .AddRightHashKey(ps_suppkey)
                       .Build();
  }

  // Aggregates
  // Make the aggregate
  std::unique_ptr<planner::AbstractPlanNode> agg1;
  planner::OutputSchemaHelper agg_out1{&expr_maker, 0};
  {
    // Read previous layer's output
    auto ps_supplycost = hash_join_out2_1.GetOutput("ps_supplycost");
    auto ps_availqty = hash_join_out2_1.GetOutput("ps_availqty");
    // Make the aggregate expressions
    auto value =
        expr_maker.OpMul(ps_supplycost, expr_maker.OpCast(ps_availqty, Type::RealType(false)));
    auto value_sum = expr_maker.AggSum(value);
    // Add them to the helper.
    agg_out1.AddAggTerm("value_sum", value_sum);
    // Make the output schema
    agg_out1.AddOutput("value_sum", agg_out1.GetAggTermForOutput("value_sum"));
    auto schema = agg_out1.MakeSchema();
    // Build
    planner::AggregatePlanNode::Builder builder;
    agg1 = builder.SetOutputSchema(std::move(schema))
               .AddAggregateTerm(value_sum)
               .AddChild(std::move(hash_join2_1))
               .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
               .SetHavingClausePredicate(nullptr)
               .Build();
  }

  std::unique_ptr<planner::AbstractPlanNode> agg2;
  planner::OutputSchemaHelper agg_out2{&expr_maker, 1};
  {
    // Read previous layer's output
    auto ps_supplycost = hash_join_out2_2.GetOutput("ps_supplycost");
    auto ps_availqty = hash_join_out2_2.GetOutput("ps_availqty");
    auto ps_partkey = hash_join_out2_2.GetOutput("ps_partkey");
    // Make the aggregate expressions
    auto value =
        expr_maker.OpMul(ps_supplycost, expr_maker.OpCast(ps_availqty, Type::RealType(false)));
    auto value_sum = expr_maker.AggSum(value);
    // Add them to the helper.
    agg_out2.AddGroupByTerm("ps_partkey", ps_partkey);
    agg_out2.AddAggTerm("value_sum", value_sum);
    // Make the output schema
    agg_out2.AddOutput("ps_partkey", agg_out2.GetGroupByTermForOutput("ps_partkey"));
    agg_out2.AddOutput("value_sum", agg_out2.GetAggTermForOutput("value_sum"));
    auto schema = agg_out2.MakeSchema();
    // Build
    planner::AggregatePlanNode::Builder builder;
    agg2 = builder.SetOutputSchema(std::move(schema))
               .AddGroupByTerm(ps_partkey)
               .AddAggregateTerm(value_sum)
               .AddChild(std::move(hash_join2_2))
               .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
               .SetHavingClausePredicate(nullptr)
               .Build();
  }

  // BNL
  std::unique_ptr<planner::AbstractPlanNode> bnl;
  planner::OutputSchemaHelper bnl_out{&expr_maker, 0};
  {
    // Left
    auto value_sum1 = agg_out1.GetOutput("value_sum");
    // Right
    auto ps_partkey = agg_out2.GetOutput("ps_partkey");
    auto value_sum2 = agg_out2.GetOutput("value_sum");
    // Make the output schema
    bnl_out.AddOutput("value_sum", value_sum2);
    bnl_out.AddOutput("ps_partkey", ps_partkey);
    auto schema = bnl_out.MakeSchema();
    // Predicate
    auto left_comp = expr_maker.OpMul(value_sum1, expr_maker.Constant(0.00001f));
    auto predicate = expr_maker.CompareGt(value_sum2, left_comp);
    // Build
    planner::NestedLoopJoinPlanNode::Builder builder;
    bnl = builder.AddChild(std::move(agg1))
              .AddChild(std::move(agg2))
              .SetOutputSchema(std::move(schema))
              .SetJoinType(planner::LogicalJoinType::INNER)
              .SetJoinPredicate(predicate)
              .Build();
  }

  // Sort
  // Order By
  std::unique_ptr<planner::AbstractPlanNode> order_by;
  planner::OutputSchemaHelper order_by_out{&expr_maker, 0};
  {
    // Read previous layer
    auto ps_partkey = bnl_out.GetOutput("ps_partkey");
    auto value_sum = bnl_out.GetOutput("value_sum");

    order_by_out.AddOutput("ps_partkey", ps_partkey);
    order_by_out.AddOutput("value_sum", value_sum);
    auto schema = order_by_out.MakeSchema();
    // Order By Clause
    planner::SortKey clause1{value_sum, planner::OrderByOrderingType::DESC};
    // Build
    planner::OrderByPlanNode::Builder builder;
    order_by = builder.SetOutputSchema(std::move(schema))
                   .AddChild(std::move(bnl))
                   .AddSortKey(clause1.first, clause1.second)
                   .Build();
  }

  // Compile plan
  auto last_op = order_by.get();
  NoOpResultConsumer consumer;
  sql::MemoryPool memory(nullptr);
  sql::ExecutionContext exec_ctx(&memory, last_op->GetOutputSchema(), &consumer);
  auto query = CompilationContext::Compile(*last_op);
  // Run Once to force compilation
  query->Run(&exec_ctx, kExecutionMode);

  // Only time execution
  for (auto _ : state) {
    query->Run(&exec_ctx, kExecutionMode);
  }
}

BENCHMARK_DEFINE_F(TpchBenchmark, Q12)(benchmark::State &state) {
  auto accessor = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;

  // Scan lineitem.
  std::unique_ptr<planner::AbstractScanPlanNode> l_seq_scan;
  planner::OutputSchemaHelper l_seq_scan_out(&expr_maker, 0);
  {
    sql::Table *table = accessor->LookupTableByName("tpch.lineitem");
    const auto &schema = table->GetSchema();

    // Read all needed columns.
    auto l_orderkey = expr_maker.CVE(schema.GetColumnInfo("l_orderkey"));
    auto l_shipmode = expr_maker.CVE(schema.GetColumnInfo("l_shipmode"));
    auto l_commitdate = expr_maker.CVE(schema.GetColumnInfo("l_commitdate"));
    auto l_shipdate = expr_maker.CVE(schema.GetColumnInfo("l_shipdate"));
    auto l_receiptdate = expr_maker.CVE(schema.GetColumnInfo("l_receiptdate"));
    // Make the output schema.
    l_seq_scan_out.AddOutput("l_orderkey", l_orderkey);
    l_seq_scan_out.AddOutput("l_shipmode", l_shipmode);
    // Predicate.
    auto pred_shipmode =
        expr_maker.ConjunctionOr(expr_maker.CompareEq(l_shipmode, expr_maker.Constant("MAIL")),
                                 expr_maker.CompareEq(l_shipmode, expr_maker.Constant("SHIP")));
    auto pred_commitdate = expr_maker.CompareLt(l_commitdate, l_receiptdate);
    auto pred_shipdate = expr_maker.CompareLt(l_shipdate, l_commitdate);
    auto pred_receiptdate = expr_maker.ConjunctionAnd(
        expr_maker.CompareGe(l_receiptdate, expr_maker.Constant(1994, 01, 01)),
        expr_maker.CompareLt(l_receiptdate, expr_maker.Constant(1995, 01, 01)));
    auto predicate = expr_maker.ConjunctionAnd(
        pred_shipmode,
        expr_maker.ConjunctionAnd(pred_commitdate,
                                  expr_maker.ConjunctionAnd(pred_shipdate, pred_receiptdate)));
    // Build.
    l_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(l_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(table->GetId())
                     .Build();
  }

  // Scan orders.
  std::unique_ptr<planner::AbstractScanPlanNode> o_seq_scan;
  planner::OutputSchemaHelper o_seq_scan_out(&expr_maker, 1);
  {
    sql::Table *table = accessor->LookupTableByName("tpch.orders");
    const auto &schema = table->GetSchema();

    // Read all needed columns.
    auto o_orderkey = expr_maker.CVE(schema.GetColumnInfo("o_orderkey"));
    auto o_orderpriority = expr_maker.CVE(schema.GetColumnInfo("o_orderpriority"));
    auto o_high_line_count = expr_maker.Case(
        {
            // when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1
            {expr_maker.ConjunctionOr(
                 expr_maker.CompareEq(o_orderpriority, expr_maker.Constant("1-URGENT")),
                 expr_maker.CompareEq(o_orderpriority, expr_maker.Constant("2-HIGH"))),
             expr_maker.Constant(1)},
        },
        // else 0
        expr_maker.Constant(0));
    auto o_low_line_count = expr_maker.Case(
        {
            // when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1
            {expr_maker.ConjunctionAnd(
                 expr_maker.CompareNeq(o_orderpriority, expr_maker.Constant("1-URGENT")),
                 expr_maker.CompareNeq(o_orderpriority, expr_maker.Constant("2-HIGH"))),
             expr_maker.Constant(1)},
        },
        // else 0
        expr_maker.Constant(0));
    // Make the output schema.
    o_seq_scan_out.AddOutput("o_orderkey", o_orderkey);
    o_seq_scan_out.AddOutput("o_high_line_count", o_high_line_count);
    o_seq_scan_out.AddOutput("o_low_line_count", o_low_line_count);
    // Build.
    o_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(o_seq_scan_out.MakeSchema())
                     .SetScanPredicate(nullptr)
                     .SetTableOid(table->GetId())
                     .Build();
  }

  // Make HJ1: lineitem x orders
  std::unique_ptr<planner::AbstractPlanNode> hash_join1;
  planner::OutputSchemaHelper hash_join_out1(&expr_maker, 0);
  {
    // Left columns.
    auto l_orderkey = l_seq_scan_out.GetOutput("l_orderkey");
    auto l_shipmode = l_seq_scan_out.GetOutput("l_shipmode");
    // Right columns.
    auto o_orderkey = o_seq_scan_out.GetOutput("o_orderkey");
    auto o_high_line_count = o_seq_scan_out.GetOutput("o_high_line_count");
    auto o_low_line_count = o_seq_scan_out.GetOutput("o_low_line_count");
    // Output Schema.
    hash_join_out1.AddOutput("l_shipmode", l_shipmode);
    hash_join_out1.AddOutput("o_high_line_count", o_high_line_count);
    hash_join_out1.AddOutput("o_low_line_count", o_low_line_count);
    // Predicate.
    auto predicate = expr_maker.CompareEq(l_orderkey, o_orderkey);
    // Build.
    hash_join1 = planner::HashJoinPlanNode::Builder{}
                     .AddChild(std::move(l_seq_scan))
                     .AddChild(std::move(o_seq_scan))
                     .SetOutputSchema(hash_join_out1.MakeSchema())
                     .AddLeftHashKey(l_orderkey)
                     .AddRightHashKey(o_orderkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .Build();
  }

  // Make the aggregate
  std::unique_ptr<planner::AbstractPlanNode> agg;
  planner::OutputSchemaHelper agg_out(&expr_maker, 0);
  {
    // Read previous layer's output
    auto l_shipmode = hash_join_out1.GetOutput("l_shipmode");
    auto o_high_line_count = hash_join_out1.GetOutput("o_high_line_count");
    auto o_low_line_count = hash_join_out1.GetOutput("o_low_line_count");
    // Make the aggregate expressions
    auto high_line_count = expr_maker.AggSum(o_high_line_count);
    auto low_line_count = expr_maker.AggSum(o_low_line_count);
    // Add them to the helper.
    agg_out.AddGroupByTerm("l_shipmode", l_shipmode);
    agg_out.AddAggTerm("high_line_count", high_line_count);
    agg_out.AddAggTerm("low_line_count", low_line_count);
    // Make the output schema
    agg_out.AddOutput("l_shipmode", agg_out.GetGroupByTermForOutput("l_shipmode"));
    agg_out.AddOutput("high_line_count", agg_out.GetAggTermForOutput("high_line_count"));
    agg_out.AddOutput("low_line_count", agg_out.GetAggTermForOutput("low_line_count"));
    // Build
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(agg_out.MakeSchema())
              .AddGroupByTerm(l_shipmode)
              .AddAggregateTerm(high_line_count)
              .AddAggregateTerm(low_line_count)
              .AddChild(std::move(hash_join1))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }

  // Make final sort
  std::unique_ptr<planner::AbstractPlanNode> order_by;
  planner::OutputSchemaHelper order_by_out(&expr_maker, 0);
  {
    // Output colums
    auto l_shipmode = agg_out.GetOutput("l_shipmode");
    auto high_line_count = agg_out.GetOutput("high_line_count");
    auto low_line_count = agg_out.GetOutput("low_line_count");
    order_by_out.AddOutput("l_shipmode", l_shipmode);
    order_by_out.AddOutput("high_line_count", high_line_count);
    order_by_out.AddOutput("low_line_count", low_line_count);
    // Build
    planner::OrderByPlanNode::Builder builder;
    order_by = builder.SetOutputSchema(order_by_out.MakeSchema())
                   .AddChild(std::move(agg))
                   .AddSortKey(l_shipmode, planner::OrderByOrderingType::ASC)
                   .Build();
  }

  // Compile plan
  auto last_op = order_by.get();
  NoOpResultConsumer consumer;
  sql::MemoryPool memory(nullptr);
  sql::ExecutionContext exec_ctx(&memory, last_op->GetOutputSchema(), &consumer);
  auto query = CompilationContext::Compile(*last_op);
  // Run Once to force compilation
  query->Run(&exec_ctx, kExecutionMode);

  // Only time execution
  for (auto _ : state) {
    query->Run(&exec_ctx, kExecutionMode);
  }
}

BENCHMARK_DEFINE_F(TpchBenchmark, Q14)(benchmark::State &state) {
  auto accessor = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;

  // Scan lineitem.
  std::unique_ptr<planner::AbstractPlanNode> l_seq_scan;
  planner::OutputSchemaHelper l_seq_scan_out(&expr_maker, 0);
  {
    sql::Table *l_table = accessor->LookupTableByName("tpch.lineitem");
    const auto &l_schema = l_table->GetSchema();

    // Read all needed columns.
    auto l_extendedprice = expr_maker.CVE(l_schema.GetColumnInfo("l_extendedprice"));
    auto l_discount = expr_maker.CVE(l_schema.GetColumnInfo("l_discount"));
    auto l_shipdate = expr_maker.CVE(l_schema.GetColumnInfo("l_shipdate"));
    auto l_partkey = expr_maker.CVE(l_schema.GetColumnInfo("l_partkey"));
    // Make the output schema.
    l_seq_scan_out.AddOutput("l_extendedprice", l_extendedprice);
    l_seq_scan_out.AddOutput("l_discount", l_discount);
    l_seq_scan_out.AddOutput("l_partkey", l_partkey);
    // Predicate.
    auto predicate = expr_maker.ConjunctionAnd(
        expr_maker.CompareGe(l_shipdate, expr_maker.Constant(1995, 9, 1)),
        expr_maker.CompareLt(l_shipdate, expr_maker.Constant(1995, 10, 1)));
    // Build.
    l_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(l_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(l_table->GetId())
                     .Build();
  }

  // Scan part.
  std::unique_ptr<planner::AbstractPlanNode> p_seq_scan;
  planner::OutputSchemaHelper p_seq_scan_out(&expr_maker, 1);
  {
    sql::Table *p_table = accessor->LookupTableByName("tpch.part");
    const auto &p_schema = p_table->GetSchema();

    // Read all needed columns.
    auto p_partkey = expr_maker.CVE(p_schema.GetColumnInfo("p_partkey"));
    auto p_type = expr_maker.CVE(p_schema.GetColumnInfo("p_type"));
    // Make the output schema.
    p_seq_scan_out.AddOutput("p_partkey", p_partkey);
    p_seq_scan_out.AddOutput("p_type", p_type);
    // Build.
    p_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(p_seq_scan_out.MakeSchema())
                     .SetScanPredicate(nullptr)
                     .SetTableOid(p_table->GetId())
                     .Build();
  }

  // Make HJ1: lineitem x part.
  std::unique_ptr<planner::AbstractPlanNode> hash_join1;
  planner::OutputSchemaHelper hash_join_out1(&expr_maker, 0);
  {
    // Left columns.
    auto l_extendedprice = l_seq_scan_out.GetOutput("l_extendedprice");
    auto l_discount = l_seq_scan_out.GetOutput("l_discount");
    auto l_partkey = l_seq_scan_out.GetOutput("l_partkey");
    // Right columns.
    auto p_partkey = p_seq_scan_out.GetOutput("p_partkey");
    auto p_type = p_seq_scan_out.GetOutput("p_type");
    // Output Schema.
    hash_join_out1.AddOutput("l_extendedprice", l_extendedprice);
    hash_join_out1.AddOutput("l_discount", l_discount);
    hash_join_out1.AddOutput("p_type", p_type);
    // Predicate.
    auto predicate = expr_maker.CompareEq(l_partkey, p_partkey);
    // Build.
    hash_join1 = planner::HashJoinPlanNode::Builder{}
                     .AddChild(std::move(l_seq_scan))
                     .AddChild(std::move(p_seq_scan))
                     .SetOutputSchema(hash_join_out1.MakeSchema())
                     .AddLeftHashKey(l_partkey)
                     .AddRightHashKey(p_partkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .Build();
  }

  // Make the aggregate.
  std::unique_ptr<planner::AbstractPlanNode> agg;
  planner::OutputSchemaHelper agg_out(&expr_maker, 0);
  {
    // Read previous layer's output
    auto l_extendedprice = hash_join_out1.GetOutput("l_extendedprice");
    auto l_discount = hash_join_out1.GetOutput("l_discount");
    auto p_type = hash_join_out1.GetOutput("p_type");
    // Make the aggregate expressions.
    auto raw_rev =
        expr_maker.OpMul(l_extendedprice, expr_maker.OpMin(expr_maker.Constant(1.0f), l_discount));
    std::vector<std::pair<const planner::AbstractExpression *, const planner::AbstractExpression *>>
        clauses;
    clauses.emplace_back(expr_maker.CompareLike(p_type, expr_maker.Constant("PROMO%")), raw_rev);
    auto promo_rev_1 = expr_maker.AggSum(expr_maker.Case(clauses, expr_maker.Constant(1.0F)));
    auto promo_rev_2 = expr_maker.AggSum(raw_rev);
    agg_out.AddAggTerm("promo_revenue_1", promo_rev_1);
    agg_out.AddAggTerm("promo_revenue_2", promo_rev_2);
    // Make the output schema.
    agg_out.AddOutput("promo_revenue_1", agg_out.GetAggTermForOutput("promo_revenue_1"));
    agg_out.AddOutput("promo_revenue_2", agg_out.GetAggTermForOutput("promo_revenue_2"));
    // Build
    agg = planner::AggregatePlanNode::Builder{}
              .SetOutputSchema(agg_out.MakeSchema())
              .AddAggregateTerm(promo_rev_1)
              .AddAggregateTerm(promo_rev_2)
              .AddChild(std::move(hash_join1))
              .SetAggregateStrategyType(planner::AggregateStrategyType::PLAIN)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }

  // Make the final projection.
  std::unique_ptr<planner::AbstractPlanNode> proj;
  planner::OutputSchemaHelper proj_out(&expr_maker, 0);
  {
    // Read previous layer's output
    auto promo_revenue_1 = agg_out.GetOutput("promo_revenue_1");
    auto promo_revenue_2 = agg_out.GetOutput("promo_revenue_2");
    // Make the output schema.
    proj_out.AddOutput("promo_revenue",
                       expr_maker.OpMul(expr_maker.Constant(100.0f),
                                        expr_maker.OpDiv(promo_revenue_1, promo_revenue_2)));
    // Build.
    proj = planner::ProjectionPlanNode::Builder{}
               .AddChild(std::move(agg))
               .SetOutputSchema(proj_out.MakeSchema())
               .Build();
  }

  // Compile plan
  auto last_op = proj.get();
  NoOpResultConsumer consumer;
  sql::MemoryPool memory(nullptr);
  sql::ExecutionContext exec_ctx(&memory, last_op->GetOutputSchema(), &consumer);
  auto query = CompilationContext::Compile(*last_op);
  // Run Once to force compilation
  query->Run(&exec_ctx, kExecutionMode);

  // Only time execution
  for (auto _ : state) {
    query->Run(&exec_ctx, kExecutionMode);
  }
}

BENCHMARK_DEFINE_F(TpchBenchmark, Q16)(benchmark::State &state) {
  auto accessor = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;
  // Part.
  sql::Table *p_table = accessor->LookupTableByName("tpch.part");
  const auto &p_schema = p_table->GetSchema();
  // Partsupp.
  sql::Table *ps_table = accessor->LookupTableByName("tpch.partsupp");
  const auto &ps_schema = ps_table->GetSchema();
  // Supplier.
  sql::Table *s_table = accessor->LookupTableByName("tpch.supplier");
  const auto &s_schema = s_table->GetSchema();
  // Scan part
  std::unique_ptr<planner::AbstractPlanNode> p_seq_scan;
  planner::OutputSchemaHelper p_seq_scan_out{&expr_maker, 0};
  {
    // Read all needed columns
    auto p_brand = expr_maker.CVE(p_schema.GetColumnInfo("p_brand"));
    auto p_type = expr_maker.CVE(p_schema.GetColumnInfo("p_type"));
    auto p_partkey = expr_maker.CVE(p_schema.GetColumnInfo("p_partkey"));
    auto p_size = expr_maker.CVE(p_schema.GetColumnInfo("p_size"));
    // Make the output schema
    p_seq_scan_out.AddOutput("p_brand", p_brand);
    p_seq_scan_out.AddOutput("p_type", p_type);
    p_seq_scan_out.AddOutput("p_partkey", p_partkey);
    p_seq_scan_out.AddOutput("p_size", p_size);
    auto schema = p_seq_scan_out.MakeSchema();

    // Predicate
    auto brand_comp = expr_maker.CompareNeq(p_brand, expr_maker.Constant("Brand#45"));
    auto type_comp = expr_maker.CompareNotLike(p_type, expr_maker.Constant("MEDIUM POLISHED%"));
    auto size_comp = expr_maker.ConjunctionOr(
        expr_maker.CompareEq(p_size, expr_maker.Constant(49)),
        expr_maker.ConjunctionOr(
            expr_maker.CompareEq(p_size, expr_maker.Constant(14)),
            expr_maker.ConjunctionOr(
                expr_maker.CompareEq(p_size, expr_maker.Constant(23)),
                expr_maker.ConjunctionOr(
                    expr_maker.CompareEq(p_size, expr_maker.Constant(45)),
                    expr_maker.ConjunctionOr(
                        expr_maker.CompareEq(p_size, expr_maker.Constant(19)),
                        expr_maker.ConjunctionOr(
                            expr_maker.CompareEq(p_size, expr_maker.Constant(3)),
                            expr_maker.ConjunctionOr(
                                expr_maker.CompareEq(p_size, expr_maker.Constant(36)),
                                expr_maker.CompareEq(p_size, expr_maker.Constant(9)))))))));
    auto predicate =
        expr_maker.ConjunctionAnd(brand_comp, expr_maker.ConjunctionAnd(type_comp, size_comp));
    // Build
    planner::SeqScanPlanNode::Builder builder;
    p_seq_scan = builder.SetOutputSchema(std::move(schema))
                     .SetScanPredicate(predicate)
                     .SetTableOid(p_table->GetId())
                     .Build();
  }

  // Scan supplier
  std::unique_ptr<planner::AbstractPlanNode> s_seq_scan;
  planner::OutputSchemaHelper s_seq_scan_out{&expr_maker, 0};
  {
    // Read all needed columns
    auto s_suppkey = expr_maker.CVE(s_schema.GetColumnInfo("s_suppkey"));
    auto s_comment = expr_maker.CVE(s_schema.GetColumnInfo("s_comment"));
    // Make the output schema
    s_seq_scan_out.AddOutput("s_suppkey", s_suppkey);
    auto schema = s_seq_scan_out.MakeSchema();
    // Predicate
    auto predicate =
        expr_maker.CompareLike(s_comment, expr_maker.Constant("%Customer%Complaints%"));
    // Build
    planner::SeqScanPlanNode::Builder builder;
    s_seq_scan = builder.SetOutputSchema(std::move(schema))
                     .SetScanPredicate(predicate)
                     .SetTableOid(s_table->GetId())
                     .Build();
  }

  // Scan partsupp
  std::unique_ptr<planner::AbstractPlanNode> ps_seq_scan;
  planner::OutputSchemaHelper ps_seq_scan_out{&expr_maker, 1};
  {
    // Read all needed columns
    auto ps_suppkey = expr_maker.CVE(ps_schema.GetColumnInfo("ps_suppkey"));
    auto ps_partkey = expr_maker.CVE(ps_schema.GetColumnInfo("ps_partkey"));
    // Make the output schema
    ps_seq_scan_out.AddOutput("ps_suppkey", ps_suppkey);
    ps_seq_scan_out.AddOutput("ps_partkey", ps_partkey);
    auto schema = ps_seq_scan_out.MakeSchema();
    // Build
    planner::SeqScanPlanNode::Builder builder;
    ps_seq_scan = builder.SetOutputSchema(std::move(schema))
                      .SetScanPredicate(nullptr)
                      .SetTableOid(ps_table->GetId())
                      .Build();
  }

  // First hash join
  // Hash Join 1
  std::unique_ptr<planner::AbstractPlanNode> hash_join1;
  planner::OutputSchemaHelper hash_join_out1{&expr_maker, 1};
  {
    // Left columns
    auto p_brand = p_seq_scan_out.GetOutput("p_brand");
    auto p_type = p_seq_scan_out.GetOutput("p_type");
    auto p_size = p_seq_scan_out.GetOutput("p_size");
    auto p_partkey = p_seq_scan_out.GetOutput("p_partkey");
    // Right columns
    auto ps_suppkey = ps_seq_scan_out.GetOutput("ps_suppkey");
    auto ps_partkey = ps_seq_scan_out.GetOutput("ps_partkey");
    // Output Schema
    hash_join_out1.AddOutput("p_brand", p_brand);
    hash_join_out1.AddOutput("p_type", p_type);
    hash_join_out1.AddOutput("p_size", p_size);
    hash_join_out1.AddOutput("ps_suppkey", ps_suppkey);
    auto schema = hash_join_out1.MakeSchema();
    // Predicate
    auto predicate = expr_maker.CompareEq(p_partkey, ps_partkey);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join1 = builder.AddChild(std::move(p_seq_scan))
                     .AddChild(std::move(ps_seq_scan))
                     .SetOutputSchema(std::move(schema))
                     .AddLeftHashKey(p_partkey)
                     .AddRightHashKey(ps_partkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .Build();
  }

  // Second hash join
  std::unique_ptr<planner::AbstractPlanNode> hash_join2;
  planner::OutputSchemaHelper hash_join_out2{&expr_maker, 0};
  {
    // Left columns
    auto s_suppkey = s_seq_scan_out.GetOutput("s_suppkey");
    // Right columns
    auto p_brand = hash_join_out1.GetOutput("p_brand");
    auto p_type = hash_join_out1.GetOutput("p_type");
    auto p_size = hash_join_out1.GetOutput("p_size");
    auto ps_suppkey = hash_join_out1.GetOutput("ps_suppkey");
    // Output Schema
    hash_join_out2.AddOutput("p_brand", p_brand);
    hash_join_out2.AddOutput("p_type", p_type);
    hash_join_out2.AddOutput("p_size", p_size);
    hash_join_out2.AddOutput("ps_suppkey", ps_suppkey);
    auto schema = hash_join_out2.MakeSchema();
    // Predicate
    auto predicate = expr_maker.CompareEq(s_suppkey, ps_suppkey);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join2 = builder.AddChild(std::move(s_seq_scan))
                     .AddChild(std::move(hash_join1))
                     .SetOutputSchema(std::move(schema))
                     .AddLeftHashKey(s_suppkey)
                     .AddRightHashKey(ps_suppkey)
                     .SetJoinType(planner::LogicalJoinType::RIGHT_ANTI)
                     .SetJoinPredicate(predicate)
                     .Build();
  }

  // Make the aggregate
  std::unique_ptr<planner::AbstractPlanNode> agg;
  planner::OutputSchemaHelper agg_out{&expr_maker, 0};
  {
    // Read previous layer's output
    auto p_brand = hash_join_out2.GetOutput("p_brand");
    auto p_type = hash_join_out2.GetOutput("p_type");
    auto p_size = hash_join_out2.GetOutput("p_size");
    auto ps_suppkey = hash_join_out2.GetOutput("ps_suppkey");
    // Make the aggregate expressions
    auto supplier_cnt = expr_maker.AggCount(ps_suppkey, true);
    // Add them to the helper.
    agg_out.AddGroupByTerm("p_brand", p_brand);
    agg_out.AddGroupByTerm("p_type", p_type);
    agg_out.AddGroupByTerm("p_size", p_size);
    agg_out.AddAggTerm("supplier_cnt", supplier_cnt);
    // Make the output schema
    agg_out.AddOutput("p_brand", agg_out.GetGroupByTermForOutput("p_brand"));
    agg_out.AddOutput("p_type", agg_out.GetGroupByTermForOutput("p_type"));
    agg_out.AddOutput("p_size", agg_out.GetGroupByTermForOutput("p_size"));
    agg_out.AddOutput("supplier_cnt", agg_out.GetAggTermForOutput("supplier_cnt"));
    auto schema = agg_out.MakeSchema();
    // Build
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(std::move(schema))
              .AddGroupByTerm(p_brand)
              .AddGroupByTerm(p_type)
              .AddGroupByTerm(p_size)
              .AddAggregateTerm(supplier_cnt)
              .AddChild(std::move(hash_join2))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }

  // Order By
  std::unique_ptr<planner::AbstractPlanNode> order_by;
  planner::OutputSchemaHelper order_by_out{&expr_maker, 0};
  {
    // Read previous layer
    auto p_brand = agg_out.GetOutput("p_brand");
    auto p_type = agg_out.GetOutput("p_type");
    auto p_size = agg_out.GetOutput("p_size");
    auto supplier_cnt = agg_out.GetOutput("supplier_cnt");

    order_by_out.AddOutput("p_brand", p_brand);
    order_by_out.AddOutput("p_type", p_type);
    order_by_out.AddOutput("p_size", p_size);
    order_by_out.AddOutput("supplier_cnt", supplier_cnt);
    auto schema = order_by_out.MakeSchema();
    // Order By Clause
    planner::SortKey clause1{supplier_cnt, planner::OrderByOrderingType::DESC};
    planner::SortKey clause2{p_brand, planner::OrderByOrderingType::ASC};
    planner::SortKey clause3{p_type, planner::OrderByOrderingType::ASC};
    planner::SortKey clause4{p_size, planner::OrderByOrderingType::ASC};
    // Build
    planner::OrderByPlanNode::Builder builder;
    order_by = builder.SetOutputSchema(std::move(schema))
                   .AddChild(std::move(agg))
                   .AddSortKey(clause1.first, clause1.second)
                   .AddSortKey(clause2.first, clause2.second)
                   .AddSortKey(clause3.first, clause3.second)
                   .AddSortKey(clause4.first, clause4.second)
                   .Build();
  }

  // Compile plan
  auto last_op = order_by.get();
  NoOpResultConsumer consumer;
  sql::MemoryPool memory(nullptr);
  sql::ExecutionContext exec_ctx(&memory, last_op->GetOutputSchema(), &consumer);
  auto query = CompilationContext::Compile(*last_op);
  // Run Once to force compilation
  query->Run(&exec_ctx, kExecutionMode);

  // Only time execution
  for (auto _ : state) {
    query->Run(&exec_ctx, kExecutionMode);
  }
}

BENCHMARK_DEFINE_F(TpchBenchmark, Q18)(benchmark::State &state) {
  auto accessor = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;
  // Customer.
  sql::Table *c_table = accessor->LookupTableByName("tpch.customer");
  const auto &c_schema = c_table->GetSchema();
  // Orders.
  sql::Table *o_table = accessor->LookupTableByName("tpch.orders");
  const auto &o_schema = o_table->GetSchema();
  // Lineitem.
  sql::Table *l_table = accessor->LookupTableByName("tpch.lineitem");
  const auto &l_schema = l_table->GetSchema();
  // Scan customer
  std::unique_ptr<planner::AbstractPlanNode> c_seq_scan;
  planner::OutputSchemaHelper c_seq_scan_out{&expr_maker, 0};
  {
    // Read all needed columns
    auto c_custkey = expr_maker.CVE(c_schema.GetColumnInfo("c_custkey"));
    auto c_name = expr_maker.CVE(c_schema.GetColumnInfo("c_name"));
    // Make the output schema
    c_seq_scan_out.AddOutput("c_custkey", c_custkey);
    c_seq_scan_out.AddOutput("c_name", c_name);
    auto schema = c_seq_scan_out.MakeSchema();
    // Build
    planner::SeqScanPlanNode::Builder builder;
    c_seq_scan = builder.SetOutputSchema(std::move(schema))
                     .SetScanPredicate(nullptr)
                     .SetTableOid(c_table->GetId())
                     .Build();
  }
  // Scan orders
  std::unique_ptr<planner::AbstractPlanNode> o_seq_scan;
  planner::OutputSchemaHelper o_seq_scan_out{&expr_maker, 1};
  {
    // Read all needed columns
    auto o_orderkey = expr_maker.CVE(o_schema.GetColumnInfo("o_orderkey"));
    auto o_custkey = expr_maker.CVE(o_schema.GetColumnInfo("o_custkey"));
    auto o_orderdate = expr_maker.CVE(o_schema.GetColumnInfo("o_orderdate"));
    auto o_totalprice = expr_maker.CVE(o_schema.GetColumnInfo("o_totalprice"));
    // Make the output schema
    o_seq_scan_out.AddOutput("o_orderkey", o_orderkey);
    o_seq_scan_out.AddOutput("o_custkey", o_custkey);
    o_seq_scan_out.AddOutput("o_orderdate", o_orderdate);
    o_seq_scan_out.AddOutput("o_totalprice", o_totalprice);
    auto schema = o_seq_scan_out.MakeSchema();
    // Build
    planner::SeqScanPlanNode::Builder builder;
    o_seq_scan = builder.SetOutputSchema(std::move(schema))
                     .SetScanPredicate(nullptr)
                     .SetTableOid(o_table->GetId())
                     .Build();
  }
  // Scan lineitem1
  std::unique_ptr<planner::AbstractPlanNode> l_seq_scan1;
  planner::OutputSchemaHelper l_seq_scan_out1{&expr_maker, 0};
  {
    // Read all needed columns
    auto l_quantity = expr_maker.CVE(l_schema.GetColumnInfo("l_quantity"));
    auto l_orderkey = expr_maker.CVE(l_schema.GetColumnInfo("l_orderkey"));
    // Make the output schema
    l_seq_scan_out1.AddOutput("l_quantity", l_quantity);
    l_seq_scan_out1.AddOutput("l_orderkey", l_orderkey);
    auto schema = l_seq_scan_out1.MakeSchema();
    // Build
    planner::SeqScanPlanNode::Builder builder;
    l_seq_scan1 = builder.SetOutputSchema(std::move(schema))
                      .SetScanPredicate(nullptr)
                      .SetTableOid(l_table->GetId())
                      .Build();
  }
  // Scan lineitem2
  std::unique_ptr<planner::AbstractPlanNode> l_seq_scan2;
  planner::OutputSchemaHelper l_seq_scan_out2{&expr_maker, 1};
  {
    // Read all needed columns
    auto l_quantity = expr_maker.CVE(l_schema.GetColumnInfo("l_quantity"));
    auto l_orderkey = expr_maker.CVE(l_schema.GetColumnInfo("l_orderkey"));
    // Make the output schema
    l_seq_scan_out2.AddOutput("l_quantity", l_quantity);
    l_seq_scan_out2.AddOutput("l_orderkey", l_orderkey);
    auto schema = l_seq_scan_out2.MakeSchema();
    // Build
    planner::SeqScanPlanNode::Builder builder;
    l_seq_scan2 = builder.SetOutputSchema(std::move(schema))
                      .SetScanPredicate(nullptr)
                      .SetTableOid(l_table->GetId())
                      .Build();
  }
  // Make the aggregate
  std::unique_ptr<planner::AbstractPlanNode> agg1;
  planner::OutputSchemaHelper agg_out1{&expr_maker, 0};
  {
    // Read previous layer's output
    auto l_orderkey = l_seq_scan_out1.GetOutput("l_orderkey");
    auto l_quantity = l_seq_scan_out1.GetOutput("l_quantity");
    // Make the aggregate expressions
    auto sum_qty = expr_maker.AggSum(l_quantity);
    // Add them to the helper.
    agg_out1.AddGroupByTerm("l_orderkey", l_orderkey);
    agg_out1.AddAggTerm("sum_qty", sum_qty);
    // Make the output schema
    agg_out1.AddOutput("l_orderkey", agg_out1.GetGroupByTermForOutput("l_orderkey"));
    auto schema = agg_out1.MakeSchema();
    // Make having
    auto having =
        expr_maker.CompareGt(agg_out1.GetAggTermForOutput("sum_qty"), expr_maker.Constant(300.0f));
    // Build
    planner::AggregatePlanNode::Builder builder;
    agg1 = builder.SetOutputSchema(std::move(schema))
               .AddGroupByTerm(l_orderkey)
               .AddAggregateTerm(sum_qty)
               .AddChild(std::move(l_seq_scan1))
               .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
               .SetHavingClausePredicate(having)
               .Build();
  }
  // First hash join
  // Hash Join 1
  std::unique_ptr<planner::AbstractPlanNode> hash_join1;
  planner::OutputSchemaHelper hash_join_out1{&expr_maker, 1};
  {
    // Left columns
    auto l_orderkey = agg_out1.GetOutput("l_orderkey");
    // Right columns
    auto o_orderkey = o_seq_scan_out.GetOutput("o_orderkey");
    auto o_orderdate = o_seq_scan_out.GetOutput("o_orderdate");
    auto o_totalprice = o_seq_scan_out.GetOutput("o_totalprice");
    auto o_custkey = o_seq_scan_out.GetOutput("o_custkey");
    // Output Schema
    hash_join_out1.AddOutput("o_orderkey", o_orderkey);
    hash_join_out1.AddOutput("o_orderdate", o_orderdate);
    hash_join_out1.AddOutput("o_totalprice", o_totalprice);
    hash_join_out1.AddOutput("o_custkey", o_custkey);
    auto schema = hash_join_out1.MakeSchema();
    // Predicate
    auto predicate = expr_maker.CompareEq(l_orderkey, o_orderkey);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join1 = builder.AddChild(std::move(agg1))
                     .AddChild(std::move(o_seq_scan))
                     .SetOutputSchema(std::move(schema))
                     .AddLeftHashKey(l_orderkey)
                     .AddRightHashKey(o_orderkey)
                     .SetJoinType(planner::LogicalJoinType::RIGHT_SEMI)
                     .SetJoinPredicate(predicate)
                     .Build();
  }
  // Second hash join
  std::unique_ptr<planner::AbstractPlanNode> hash_join2;
  planner::OutputSchemaHelper hash_join_out2{&expr_maker, 0};
  {
    // Left columns
    auto c_custkey = c_seq_scan_out.GetOutput("c_custkey");
    auto c_name = c_seq_scan_out.GetOutput("c_name");
    // Right columns
    auto o_orderkey = hash_join_out1.GetOutput("o_orderkey");
    auto o_orderdate = hash_join_out1.GetOutput("o_orderdate");
    auto o_totalprice = hash_join_out1.GetOutput("o_totalprice");
    auto o_custkey = hash_join_out1.GetOutput("o_custkey");
    // Output Schema
    hash_join_out2.AddOutput("c_name", c_name);
    hash_join_out2.AddOutput("c_custkey", c_custkey);
    hash_join_out2.AddOutput("o_orderkey", o_orderkey);
    hash_join_out2.AddOutput("o_orderdate", o_orderdate);
    hash_join_out2.AddOutput("o_totalprice", o_totalprice);
    auto schema = hash_join_out2.MakeSchema();
    // Predicate
    auto predicate = expr_maker.CompareEq(c_custkey, o_custkey);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join2 = builder.AddChild(std::move(c_seq_scan))
                     .AddChild(std::move(hash_join1))
                     .SetOutputSchema(std::move(schema))
                     .AddLeftHashKey(c_custkey)
                     .AddRightHashKey(o_custkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .Build();
  }
  // Make third hash join
  std::unique_ptr<planner::AbstractPlanNode> hash_join3;
  planner::OutputSchemaHelper hash_join_out3{&expr_maker, 0};
  {
    // Left columns
    auto c_name = hash_join_out2.GetOutput("c_name");
    auto c_custkey = hash_join_out2.GetOutput("c_custkey");
    auto o_orderkey = hash_join_out2.GetOutput("o_orderkey");
    auto o_orderdate = hash_join_out2.GetOutput("o_orderdate");
    auto o_totalprice = hash_join_out2.GetOutput("o_totalprice");
    // Right columns
    auto l_orderkey = l_seq_scan_out2.GetOutput("l_orderkey");
    auto l_quantity = l_seq_scan_out2.GetOutput("l_quantity");
    // Output Schema
    hash_join_out3.AddOutput("c_name", c_name);
    hash_join_out3.AddOutput("c_custkey", c_custkey);
    hash_join_out3.AddOutput("o_orderkey", o_orderkey);
    hash_join_out3.AddOutput("o_orderdate", o_orderdate);
    hash_join_out3.AddOutput("o_totalprice", o_totalprice);
    hash_join_out3.AddOutput("l_quantity", l_quantity);
    auto schema = hash_join_out3.MakeSchema();
    // Predicate
    auto predicate = expr_maker.CompareEq(o_orderkey, l_orderkey);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join3 = builder.AddChild(std::move(hash_join2))
                     .AddChild(std::move(l_seq_scan2))
                     .SetOutputSchema(std::move(schema))
                     .AddLeftHashKey(o_orderkey)
                     .AddRightHashKey(l_orderkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .Build();
  }
  // Make the aggregate
  std::unique_ptr<planner::AbstractPlanNode> agg2;
  planner::OutputSchemaHelper agg_out2{&expr_maker, 0};
  {
    // Read previous layer's output
    auto c_name = hash_join_out3.GetOutput("c_name");
    auto c_custkey = hash_join_out3.GetOutput("c_custkey");
    auto o_orderkey = hash_join_out3.GetOutput("o_orderkey");
    auto o_orderdate = hash_join_out3.GetOutput("o_orderdate");
    auto o_totalprice = hash_join_out3.GetOutput("o_totalprice");
    auto l_quantity = hash_join_out3.GetOutput("l_quantity");
    // Make the aggregate expressions
    auto sum_qty = expr_maker.AggSum(l_quantity);
    // Add them to the helper.
    agg_out2.AddGroupByTerm("c_name", c_name);
    agg_out2.AddGroupByTerm("c_custkey", c_custkey);
    agg_out2.AddGroupByTerm("o_orderkey", o_orderkey);
    agg_out2.AddGroupByTerm("o_orderdate", o_orderdate);
    agg_out2.AddGroupByTerm("o_totalprice", o_totalprice);
    agg_out2.AddAggTerm("sum_qty", sum_qty);
    // Make the output schema
    agg_out2.AddOutput("c_name", agg_out2.GetGroupByTermForOutput("c_name"));
    agg_out2.AddOutput("c_custkey", agg_out2.GetGroupByTermForOutput("c_custkey"));
    agg_out2.AddOutput("o_orderkey", agg_out2.GetGroupByTermForOutput("o_orderkey"));
    agg_out2.AddOutput("o_orderdate", agg_out2.GetGroupByTermForOutput("o_orderdate"));
    agg_out2.AddOutput("o_totalprice", agg_out2.GetGroupByTermForOutput("o_totalprice"));
    agg_out2.AddOutput("sum_qty", agg_out2.GetAggTermForOutput("sum_qty"));
    auto schema = agg_out2.MakeSchema();
    // Make having
    // Build
    planner::AggregatePlanNode::Builder builder;
    agg2 = builder.SetOutputSchema(std::move(schema))
               .AddGroupByTerm(c_name)
               .AddGroupByTerm(c_custkey)
               .AddGroupByTerm(o_orderkey)
               .AddGroupByTerm(o_orderdate)
               .AddGroupByTerm(o_totalprice)
               .AddAggregateTerm(sum_qty)
               .AddChild(std::move(hash_join3))
               .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
               .SetHavingClausePredicate(nullptr)
               .Build();
  }
  // Order By
  std::unique_ptr<planner::AbstractPlanNode> order_by;
  planner::OutputSchemaHelper order_by_out{&expr_maker, 0};
  {
    // Read previous layer
    auto c_name = agg_out2.GetOutput("c_name");
    auto c_custkey = agg_out2.GetOutput("c_custkey");
    auto o_orderkey = agg_out2.GetOutput("o_orderkey");
    auto o_orderdate = agg_out2.GetOutput("o_orderdate");
    auto o_totalprice = agg_out2.GetOutput("o_totalprice");
    auto sum_qty = agg_out2.GetOutput("sum_qty");
    order_by_out.AddOutput("c_name", c_name);
    order_by_out.AddOutput("c_custkey", c_custkey);
    order_by_out.AddOutput("o_orderkey", o_orderkey);
    order_by_out.AddOutput("o_orderdate", o_orderdate);
    order_by_out.AddOutput("o_totalprice", o_totalprice);
    order_by_out.AddOutput("sum_qty", sum_qty);
    auto schema = order_by_out.MakeSchema();
    // Order By Clause
    planner::SortKey clause1{o_totalprice, planner::OrderByOrderingType::DESC};
    planner::SortKey clause2{o_orderdate, planner::OrderByOrderingType::ASC};
    // Build
    planner::OrderByPlanNode::Builder builder;
    order_by = builder.SetOutputSchema(std::move(schema))
                   .AddChild(std::move(agg2))
                   .AddSortKey(clause1.first, clause1.second)
                   .AddSortKey(clause2.first, clause2.second)
                   .Build();
  }

  // Compile plan
  auto last_op = order_by.get();
  NoOpResultConsumer consumer;
  sql::MemoryPool memory(nullptr);
  sql::ExecutionContext exec_ctx(&memory, last_op->GetOutputSchema(), &consumer);
  auto query = CompilationContext::Compile(*last_op);
  // Run Once to force compilation
  query->Run(&exec_ctx, kExecutionMode);

  // Only time execution
  for (auto _ : state) {
    query->Run(&exec_ctx, kExecutionMode);
  }
}

BENCHMARK_DEFINE_F(TpchBenchmark, Q19)(benchmark::State &state) {
  auto accessor = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;
  // Lineitem.
  sql::Table *l_table = accessor->LookupTableByName("tpch.lineitem");
  const auto &l_schema = l_table->GetSchema();
  // Part.
  sql::Table *p_table = accessor->LookupTableByName("tpch.part");
  const auto &p_schema = p_table->GetSchema();
  // Lineitem scan
  std::unique_ptr<planner::AbstractPlanNode> l_seq_scan;
  planner::OutputSchemaHelper l_seq_scan_out{&expr_maker, 1};
  {
    // Read all needed columns
    auto l_extendedprice = expr_maker.CVE(l_schema.GetColumnInfo("l_extendedprice"));
    auto l_discount = expr_maker.CVE(l_schema.GetColumnInfo("l_discount"));
    auto l_partkey = expr_maker.CVE(l_schema.GetColumnInfo("l_partkey"));
    auto l_quantity = expr_maker.CVE(l_schema.GetColumnInfo("l_quantity"));
    auto l_shipmode = expr_maker.CVE(l_schema.GetColumnInfo("l_shipmode"));
    auto l_shipinstruct = expr_maker.CVE(l_schema.GetColumnInfo("l_shipinstruct"));
    // Make the output schema
    l_seq_scan_out.AddOutput("l_extendedprice", l_extendedprice);
    l_seq_scan_out.AddOutput("l_discount", l_discount);
    l_seq_scan_out.AddOutput("l_partkey", l_partkey);
    l_seq_scan_out.AddOutput("l_quantity", l_quantity);
    l_seq_scan_out.AddOutput("l_shipmode", l_shipmode);
    l_seq_scan_out.AddOutput("l_shipinstruct", l_shipinstruct);
    auto schema = l_seq_scan_out.MakeSchema();

    // Predicate.
    auto shipmode_comp =
        expr_maker.ConjunctionOr(expr_maker.CompareEq(l_shipmode, expr_maker.Constant("AIR")),
                                 expr_maker.CompareEq(l_shipmode, expr_maker.Constant("AIR REG")));
    auto shipinstruct_comp =
        expr_maker.CompareEq(l_shipinstruct, expr_maker.Constant("DELIVER IN PERSON"));
    auto predicate = expr_maker.ConjunctionAnd(shipmode_comp, shipinstruct_comp);

    // Build
    planner::SeqScanPlanNode::Builder builder;
    l_seq_scan = builder.SetOutputSchema(std::move(schema))
                     .SetScanPredicate(predicate)
                     .SetTableOid(l_table->GetId())
                     .Build();
  }
  // Part Scan
  std::unique_ptr<planner::AbstractPlanNode> p_seq_scan;
  planner::OutputSchemaHelper p_seq_scan_out{&expr_maker, 0};
  {
    // Read all needed columns
    auto p_brand = expr_maker.CVE(p_schema.GetColumnInfo("p_brand"));
    auto p_container = expr_maker.CVE(p_schema.GetColumnInfo("p_container"));
    auto p_partkey = expr_maker.CVE(p_schema.GetColumnInfo("p_partkey"));
    auto p_size = expr_maker.CVE(p_schema.GetColumnInfo("p_size"));
    // Make the output schema
    p_seq_scan_out.AddOutput("p_brand", p_brand);
    p_seq_scan_out.AddOutput("p_container", p_container);
    p_seq_scan_out.AddOutput("p_partkey", p_partkey);
    p_seq_scan_out.AddOutput("p_size", p_size);

    // Generate predicate clause.
    auto gen_predicate_clause = [&](int32_t lo_size, int32_t hi_size, const std::string &brand,
                                    const std::vector<std::string> &containers) {
      auto size_comp =
          expr_maker.ConjunctionAnd(expr_maker.CompareGe(p_size, expr_maker.Constant(lo_size)),
                                    expr_maker.CompareLe(p_size, expr_maker.Constant(hi_size)));
      auto brand_comp = expr_maker.CompareEq(p_brand, expr_maker.Constant(brand));
      auto container_comp = expr_maker.ConjunctionOr(
          expr_maker.CompareEq(p_container, expr_maker.Constant(containers[0])),
          expr_maker.ConjunctionOr(
              expr_maker.CompareEq(p_container, expr_maker.Constant(containers[1])),
              expr_maker.ConjunctionOr(
                  expr_maker.CompareEq(p_container, expr_maker.Constant(containers[2])),
                  expr_maker.CompareEq(p_container, expr_maker.Constant(containers[3])))));
      return expr_maker.ConjunctionAnd(brand_comp,
                                       expr_maker.ConjunctionAnd(container_comp, size_comp));
    };

    auto predicate = expr_maker.ConjunctionOr(
        gen_predicate_clause(1, 5, "Brand#12", {"SM CASE", "SM BOX", "SM PACK", "SM PKG"}),
        expr_maker.ConjunctionOr(
            gen_predicate_clause(1, 10, "Brand#23", {"MED BAG", "MED BOX", "MED PKG", "MED PACK"}),
            gen_predicate_clause(1, 15, "Brand#34", {"LG CASE", "LG BOX", "LG PACK", "LG PKG"})));

    // Build
    p_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(p_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(p_table->GetId())
                     .Build();
  }

  // Hash Join 1
  std::unique_ptr<planner::AbstractPlanNode> hash_join1;
  planner::OutputSchemaHelper hash_join_out1{&expr_maker, 0};
  {
    // Left columns
    auto p_brand = p_seq_scan_out.GetOutput("p_brand");
    auto p_container = p_seq_scan_out.GetOutput("p_container");
    auto p_partkey = p_seq_scan_out.GetOutput("p_partkey");
    auto p_size = p_seq_scan_out.GetOutput("p_size");
    // Right columns
    auto l_partkey = l_seq_scan_out.GetOutput("l_partkey");
    auto l_quantity = l_seq_scan_out.GetOutput("l_quantity");
    auto l_discount = l_seq_scan_out.GetOutput("l_discount");
    auto l_extendedprice = l_seq_scan_out.GetOutput("l_extendedprice");
    // Output Schema
    hash_join_out1.AddOutput("l_extendedprice", l_extendedprice);
    hash_join_out1.AddOutput("l_discount", l_discount);
    hash_join_out1.AddOutput("p_brand", p_brand);
    hash_join_out1.AddOutput("p_size", p_size);
    auto schema = hash_join_out1.MakeSchema();
    // Predicate1
    planner::ExpressionMaker::Expression predicate1, predicate2, predicate3;
    auto gen_predicate_clause = [&](const std::string &brand, const std::vector<std::string> &sm,
                                    float lo_qty, float hi_qty, int32_t lo_size, int32_t hi_size) {
      auto brand_comp = expr_maker.CompareEq(p_brand, expr_maker.Constant(brand));
      auto container_comp = expr_maker.ConjunctionOr(
          expr_maker.CompareEq(p_container, expr_maker.Constant(sm[0])),
          expr_maker.ConjunctionOr(
              expr_maker.CompareEq(p_container, expr_maker.Constant(sm[1])),
              expr_maker.ConjunctionOr(
                  expr_maker.CompareEq(p_container, expr_maker.Constant(sm[2])),
                  expr_maker.CompareEq(p_container, expr_maker.Constant(sm[3])))));
      auto qty_lo_comp = expr_maker.CompareGe(l_quantity, expr_maker.Constant(lo_qty));
      auto qty_hi_comp = expr_maker.CompareLe(l_quantity, expr_maker.Constant(hi_qty));
      auto qty_comp = expr_maker.ConjunctionAnd(qty_lo_comp, qty_hi_comp);
      auto size_lo_comp = expr_maker.CompareGe(p_size, expr_maker.Constant(lo_size));
      auto size_hi_comp = expr_maker.CompareLe(p_size, expr_maker.Constant(hi_size));
      auto size_comp = expr_maker.ConjunctionAnd(size_lo_comp, size_hi_comp);

      return expr_maker.ConjunctionAnd(
          brand_comp, expr_maker.ConjunctionAnd(container_comp,
                                                expr_maker.ConjunctionAnd(qty_comp, size_comp)));
    };
    predicate1 =
        gen_predicate_clause("Brand#12", {"SM CASE", "SM BOX", "SM PACK", "SM PKG"}, 1, 11, 1, 5);
    predicate2 = gen_predicate_clause("Brand#23", {"MED BAG", "MED BOX", "MED PKG", "MED PACK"}, 10,
                                      20, 1, 10);
    predicate3 =
        gen_predicate_clause("Brand#34", {"LG CASE", "LG BOX", "LG PACK", "LG PKG"}, 20, 30, 1, 15);
    auto predicate =
        expr_maker.ConjunctionOr(predicate1, expr_maker.ConjunctionOr(predicate2, predicate3));
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join1 = builder.AddChild(std::move(p_seq_scan))
                     .AddChild(std::move(l_seq_scan))
                     .SetOutputSchema(std::move(schema))
                     .AddLeftHashKey(p_partkey)
                     .AddRightHashKey(l_partkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .Build();
  }
  // Make the aggregate
  std::unique_ptr<planner::AbstractPlanNode> agg;
  planner::OutputSchemaHelper agg_out{&expr_maker, 0};
  {
    // Read previous layer's output
    auto l_extendedprice = hash_join_out1.GetOutput("l_extendedprice");
    auto l_discount = hash_join_out1.GetOutput("l_discount");
    // Make the aggregate expressions
    auto one_const = expr_maker.Constant(1.0f);
    auto revenue = expr_maker.AggSum(
        expr_maker.OpMul(l_extendedprice, expr_maker.OpMin(one_const, l_discount)));
    // Add them to the helper.
    agg_out.AddAggTerm("revenue", revenue);
    // Make the output schema
    agg_out.AddOutput("revenue", agg_out.GetAggTermForOutput("revenue"));
    auto schema = agg_out.MakeSchema();
    // Make having
    // Build
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(std::move(schema))
              .AddAggregateTerm(revenue)
              .AddChild(std::move(hash_join1))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }

  // Compile plan
  auto last_op = agg.get();
  NoOpResultConsumer consumer;
  // PrintingConsumer consumer(std::cout, last_op->GetOutputSchema());
  sql::MemoryPool memory(nullptr);
  sql::ExecutionContext exec_ctx(&memory, last_op->GetOutputSchema(), &consumer);
  auto query = CompilationContext::Compile(*last_op);
  // Run Once to force compilation
  query->Run(&exec_ctx, kExecutionMode);

  // Only time execution
  for (auto _ : state) {
    query->Run(&exec_ctx, kExecutionMode);
  }
}

// ---------------------------------------------------------
//
// Benchmark Configs
//
// ---------------------------------------------------------

BENCHMARK_REGISTER_F(TpchBenchmark, Q1)->Iterations(10)->Unit(benchmark::kMillisecond);
BENCHMARK_REGISTER_F(TpchBenchmark, Q3)->Iterations(10)->Unit(benchmark::kMillisecond);
BENCHMARK_REGISTER_F(TpchBenchmark, Q4)->Iterations(10)->Unit(benchmark::kMillisecond);
BENCHMARK_REGISTER_F(TpchBenchmark, Q5)->Iterations(10)->Unit(benchmark::kMillisecond);
BENCHMARK_REGISTER_F(TpchBenchmark, Q6)->Iterations(10)->Unit(benchmark::kMillisecond);
BENCHMARK_REGISTER_F(TpchBenchmark, Q7)->Iterations(10)->Unit(benchmark::kMillisecond);
BENCHMARK_REGISTER_F(TpchBenchmark, Q9)->Iterations(10)->Unit(benchmark::kMillisecond);
BENCHMARK_REGISTER_F(TpchBenchmark, Q10)->Iterations(10)->Unit(benchmark::kMillisecond);
BENCHMARK_REGISTER_F(TpchBenchmark, Q11)->Iterations(10)->Unit(benchmark::kMillisecond);
BENCHMARK_REGISTER_F(TpchBenchmark, Q12)->Iterations(10)->Unit(benchmark::kMillisecond);
BENCHMARK_REGISTER_F(TpchBenchmark, Q14)->Iterations(10)->Unit(benchmark::kMillisecond);
BENCHMARK_REGISTER_F(TpchBenchmark, Q16)->Iterations(10)->Unit(benchmark::kMillisecond);
BENCHMARK_REGISTER_F(TpchBenchmark, Q18)->Iterations(10)->Unit(benchmark::kMillisecond);
BENCHMARK_REGISTER_F(TpchBenchmark, Q19)->Iterations(10)->Unit(benchmark::kMillisecond);

}  // namespace tpl::sql::codegen
