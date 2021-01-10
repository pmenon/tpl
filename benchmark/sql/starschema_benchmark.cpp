#include <memory>
#include <mutex>
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
#include "sql/planner/plannodes/order_by_plan_node.h"
#include "sql/planner/plannodes/output_schema.h"
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

// Change this path to where your SSBM data is.
constexpr char kSSBMDataDir[] = "/home/pmenon/tools/SSBM/data/sf-0.1";

// Flag used to ensure the SSBM database is only loaded once.
std::once_flag kLoadSSBMDatabaseOnce{};

}  // namespace

class StarSchemaBenchmark : public benchmark::Fixture {
 public:
  void SetUp(benchmark::State &st) override {
    Fixture::SetUp(st);
    std::call_once(kLoadSSBMDatabaseOnce, []() {
      tablegen::TableGenerator::GenerateSSBMTables(Catalog::Instance(), kSSBMDataDir);
    });
  }
};

BENCHMARK_DEFINE_F(StarSchemaBenchmark, Q1_1)(benchmark::State &state) {
  auto catalog = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;
  // Date
  sql::Table *d_table = catalog->LookupTableByName("ssbm.date");
  const auto &d_schema = d_table->GetSchema();
  // LineOrder
  sql::Table *lo_table = catalog->LookupTableByName("ssbm.lineorder");
  const auto &lo_schema = lo_table->GetSchema();

  // Scan date
  std::unique_ptr<planner::AbstractPlanNode> d_seq_scan;
  planner::OutputSchemaHelper d_seq_scan_out{&expr_maker, 0};
  {
    auto d_datekey = expr_maker.CVE(d_schema.GetColumnInfo("d_datekey"));
    auto d_year = expr_maker.CVE(d_schema.GetColumnInfo("d_year"));
    // Make the predicate: d_year=1993
    auto _1993 = expr_maker.Constant(1993);
    auto predicate = expr_maker.CompareEq(d_year, _1993);
    // Make output schema.
    d_seq_scan_out.AddOutput("d_datekey", d_datekey);
    // Build plan node.
    d_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(d_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(d_table->GetId())
                     .Build();
  }

  // Scan lineorder.
  std::unique_ptr<planner::AbstractPlanNode> lo_seq_scan;
  planner::OutputSchemaHelper lo_seq_scan_out{&expr_maker, 1};
  {
    auto lo_orderdate = expr_maker.CVE(lo_schema.GetColumnInfo("lo_orderdate"));
    auto lo_extendedprice = expr_maker.CVE(lo_schema.GetColumnInfo("lo_extendedprice"));
    auto lo_discount = expr_maker.CVE(lo_schema.GetColumnInfo("lo_discount"));
    auto lo_quantity = expr_maker.CVE(lo_schema.GetColumnInfo("lo_quantity"));
    // Make predicate: lo_discount between 1 and 3 AND lo_quantity < 25
    auto predicate = expr_maker.ConjunctionAnd(
        expr_maker.CompareBetween(lo_discount, expr_maker.Constant(1), expr_maker.Constant(3)),
        expr_maker.CompareLt(lo_quantity, expr_maker.Constant(25)));
    // Make output schema.
    lo_seq_scan_out.AddOutput("lo_orderdate", lo_orderdate);
    lo_seq_scan_out.AddOutput("lo_extendedprice", lo_extendedprice);
    lo_seq_scan_out.AddOutput("lo_discount", lo_discount);
    // Build plan node.
    planner::SeqScanPlanNode::Builder builder;
    lo_seq_scan = builder.SetOutputSchema(lo_seq_scan_out.MakeSchema())
                      .SetScanPredicate(predicate)
                      .SetTableOid(lo_table->GetId())
                      .Build();
  }

  // date <-> lineorder join.
  std::unique_ptr<planner::AbstractPlanNode> hash_join;
  planner::OutputSchemaHelper hash_join_out{&expr_maker, 0};
  {
    // Left columns.
    auto d_datekey = d_seq_scan_out.GetOutput("d_datekey");
    // Right columns.
    auto lo_orderdate = lo_seq_scan_out.GetOutput("lo_orderdate");
    auto lo_extendedprice = lo_seq_scan_out.GetOutput("lo_extendedprice");
    auto lo_discount = lo_seq_scan_out.GetOutput("lo_discount");
    // Output Schema
    hash_join_out.AddOutput("lo_extendedprice", lo_extendedprice);
    hash_join_out.AddOutput("lo_discount", lo_discount);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join = builder.AddChild(std::move(d_seq_scan))
                    .AddChild(std::move(lo_seq_scan))
                    .SetOutputSchema(hash_join_out.MakeSchema())
                    .AddLeftHashKey(d_datekey)
                    .AddRightHashKey(lo_orderdate)
                    .SetJoinType(planner::LogicalJoinType::INNER)
                    .SetJoinPredicate(expr_maker.CompareEq(d_datekey, lo_orderdate))
                    .Build();
  }

  // Make the aggregate
  std::unique_ptr<planner::AbstractPlanNode> agg;
  planner::OutputSchemaHelper agg_out{&expr_maker, 0};
  {
    // Read previous layer's output
    auto lo_extendedprice = hash_join_out.GetOutput("lo_extendedprice");
    auto lo_discount = hash_join_out.GetOutput("lo_discount");
    auto revenue = expr_maker.AggSum(expr_maker.OpMul(lo_extendedprice, lo_discount));
    // Add them to the helper.
    agg_out.AddAggTerm("revenue", revenue);
    // Make the output schema.
    agg_out.AddOutput("revenue", agg_out.GetAggTermForOutput("revenue"));
    // Build plan node.
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(agg_out.MakeSchema())
              .AddAggregateTerm(revenue)
              .AddChild(std::move(hash_join))
              .SetAggregateStrategyType(planner::AggregateStrategyType::PLAIN)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }

  // Compile plan!
  auto query = CompilationContext::Compile(*agg);

  // Run Once to force compilation
  NoOpResultConsumer consumer;
  {
    sql::MemoryPool memory(nullptr);
    sql::ExecutionContext exec_ctx(&memory, agg->GetOutputSchema(), &consumer);
    query->Run(&exec_ctx, kExecutionMode);
  }

  // Only time execution.
  for (auto _ : state) {
    sql::MemoryPool memory(nullptr);
    sql::ExecutionContext exec_ctx(&memory, agg->GetOutputSchema(), &consumer);
    query->Run(&exec_ctx, kExecutionMode);
  }
}

BENCHMARK_DEFINE_F(StarSchemaBenchmark, Q1_2)(benchmark::State &state) {
  auto catalog = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;
  // Date
  sql::Table *d_table = catalog->LookupTableByName("ssbm.date");
  const auto &d_schema = d_table->GetSchema();
  // LineOrder
  sql::Table *lo_table = catalog->LookupTableByName("ssbm.lineorder");
  const auto &lo_schema = lo_table->GetSchema();

  // Scan date
  std::unique_ptr<planner::AbstractPlanNode> d_seq_scan;
  planner::OutputSchemaHelper d_seq_scan_out{&expr_maker, 0};
  {
    auto d_datekey = expr_maker.CVE(d_schema.GetColumnInfo("d_datekey"));
    auto d_yearmonthnum = expr_maker.CVE(d_schema.GetColumnInfo("d_yearmonthnum"));
    // Make the predicate: d_yearmonthnum = 199401
    auto predicate = expr_maker.CompareEq(d_yearmonthnum, expr_maker.Constant(199401));
    // Make output schema.
    d_seq_scan_out.AddOutput("d_datekey", d_datekey);
    // Build plan node.
    d_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(d_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(d_table->GetId())
                     .Build();
  }

  // Scan lineorder.
  std::unique_ptr<planner::AbstractPlanNode> lo_seq_scan;
  planner::OutputSchemaHelper lo_seq_scan_out{&expr_maker, 1};
  {
    auto lo_orderdate = expr_maker.CVE(lo_schema.GetColumnInfo("lo_orderdate"));
    auto lo_extendedprice = expr_maker.CVE(lo_schema.GetColumnInfo("lo_extendedprice"));
    auto lo_discount = expr_maker.CVE(lo_schema.GetColumnInfo("lo_discount"));
    auto lo_quantity = expr_maker.CVE(lo_schema.GetColumnInfo("lo_quantity"));
    // Make predicate: lo_discount between 4 and 6 AND lo_quantity between 26 and 35
    auto predicate = expr_maker.ConjunctionAnd(
        expr_maker.CompareBetween(lo_discount, expr_maker.Constant(4), expr_maker.Constant(6)),
        expr_maker.CompareBetween(lo_quantity, expr_maker.Constant(26), expr_maker.Constant(35)));
    // Make output schema.
    lo_seq_scan_out.AddOutput("lo_orderdate", lo_orderdate);
    lo_seq_scan_out.AddOutput("lo_extendedprice", lo_extendedprice);
    lo_seq_scan_out.AddOutput("lo_discount", lo_discount);
    // Build plan node.
    planner::SeqScanPlanNode::Builder builder;
    lo_seq_scan = builder.SetOutputSchema(lo_seq_scan_out.MakeSchema())
                      .SetScanPredicate(predicate)
                      .SetTableOid(lo_table->GetId())
                      .Build();
  }

  // date <-> lineorder join.
  std::unique_ptr<planner::AbstractPlanNode> hash_join;
  planner::OutputSchemaHelper hash_join_out{&expr_maker, 0};
  {
    // Left columns.
    auto d_datekey = d_seq_scan_out.GetOutput("d_datekey");
    // Right columns.
    auto lo_orderdate = lo_seq_scan_out.GetOutput("lo_orderdate");
    auto lo_extendedprice = lo_seq_scan_out.GetOutput("lo_extendedprice");
    auto lo_discount = lo_seq_scan_out.GetOutput("lo_discount");
    // Output Schema
    hash_join_out.AddOutput("lo_extendedprice", lo_extendedprice);
    hash_join_out.AddOutput("lo_discount", lo_discount);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join = builder.AddChild(std::move(d_seq_scan))
                    .AddChild(std::move(lo_seq_scan))
                    .SetOutputSchema(hash_join_out.MakeSchema())
                    .AddLeftHashKey(d_datekey)
                    .AddRightHashKey(lo_orderdate)
                    .SetJoinType(planner::LogicalJoinType::INNER)
                    .SetJoinPredicate(expr_maker.CompareEq(d_datekey, lo_orderdate))
                    .Build();
  }

  // Make the aggregate
  std::unique_ptr<planner::AbstractPlanNode> agg;
  planner::OutputSchemaHelper agg_out{&expr_maker, 0};
  {
    // Read previous layer's output
    auto lo_extendedprice = hash_join_out.GetOutput("lo_extendedprice");
    auto lo_discount = hash_join_out.GetOutput("lo_discount");
    auto revenue = expr_maker.AggSum(expr_maker.OpMul(lo_extendedprice, lo_discount));
    // Add them to the helper.
    agg_out.AddAggTerm("revenue", revenue);
    // Make the output schema.
    agg_out.AddOutput("revenue", agg_out.GetAggTermForOutput("revenue"));
    // Build plan node.
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(agg_out.MakeSchema())
              .AddAggregateTerm(revenue)
              .AddChild(std::move(hash_join))
              .SetAggregateStrategyType(planner::AggregateStrategyType::PLAIN)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }

  // Compile plan!
  auto query = CompilationContext::Compile(*agg);

  // Run Once to force compilation
  NoOpResultConsumer consumer;
  {
    sql::MemoryPool memory(nullptr);
    sql::ExecutionContext exec_ctx(&memory, agg->GetOutputSchema(), &consumer);
    query->Run(&exec_ctx, kExecutionMode);
  }

  // Only time execution.
  for (auto _ : state) {
    sql::MemoryPool memory(nullptr);
    sql::ExecutionContext exec_ctx(&memory, agg->GetOutputSchema(), &consumer);
    query->Run(&exec_ctx, kExecutionMode);
  }
}

BENCHMARK_DEFINE_F(StarSchemaBenchmark, Q1_3)(benchmark::State &state) {
  auto catalog = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;
  // Date
  sql::Table *d_table = catalog->LookupTableByName("ssbm.date");
  const auto &d_schema = d_table->GetSchema();
  // LineOrder
  sql::Table *lo_table = catalog->LookupTableByName("ssbm.lineorder");
  const auto &lo_schema = lo_table->GetSchema();

  // Scan date
  std::unique_ptr<planner::AbstractPlanNode> d_seq_scan;
  planner::OutputSchemaHelper d_seq_scan_out{&expr_maker, 0};
  {
    auto d_datekey = expr_maker.CVE(d_schema.GetColumnInfo("d_datekey"));
    auto d_weeknuminyear = expr_maker.CVE(d_schema.GetColumnInfo("d_weeknuminyear"));
    auto d_year = expr_maker.CVE(d_schema.GetColumnInfo("d_year"));
    // Make the predicate: d_weeknuminyear = 6 AND d_year = 1994
    auto predicate =
        expr_maker.ConjunctionAnd(expr_maker.CompareEq(d_weeknuminyear, expr_maker.Constant(6)),
                                  expr_maker.CompareEq(d_year, expr_maker.Constant(1994)));
    // Make output schema.
    d_seq_scan_out.AddOutput("d_datekey", d_datekey);
    // Build plan node.
    d_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(d_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(d_table->GetId())
                     .Build();
  }

  // Scan lineorder.
  std::unique_ptr<planner::AbstractPlanNode> lo_seq_scan;
  planner::OutputSchemaHelper lo_seq_scan_out{&expr_maker, 1};
  {
    auto lo_orderdate = expr_maker.CVE(lo_schema.GetColumnInfo("lo_orderdate"));
    auto lo_extendedprice = expr_maker.CVE(lo_schema.GetColumnInfo("lo_extendedprice"));
    auto lo_discount = expr_maker.CVE(lo_schema.GetColumnInfo("lo_discount"));
    auto lo_quantity = expr_maker.CVE(lo_schema.GetColumnInfo("lo_quantity"));
    // Make predicate: lo_discount between 5 and 7 AND lo_quantity between 26 and 35
    auto predicate = expr_maker.ConjunctionAnd(
        expr_maker.CompareBetween(lo_discount, expr_maker.Constant(5), expr_maker.Constant(7)),
        expr_maker.CompareBetween(lo_quantity, expr_maker.Constant(26), expr_maker.Constant(35)));
    // Make output schema.
    lo_seq_scan_out.AddOutput("lo_orderdate", lo_orderdate);
    lo_seq_scan_out.AddOutput("lo_extendedprice", lo_extendedprice);
    lo_seq_scan_out.AddOutput("lo_discount", lo_discount);
    // Build plan node.
    planner::SeqScanPlanNode::Builder builder;
    lo_seq_scan = builder.SetOutputSchema(lo_seq_scan_out.MakeSchema())
                      .SetScanPredicate(predicate)
                      .SetTableOid(lo_table->GetId())
                      .Build();
  }

  // date <-> lineorder join.
  std::unique_ptr<planner::AbstractPlanNode> hash_join;
  planner::OutputSchemaHelper hash_join_out{&expr_maker, 0};
  {
    // Left columns.
    auto d_datekey = d_seq_scan_out.GetOutput("d_datekey");
    // Right columns.
    auto lo_orderdate = lo_seq_scan_out.GetOutput("lo_orderdate");
    auto lo_extendedprice = lo_seq_scan_out.GetOutput("lo_extendedprice");
    auto lo_discount = lo_seq_scan_out.GetOutput("lo_discount");
    // Output Schema
    hash_join_out.AddOutput("lo_extendedprice", lo_extendedprice);
    hash_join_out.AddOutput("lo_discount", lo_discount);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join = builder.AddChild(std::move(d_seq_scan))
                    .AddChild(std::move(lo_seq_scan))
                    .SetOutputSchema(hash_join_out.MakeSchema())
                    .AddLeftHashKey(d_datekey)
                    .AddRightHashKey(lo_orderdate)
                    .SetJoinType(planner::LogicalJoinType::INNER)
                    .SetJoinPredicate(expr_maker.CompareEq(d_datekey, lo_orderdate))
                    .Build();
  }

  // Make the aggregate
  std::unique_ptr<planner::AbstractPlanNode> agg;
  planner::OutputSchemaHelper agg_out{&expr_maker, 0};
  {
    // Read previous layer's output
    auto lo_extendedprice = hash_join_out.GetOutput("lo_extendedprice");
    auto lo_discount = hash_join_out.GetOutput("lo_discount");
    auto revenue = expr_maker.AggSum(expr_maker.OpMul(lo_extendedprice, lo_discount));
    // Add them to the helper.
    agg_out.AddAggTerm("revenue", revenue);
    // Make the output schema.
    agg_out.AddOutput("revenue", agg_out.GetAggTermForOutput("revenue"));
    // Build plan node.
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(agg_out.MakeSchema())
              .AddAggregateTerm(revenue)
              .AddChild(std::move(hash_join))
              .SetAggregateStrategyType(planner::AggregateStrategyType::PLAIN)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }

  // Compile plan!
  auto query = CompilationContext::Compile(*agg);

  // Run Once to force compilation
  NoOpResultConsumer consumer;
  {
    sql::MemoryPool memory(nullptr);
    sql::ExecutionContext exec_ctx(&memory, agg->GetOutputSchema(), &consumer);
    query->Run(&exec_ctx, kExecutionMode);
  }

  // Only time execution.
  for (auto _ : state) {
    sql::MemoryPool memory(nullptr);
    sql::ExecutionContext exec_ctx(&memory, agg->GetOutputSchema(), &consumer);
    query->Run(&exec_ctx, kExecutionMode);
  }
}

BENCHMARK_DEFINE_F(StarSchemaBenchmark, Q2_1)(benchmark::State &state) {
  auto catalog = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;
  // Date
  sql::Table *d_table = catalog->LookupTableByName("ssbm.date");
  const auto &d_schema = d_table->GetSchema();
  // Part
  sql::Table *p_table = catalog->LookupTableByName("ssbm.part");
  const auto &p_schema = p_table->GetSchema();
  // Supplier
  sql::Table *s_table = catalog->LookupTableByName("ssbm.supplier");
  const auto &s_schema = s_table->GetSchema();
  // LineOrder
  sql::Table *lo_table = catalog->LookupTableByName("ssbm.lineorder");
  const auto &lo_schema = lo_table->GetSchema();

  // Scan date
  std::unique_ptr<planner::AbstractPlanNode> d_seq_scan;
  planner::OutputSchemaHelper d_seq_scan_out{&expr_maker, 1};
  {
    auto d_datekey = expr_maker.CVE(d_schema.GetColumnInfo("d_datekey"));
    auto d_year = expr_maker.CVE(d_schema.GetColumnInfo("d_year"));
    // Make output schema.
    d_seq_scan_out.AddOutput("d_datekey", d_datekey);
    d_seq_scan_out.AddOutput("d_year", d_year);
    // Build plan node.
    d_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(d_seq_scan_out.MakeSchema())
                     .SetScanPredicate(nullptr)
                     .SetTableOid(d_table->GetId())
                     .Build();
  }

  // Scan part
  std::unique_ptr<planner::AbstractPlanNode> p_seq_scan;
  planner::OutputSchemaHelper p_seq_scan_out{&expr_maker, 0};
  {
    auto p_partkey = expr_maker.CVE(p_schema.GetColumnInfo("p_partkey"));
    auto p_brand1 = expr_maker.CVE(p_schema.GetColumnInfo("p_brand1"));
    auto p_category = expr_maker.CVE(p_schema.GetColumnInfo("p_category"));
    // Make the predicate: p_category = 'MFGR#12'
    auto predicate = expr_maker.CompareEq(p_category, expr_maker.Constant("MFGR#12"));
    // Make output schema.
    p_seq_scan_out.AddOutput("p_partkey", p_partkey);
    p_seq_scan_out.AddOutput("p_brand1", p_brand1);
    // Build plan node.
    p_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(p_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(p_table->GetId())
                     .Build();
  }

  // Scan supplier
  std::unique_ptr<planner::AbstractPlanNode> s_seq_scan;
  planner::OutputSchemaHelper s_seq_scan_out{&expr_maker, 0};
  {
    auto s_suppkey = expr_maker.CVE(s_schema.GetColumnInfo("s_suppkey"));
    auto s_region = expr_maker.CVE(s_schema.GetColumnInfo("s_region"));
    // Make the predicate: s_region = 'AMERICA'
    auto predicate = expr_maker.CompareEq(s_region, expr_maker.Constant("AMERICA"));
    // Make output schema.
    s_seq_scan_out.AddOutput("s_suppkey", s_suppkey);
    // Build plan node.
    s_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(s_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(s_table->GetId())
                     .Build();
  }

  // Scan lineorder.
  std::unique_ptr<planner::AbstractPlanNode> lo_seq_scan;
  planner::OutputSchemaHelper lo_seq_scan_out{&expr_maker, 1};
  {
    auto lo_orderdate = expr_maker.CVE(lo_schema.GetColumnInfo("lo_orderdate"));
    auto lo_partkey = expr_maker.CVE(lo_schema.GetColumnInfo("lo_partkey"));
    auto lo_suppkey = expr_maker.CVE(lo_schema.GetColumnInfo("lo_suppkey"));
    auto lo_revenue = expr_maker.CVE(lo_schema.GetColumnInfo("lo_revenue"));
    // Make output schema.
    lo_seq_scan_out.AddOutput("lo_orderdate", lo_orderdate);
    lo_seq_scan_out.AddOutput("lo_partkey", lo_partkey);
    lo_seq_scan_out.AddOutput("lo_suppkey", lo_suppkey);
    lo_seq_scan_out.AddOutput("lo_revenue", lo_revenue);
    // Build plan node.
    planner::SeqScanPlanNode::Builder builder;
    lo_seq_scan = builder.SetOutputSchema(lo_seq_scan_out.MakeSchema())
                      .SetScanPredicate(nullptr)
                      .SetTableOid(lo_table->GetId())
                      .Build();
  }

  // part <-> lineorder ==> HJ1
  std::unique_ptr<planner::AbstractPlanNode> hash_join1;
  planner::OutputSchemaHelper hash_join_out1{&expr_maker, 1};
  {
    // Left columns.
    auto p_partkey = p_seq_scan_out.GetOutput("p_partkey");
    auto p_brand1 = p_seq_scan_out.GetOutput("p_brand1");
    // Right columns.
    auto lo_orderdate = lo_seq_scan_out.GetOutput("lo_orderdate");
    auto lo_partkey = lo_seq_scan_out.GetOutput("lo_partkey");
    auto lo_suppkey = lo_seq_scan_out.GetOutput("lo_suppkey");
    auto lo_revenue = lo_seq_scan_out.GetOutput("lo_revenue");
    // Output Schema
    hash_join_out1.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out1.AddOutput("lo_suppkey", lo_suppkey);
    hash_join_out1.AddOutput("lo_revenue", lo_revenue);
    hash_join_out1.AddOutput("p_brand1", p_brand1);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join1 = builder.AddChild(std::move(p_seq_scan))
                     .AddChild(std::move(lo_seq_scan))
                     .SetOutputSchema(hash_join_out1.MakeSchema())
                     .AddLeftHashKey(p_partkey)
                     .AddRightHashKey(lo_partkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.CompareEq(p_partkey, lo_partkey))
                     .Build();
  }

  // supplier <-> HJ1   ==> HJ2
  std::unique_ptr<planner::AbstractPlanNode> hash_join2;
  planner::OutputSchemaHelper hash_join_out2{&expr_maker, 0};
  {
    // Left columns.
    auto s_suppkey = s_seq_scan_out.GetOutput("s_suppkey");
    // Right columns.
    auto lo_orderdate = hash_join_out1.GetOutput("lo_orderdate");
    auto lo_suppkey = hash_join_out1.GetOutput("lo_suppkey");
    auto lo_revenue = hash_join_out1.GetOutput("lo_revenue");
    auto p_brand1 = hash_join_out1.GetOutput("p_brand1");
    // Output Schema
    hash_join_out2.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out2.AddOutput("lo_revenue", lo_revenue);
    hash_join_out2.AddOutput("p_brand1", p_brand1);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join2 = builder.AddChild(std::move(s_seq_scan))
                     .AddChild(std::move(hash_join1))
                     .SetOutputSchema(hash_join_out2.MakeSchema())
                     .AddLeftHashKey(s_suppkey)
                     .AddRightHashKey(lo_suppkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.CompareEq(s_suppkey, lo_suppkey))
                     .Build();
  }

  // HJ2 <-> date       ==> HJ3
  std::unique_ptr<planner::AbstractPlanNode> hash_join3;
  planner::OutputSchemaHelper hash_join_out3{&expr_maker, 0};
  {
    // Left columns.
    auto lo_orderdate = hash_join_out2.GetOutput("lo_orderdate");
    auto lo_revenue = hash_join_out2.GetOutput("lo_revenue");
    auto p_brand1 = hash_join_out2.GetOutput("p_brand1");
    // Right columns.
    auto d_datekey = d_seq_scan_out.GetOutput("d_datekey");
    auto d_year = d_seq_scan_out.GetOutput("d_year");
    // Output Schema
    hash_join_out3.AddOutput("lo_revenue", lo_revenue);
    hash_join_out3.AddOutput("d_year", d_year);
    hash_join_out3.AddOutput("p_brand1", p_brand1);
    // Build.
    planner::HashJoinPlanNode::Builder builder;
    hash_join3 = builder.AddChild(std::move(hash_join2))
                     .AddChild(std::move(d_seq_scan))
                     .SetOutputSchema(hash_join_out3.MakeSchema())
                     .AddLeftHashKey(lo_orderdate)
                     .AddRightHashKey(d_datekey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.CompareEq(lo_orderdate, d_datekey))
                     .Build();
  }

  // Aggregate.
  std::unique_ptr<planner::AbstractPlanNode> agg;
  planner::OutputSchemaHelper agg_out{&expr_maker, 0};
  {
    // Read previous layer's output
    auto lo_revenue = hash_join_out3.GetOutput("lo_revenue");
    auto d_year = hash_join_out3.GetOutput("d_year");
    auto p_brand1 = hash_join_out3.GetOutput("p_brand1");
    auto revenue = expr_maker.AggSum(lo_revenue);
    // Add them to the helper.
    agg_out.AddGroupByTerm("d_year", d_year);
    agg_out.AddGroupByTerm("p_brand1", p_brand1);
    agg_out.AddAggTerm("revenue", revenue);
    // Make the output schema.
    agg_out.AddOutput("revenue", agg_out.GetAggTermForOutput("revenue"));
    agg_out.AddOutput("d_year", agg_out.GetGroupByTermForOutput("d_year"));
    agg_out.AddOutput("p_brand1", agg_out.GetGroupByTermForOutput("p_brand1"));
    // Build plan node.
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(agg_out.MakeSchema())
              .AddAggregateTerm(revenue)
              .AddGroupByTerm(d_year)
              .AddGroupByTerm(p_brand1)
              .AddChild(std::move(hash_join3))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }

  // Sort.
  std::unique_ptr<planner::AbstractPlanNode> sort;
  planner::OutputSchemaHelper sort_out{&expr_maker, 0};
  {
    // Read previous layer's output
    auto revenue = agg_out.GetOutput("revenue");
    auto d_year = agg_out.GetOutput("d_year");
    auto p_brand1 = agg_out.GetOutput("p_brand1");
    // Setup output.
    sort_out.AddOutput("revenue", revenue);
    sort_out.AddOutput("d_year", d_year);
    sort_out.AddOutput("p_brand1", p_brand1);
    // Build.
    sort = planner::OrderByPlanNode::Builder{}
               .SetOutputSchema(sort_out.MakeSchema())
               .AddChild(std::move(agg))
               .AddSortKey(d_year, planner::OrderByOrderingType::ASC)
               .AddSortKey(p_brand1, planner::OrderByOrderingType::ASC)
               .Build();
  }

  // Compile plan
  auto query = CompilationContext::Compile(*sort);

  // Consumer.
  NoOpResultConsumer consumer;

  // Run Once to force compilation
  {
    sql::MemoryPool memory(nullptr);
    sql::ExecutionContext exec_ctx(&memory, sort->GetOutputSchema(), &consumer);
    query->Run(&exec_ctx, kExecutionMode);
  }

  // Only time execution.
  for (auto _ : state) {
    sql::MemoryPool memory(nullptr);
    sql::ExecutionContext exec_ctx(&memory, sort->GetOutputSchema(), &consumer);
    query->Run(&exec_ctx, kExecutionMode);
  }
}

BENCHMARK_DEFINE_F(StarSchemaBenchmark, Q2_2)(benchmark::State &state) {
  auto catalog = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;
  // Date
  sql::Table *d_table = catalog->LookupTableByName("ssbm.date");
  const auto &d_schema = d_table->GetSchema();
  // Part
  sql::Table *p_table = catalog->LookupTableByName("ssbm.part");
  const auto &p_schema = p_table->GetSchema();
  // Supplier
  sql::Table *s_table = catalog->LookupTableByName("ssbm.supplier");
  const auto &s_schema = s_table->GetSchema();
  // LineOrder
  sql::Table *lo_table = catalog->LookupTableByName("ssbm.lineorder");
  const auto &lo_schema = lo_table->GetSchema();

  // Scan date
  std::unique_ptr<planner::AbstractPlanNode> d_seq_scan;
  planner::OutputSchemaHelper d_seq_scan_out{&expr_maker, 1};
  {
    auto d_datekey = expr_maker.CVE(d_schema.GetColumnInfo("d_datekey"));
    auto d_year = expr_maker.CVE(d_schema.GetColumnInfo("d_year"));
    // Make output schema.
    d_seq_scan_out.AddOutput("d_datekey", d_datekey);
    d_seq_scan_out.AddOutput("d_year", d_year);
    // Build plan node.
    d_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(d_seq_scan_out.MakeSchema())
                     .SetScanPredicate(nullptr)
                     .SetTableOid(d_table->GetId())
                     .Build();
  }

  // Scan part
  std::unique_ptr<planner::AbstractPlanNode> p_seq_scan;
  planner::OutputSchemaHelper p_seq_scan_out{&expr_maker, 0};
  {
    auto p_partkey = expr_maker.CVE(p_schema.GetColumnInfo("p_partkey"));
    auto p_brand1 = expr_maker.CVE(p_schema.GetColumnInfo("p_brand1"));
    // Make the predicate: p_brand1 between 'MFGR#2221' and 'MFGR#2228'
    auto predicate = expr_maker.CompareBetween(p_brand1, expr_maker.Constant("MFGR#2221"),
                                               expr_maker.Constant("MFGR#2228"));
    // Make output schema.
    p_seq_scan_out.AddOutput("p_partkey", p_partkey);
    p_seq_scan_out.AddOutput("p_brand1", p_brand1);
    // Build plan node.
    p_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(p_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(p_table->GetId())
                     .Build();
  }

  // Scan supplier
  std::unique_ptr<planner::AbstractPlanNode> s_seq_scan;
  planner::OutputSchemaHelper s_seq_scan_out{&expr_maker, 0};
  {
    auto s_suppkey = expr_maker.CVE(s_schema.GetColumnInfo("s_suppkey"));
    auto s_region = expr_maker.CVE(s_schema.GetColumnInfo("s_region"));
    // Make the predicate: s_region = 'ASIA'
    auto predicate = expr_maker.CompareEq(s_region, expr_maker.Constant("ASIA"));
    // Make output schema.
    s_seq_scan_out.AddOutput("s_suppkey", s_suppkey);
    // Build plan node.
    s_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(s_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(s_table->GetId())
                     .Build();
  }

  // Scan lineorder.
  std::unique_ptr<planner::AbstractPlanNode> lo_seq_scan;
  planner::OutputSchemaHelper lo_seq_scan_out{&expr_maker, 1};
  {
    auto lo_orderdate = expr_maker.CVE(lo_schema.GetColumnInfo("lo_orderdate"));
    auto lo_partkey = expr_maker.CVE(lo_schema.GetColumnInfo("lo_partkey"));
    auto lo_suppkey = expr_maker.CVE(lo_schema.GetColumnInfo("lo_suppkey"));
    auto lo_revenue = expr_maker.CVE(lo_schema.GetColumnInfo("lo_revenue"));
    // Make output schema.
    lo_seq_scan_out.AddOutput("lo_orderdate", lo_orderdate);
    lo_seq_scan_out.AddOutput("lo_partkey", lo_partkey);
    lo_seq_scan_out.AddOutput("lo_suppkey", lo_suppkey);
    lo_seq_scan_out.AddOutput("lo_revenue", lo_revenue);
    // Build plan node.
    planner::SeqScanPlanNode::Builder builder;
    lo_seq_scan = builder.SetOutputSchema(lo_seq_scan_out.MakeSchema())
                      .SetScanPredicate(nullptr)
                      .SetTableOid(lo_table->GetId())
                      .Build();
  }

  // part <-> lineorder ==> HJ1
  std::unique_ptr<planner::AbstractPlanNode> hash_join1;
  planner::OutputSchemaHelper hash_join_out1{&expr_maker, 1};
  {
    // Left columns.
    auto p_partkey = p_seq_scan_out.GetOutput("p_partkey");
    auto p_brand1 = p_seq_scan_out.GetOutput("p_brand1");
    // Right columns.
    auto lo_orderdate = lo_seq_scan_out.GetOutput("lo_orderdate");
    auto lo_partkey = lo_seq_scan_out.GetOutput("lo_partkey");
    auto lo_suppkey = lo_seq_scan_out.GetOutput("lo_suppkey");
    auto lo_revenue = lo_seq_scan_out.GetOutput("lo_revenue");
    // Output Schema
    hash_join_out1.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out1.AddOutput("lo_suppkey", lo_suppkey);
    hash_join_out1.AddOutput("lo_revenue", lo_revenue);
    hash_join_out1.AddOutput("p_brand1", p_brand1);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join1 = builder.AddChild(std::move(p_seq_scan))
                     .AddChild(std::move(lo_seq_scan))
                     .SetOutputSchema(hash_join_out1.MakeSchema())
                     .AddLeftHashKey(p_partkey)
                     .AddRightHashKey(lo_partkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.CompareEq(p_partkey, lo_partkey))
                     .Build();
  }

  // supplier <-> HJ1   ==> HJ2
  std::unique_ptr<planner::AbstractPlanNode> hash_join2;
  planner::OutputSchemaHelper hash_join_out2{&expr_maker, 0};
  {
    // Left columns.
    auto s_suppkey = s_seq_scan_out.GetOutput("s_suppkey");
    // Right columns.
    auto lo_orderdate = hash_join_out1.GetOutput("lo_orderdate");
    auto lo_suppkey = hash_join_out1.GetOutput("lo_suppkey");
    auto lo_revenue = hash_join_out1.GetOutput("lo_revenue");
    auto p_brand1 = hash_join_out1.GetOutput("p_brand1");
    // Output Schema
    hash_join_out2.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out2.AddOutput("lo_revenue", lo_revenue);
    hash_join_out2.AddOutput("p_brand1", p_brand1);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join2 = builder.AddChild(std::move(s_seq_scan))
                     .AddChild(std::move(hash_join1))
                     .SetOutputSchema(hash_join_out2.MakeSchema())
                     .AddLeftHashKey(s_suppkey)
                     .AddRightHashKey(lo_suppkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.CompareEq(s_suppkey, lo_suppkey))
                     .Build();
  }

  // HJ2 <-> date       ==> HJ3
  std::unique_ptr<planner::AbstractPlanNode> hash_join3;
  planner::OutputSchemaHelper hash_join_out3{&expr_maker, 0};
  {
    // Left columns.
    auto lo_orderdate = hash_join_out2.GetOutput("lo_orderdate");
    auto lo_revenue = hash_join_out2.GetOutput("lo_revenue");
    auto p_brand1 = hash_join_out2.GetOutput("p_brand1");
    // Right columns.
    auto d_datekey = d_seq_scan_out.GetOutput("d_datekey");
    auto d_year = d_seq_scan_out.GetOutput("d_year");
    // Output Schema
    hash_join_out3.AddOutput("lo_revenue", lo_revenue);
    hash_join_out3.AddOutput("d_year", d_year);
    hash_join_out3.AddOutput("p_brand1", p_brand1);
    // Build.
    planner::HashJoinPlanNode::Builder builder;
    hash_join3 = builder.AddChild(std::move(hash_join2))
                     .AddChild(std::move(d_seq_scan))
                     .SetOutputSchema(hash_join_out3.MakeSchema())
                     .AddLeftHashKey(lo_orderdate)
                     .AddRightHashKey(d_datekey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.CompareEq(lo_orderdate, d_datekey))
                     .Build();
  }

  // Aggregate.
  std::unique_ptr<planner::AbstractPlanNode> agg;
  planner::OutputSchemaHelper agg_out{&expr_maker, 0};
  {
    // Read previous layer's output
    auto lo_revenue = hash_join_out3.GetOutput("lo_revenue");
    auto d_year = hash_join_out3.GetOutput("d_year");
    auto p_brand1 = hash_join_out3.GetOutput("p_brand1");
    auto revenue = expr_maker.AggSum(lo_revenue);
    // Add them to the helper.
    agg_out.AddGroupByTerm("d_year", d_year);
    agg_out.AddGroupByTerm("p_brand1", p_brand1);
    agg_out.AddAggTerm("revenue", revenue);
    // Make the output schema.
    agg_out.AddOutput("revenue", agg_out.GetAggTermForOutput("revenue"));
    agg_out.AddOutput("d_year", agg_out.GetGroupByTermForOutput("d_year"));
    agg_out.AddOutput("p_brand1", agg_out.GetGroupByTermForOutput("p_brand1"));
    // Build plan node.
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(agg_out.MakeSchema())
              .AddAggregateTerm(revenue)
              .AddGroupByTerm(d_year)
              .AddGroupByTerm(p_brand1)
              .AddChild(std::move(hash_join3))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }

  // Sort.
  std::unique_ptr<planner::AbstractPlanNode> sort;
  planner::OutputSchemaHelper sort_out{&expr_maker, 0};
  {
    // Read previous layer's output
    auto revenue = agg_out.GetOutput("revenue");
    auto d_year = agg_out.GetOutput("d_year");
    auto p_brand1 = agg_out.GetOutput("p_brand1");
    // Setup output.
    sort_out.AddOutput("revenue", revenue);
    sort_out.AddOutput("d_year", d_year);
    sort_out.AddOutput("p_brand1", p_brand1);
    // Build.
    sort = planner::OrderByPlanNode::Builder{}
               .SetOutputSchema(sort_out.MakeSchema())
               .AddChild(std::move(agg))
               .AddSortKey(d_year, planner::OrderByOrderingType::ASC)
               .AddSortKey(p_brand1, planner::OrderByOrderingType::ASC)
               .Build();
  }

  // Compile plan
  auto query = CompilationContext::Compile(*sort);

  // Consumer.
  NoOpResultConsumer consumer;

  // Run Once to force compilation
  {
    sql::MemoryPool memory(nullptr);
    sql::ExecutionContext exec_ctx(&memory, sort->GetOutputSchema(), &consumer);
    query->Run(&exec_ctx, kExecutionMode);
  }

  // Only time execution.
  for (auto _ : state) {
    sql::MemoryPool memory(nullptr);
    sql::ExecutionContext exec_ctx(&memory, sort->GetOutputSchema(), &consumer);
    query->Run(&exec_ctx, kExecutionMode);
  }
}

BENCHMARK_DEFINE_F(StarSchemaBenchmark, Q2_3)(benchmark::State &state) {
  auto catalog = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;
  // Date
  sql::Table *d_table = catalog->LookupTableByName("ssbm.date");
  const auto &d_schema = d_table->GetSchema();
  // Part
  sql::Table *p_table = catalog->LookupTableByName("ssbm.part");
  const auto &p_schema = p_table->GetSchema();
  // Supplier
  sql::Table *s_table = catalog->LookupTableByName("ssbm.supplier");
  const auto &s_schema = s_table->GetSchema();
  // LineOrder
  sql::Table *lo_table = catalog->LookupTableByName("ssbm.lineorder");
  const auto &lo_schema = lo_table->GetSchema();

  // Scan date
  std::unique_ptr<planner::AbstractPlanNode> d_seq_scan;
  planner::OutputSchemaHelper d_seq_scan_out{&expr_maker, 1};
  {
    auto d_datekey = expr_maker.CVE(d_schema.GetColumnInfo("d_datekey"));
    auto d_year = expr_maker.CVE(d_schema.GetColumnInfo("d_year"));
    // Make output schema.
    d_seq_scan_out.AddOutput("d_datekey", d_datekey);
    d_seq_scan_out.AddOutput("d_year", d_year);
    // Build plan node.
    d_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(d_seq_scan_out.MakeSchema())
                     .SetScanPredicate(nullptr)
                     .SetTableOid(d_table->GetId())
                     .Build();
  }

  // Scan part
  std::unique_ptr<planner::AbstractPlanNode> p_seq_scan;
  planner::OutputSchemaHelper p_seq_scan_out{&expr_maker, 0};
  {
    auto p_partkey = expr_maker.CVE(p_schema.GetColumnInfo("p_partkey"));
    auto p_brand1 = expr_maker.CVE(p_schema.GetColumnInfo("p_brand1"));
    // Make the predicate: p_brand1 = 'MFGR#2221'
    auto predicate = expr_maker.CompareEq(p_brand1, expr_maker.Constant("MFGR#2221"));
    // Make output schema.
    p_seq_scan_out.AddOutput("p_partkey", p_partkey);
    p_seq_scan_out.AddOutput("p_brand1", p_brand1);
    // Build plan node.
    p_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(p_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(p_table->GetId())
                     .Build();
  }

  // Scan supplier
  std::unique_ptr<planner::AbstractPlanNode> s_seq_scan;
  planner::OutputSchemaHelper s_seq_scan_out{&expr_maker, 0};
  {
    auto s_suppkey = expr_maker.CVE(s_schema.GetColumnInfo("s_suppkey"));
    auto s_region = expr_maker.CVE(s_schema.GetColumnInfo("s_region"));
    // Make the predicate: s_region = 'EUROPE'
    auto predicate = expr_maker.CompareEq(s_region, expr_maker.Constant("EUROPE"));
    // Make output schema.
    s_seq_scan_out.AddOutput("s_suppkey", s_suppkey);
    // Build plan node.
    s_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(s_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(s_table->GetId())
                     .Build();
  }

  // Scan lineorder.
  std::unique_ptr<planner::AbstractPlanNode> lo_seq_scan;
  planner::OutputSchemaHelper lo_seq_scan_out{&expr_maker, 1};
  {
    auto lo_orderdate = expr_maker.CVE(lo_schema.GetColumnInfo("lo_orderdate"));
    auto lo_partkey = expr_maker.CVE(lo_schema.GetColumnInfo("lo_partkey"));
    auto lo_suppkey = expr_maker.CVE(lo_schema.GetColumnInfo("lo_suppkey"));
    auto lo_revenue = expr_maker.CVE(lo_schema.GetColumnInfo("lo_revenue"));
    // Make output schema.
    lo_seq_scan_out.AddOutput("lo_orderdate", lo_orderdate);
    lo_seq_scan_out.AddOutput("lo_partkey", lo_partkey);
    lo_seq_scan_out.AddOutput("lo_suppkey", lo_suppkey);
    lo_seq_scan_out.AddOutput("lo_revenue", lo_revenue);
    // Build plan node.
    planner::SeqScanPlanNode::Builder builder;
    lo_seq_scan = builder.SetOutputSchema(lo_seq_scan_out.MakeSchema())
                      .SetScanPredicate(nullptr)
                      .SetTableOid(lo_table->GetId())
                      .Build();
  }

  // part <-> lineorder ==> HJ1
  std::unique_ptr<planner::AbstractPlanNode> hash_join1;
  planner::OutputSchemaHelper hash_join_out1{&expr_maker, 1};
  {
    // Left columns.
    auto p_partkey = p_seq_scan_out.GetOutput("p_partkey");
    auto p_brand1 = p_seq_scan_out.GetOutput("p_brand1");
    // Right columns.
    auto lo_orderdate = lo_seq_scan_out.GetOutput("lo_orderdate");
    auto lo_partkey = lo_seq_scan_out.GetOutput("lo_partkey");
    auto lo_suppkey = lo_seq_scan_out.GetOutput("lo_suppkey");
    auto lo_revenue = lo_seq_scan_out.GetOutput("lo_revenue");
    // Output Schema
    hash_join_out1.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out1.AddOutput("lo_suppkey", lo_suppkey);
    hash_join_out1.AddOutput("lo_revenue", lo_revenue);
    hash_join_out1.AddOutput("p_brand1", p_brand1);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join1 = builder.AddChild(std::move(p_seq_scan))
                     .AddChild(std::move(lo_seq_scan))
                     .SetOutputSchema(hash_join_out1.MakeSchema())
                     .AddLeftHashKey(p_partkey)
                     .AddRightHashKey(lo_partkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.CompareEq(p_partkey, lo_partkey))
                     .Build();
  }

  // supplier <-> HJ1   ==> HJ2
  std::unique_ptr<planner::AbstractPlanNode> hash_join2;
  planner::OutputSchemaHelper hash_join_out2{&expr_maker, 0};
  {
    // Left columns.
    auto s_suppkey = s_seq_scan_out.GetOutput("s_suppkey");
    // Right columns.
    auto lo_orderdate = hash_join_out1.GetOutput("lo_orderdate");
    auto lo_suppkey = hash_join_out1.GetOutput("lo_suppkey");
    auto lo_revenue = hash_join_out1.GetOutput("lo_revenue");
    auto p_brand1 = hash_join_out1.GetOutput("p_brand1");
    // Output Schema
    hash_join_out2.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out2.AddOutput("lo_revenue", lo_revenue);
    hash_join_out2.AddOutput("p_brand1", p_brand1);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join2 = builder.AddChild(std::move(s_seq_scan))
                     .AddChild(std::move(hash_join1))
                     .SetOutputSchema(hash_join_out2.MakeSchema())
                     .AddLeftHashKey(s_suppkey)
                     .AddRightHashKey(lo_suppkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.CompareEq(s_suppkey, lo_suppkey))
                     .Build();
  }

  // HJ2 <-> date       ==> HJ3
  std::unique_ptr<planner::AbstractPlanNode> hash_join3;
  planner::OutputSchemaHelper hash_join_out3{&expr_maker, 0};
  {
    // Left columns.
    auto lo_orderdate = hash_join_out2.GetOutput("lo_orderdate");
    auto lo_revenue = hash_join_out2.GetOutput("lo_revenue");
    auto p_brand1 = hash_join_out2.GetOutput("p_brand1");
    // Right columns.
    auto d_datekey = d_seq_scan_out.GetOutput("d_datekey");
    auto d_year = d_seq_scan_out.GetOutput("d_year");
    // Output Schema
    hash_join_out3.AddOutput("lo_revenue", lo_revenue);
    hash_join_out3.AddOutput("d_year", d_year);
    hash_join_out3.AddOutput("p_brand1", p_brand1);
    // Build.
    planner::HashJoinPlanNode::Builder builder;
    hash_join3 = builder.AddChild(std::move(hash_join2))
                     .AddChild(std::move(d_seq_scan))
                     .SetOutputSchema(hash_join_out3.MakeSchema())
                     .AddLeftHashKey(lo_orderdate)
                     .AddRightHashKey(d_datekey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.CompareEq(lo_orderdate, d_datekey))
                     .Build();
  }

  // Aggregate.
  std::unique_ptr<planner::AbstractPlanNode> agg;
  planner::OutputSchemaHelper agg_out{&expr_maker, 0};
  {
    // Read previous layer's output
    auto lo_revenue = hash_join_out3.GetOutput("lo_revenue");
    auto d_year = hash_join_out3.GetOutput("d_year");
    auto p_brand1 = hash_join_out3.GetOutput("p_brand1");
    auto revenue = expr_maker.AggSum(lo_revenue);
    // Add them to the helper.
    agg_out.AddGroupByTerm("d_year", d_year);
    agg_out.AddGroupByTerm("p_brand1", p_brand1);
    agg_out.AddAggTerm("revenue", revenue);
    // Make the output schema.
    agg_out.AddOutput("revenue", agg_out.GetAggTermForOutput("revenue"));
    agg_out.AddOutput("d_year", agg_out.GetGroupByTermForOutput("d_year"));
    agg_out.AddOutput("p_brand1", agg_out.GetGroupByTermForOutput("p_brand1"));
    // Build plan node.
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(agg_out.MakeSchema())
              .AddAggregateTerm(revenue)
              .AddGroupByTerm(d_year)
              .AddGroupByTerm(p_brand1)
              .AddChild(std::move(hash_join3))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }

  // Sort.
  std::unique_ptr<planner::AbstractPlanNode> sort;
  planner::OutputSchemaHelper sort_out{&expr_maker, 0};
  {
    // Read previous layer's output
    auto revenue = agg_out.GetOutput("revenue");
    auto d_year = agg_out.GetOutput("d_year");
    auto p_brand1 = agg_out.GetOutput("p_brand1");
    // Setup output.
    sort_out.AddOutput("revenue", revenue);
    sort_out.AddOutput("d_year", d_year);
    sort_out.AddOutput("p_brand1", p_brand1);
    // Build.
    sort = planner::OrderByPlanNode::Builder{}
               .SetOutputSchema(sort_out.MakeSchema())
               .AddChild(std::move(agg))
               .AddSortKey(d_year, planner::OrderByOrderingType::ASC)
               .AddSortKey(p_brand1, planner::OrderByOrderingType::ASC)
               .Build();
  }

  // Compile plan
  auto query = CompilationContext::Compile(*sort);

  // Consumer.
  NoOpResultConsumer consumer;

  // Run Once to force compilation
  {
    sql::MemoryPool memory(nullptr);
    sql::ExecutionContext exec_ctx(&memory, sort->GetOutputSchema(), &consumer);
    query->Run(&exec_ctx, kExecutionMode);
  }

  // Only time execution.
  for (auto _ : state) {
    sql::MemoryPool memory(nullptr);
    sql::ExecutionContext exec_ctx(&memory, sort->GetOutputSchema(), &consumer);
    query->Run(&exec_ctx, kExecutionMode);
  }
}

BENCHMARK_DEFINE_F(StarSchemaBenchmark, Q3_1)(benchmark::State &state) {
  auto catalog = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;
  // Date
  sql::Table *d_table = catalog->LookupTableByName("ssbm.date");
  const auto &d_schema = d_table->GetSchema();
  // Customer
  sql::Table *c_table = catalog->LookupTableByName("ssbm.customer");
  const auto &c_schema = c_table->GetSchema();
  // Supplier
  sql::Table *s_table = catalog->LookupTableByName("ssbm.supplier");
  const auto &s_schema = s_table->GetSchema();
  // LineOrder
  sql::Table *lo_table = catalog->LookupTableByName("ssbm.lineorder");
  const auto &lo_schema = lo_table->GetSchema();

  // Scan date
  std::unique_ptr<planner::AbstractPlanNode> d_seq_scan;
  planner::OutputSchemaHelper d_seq_scan_out{&expr_maker, 0};
  {
    auto d_datekey = expr_maker.CVE(d_schema.GetColumnInfo("d_datekey"));
    auto d_year = expr_maker.CVE(d_schema.GetColumnInfo("d_year"));
    // Make output schema.
    d_seq_scan_out.AddOutput("d_datekey", d_datekey);
    d_seq_scan_out.AddOutput("d_year", d_year);
    // Make predicate: d_year >= 1992 and d_year <= 1997
    auto predicate =
        expr_maker.CompareBetween(d_year, expr_maker.Constant(1992), expr_maker.Constant(1997));
    // Build plan node.
    d_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(d_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(d_table->GetId())
                     .Build();
  }

  // Scan customer
  std::unique_ptr<planner::AbstractPlanNode> c_seq_scan;
  planner::OutputSchemaHelper c_seq_scan_out{&expr_maker, 0};
  {
    auto c_custkey = expr_maker.CVE(c_schema.GetColumnInfo("c_custkey"));
    auto c_nation = expr_maker.CVE(c_schema.GetColumnInfo("c_nation"));
    auto c_region = expr_maker.CVE(c_schema.GetColumnInfo("c_region"));
    // Make the predicate: c_region = 'ASIA'
    auto predicate = expr_maker.CompareEq(c_region, expr_maker.Constant("ASIA"));
    // Make output schema.
    c_seq_scan_out.AddOutput("c_custkey", c_custkey);
    c_seq_scan_out.AddOutput("c_nation", c_nation);
    // Build plan node.
    c_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(c_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(c_table->GetId())
                     .Build();
  }

  // Scan supplier
  std::unique_ptr<planner::AbstractPlanNode> s_seq_scan;
  planner::OutputSchemaHelper s_seq_scan_out{&expr_maker, 0};
  {
    auto s_suppkey = expr_maker.CVE(s_schema.GetColumnInfo("s_suppkey"));
    auto s_nation = expr_maker.CVE(s_schema.GetColumnInfo("s_nation"));
    auto s_region = expr_maker.CVE(s_schema.GetColumnInfo("s_region"));
    // Make the predicate: s_region = 'ASIA'
    auto predicate = expr_maker.CompareEq(s_region, expr_maker.Constant("ASIA"));
    // Make output schema.
    s_seq_scan_out.AddOutput("s_suppkey", s_suppkey);
    s_seq_scan_out.AddOutput("s_nation", s_nation);
    // Build plan node.
    s_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(s_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(s_table->GetId())
                     .Build();
  }

  // Scan lineorder.
  std::unique_ptr<planner::AbstractPlanNode> lo_seq_scan;
  planner::OutputSchemaHelper lo_seq_scan_out{&expr_maker, 1};
  {
    auto lo_orderdate = expr_maker.CVE(lo_schema.GetColumnInfo("lo_orderdate"));
    auto lo_custkey = expr_maker.CVE(lo_schema.GetColumnInfo("lo_custkey"));
    auto lo_suppkey = expr_maker.CVE(lo_schema.GetColumnInfo("lo_suppkey"));
    auto lo_revenue = expr_maker.CVE(lo_schema.GetColumnInfo("lo_revenue"));
    // Make output schema.
    lo_seq_scan_out.AddOutput("lo_orderdate", lo_orderdate);
    lo_seq_scan_out.AddOutput("lo_custkey", lo_custkey);
    lo_seq_scan_out.AddOutput("lo_suppkey", lo_suppkey);
    lo_seq_scan_out.AddOutput("lo_revenue", lo_revenue);
    // Build plan node.
    planner::SeqScanPlanNode::Builder builder;
    lo_seq_scan = builder.SetOutputSchema(lo_seq_scan_out.MakeSchema())
                      .SetScanPredicate(nullptr)
                      .SetTableOid(lo_table->GetId())
                      .Build();
  }

  // customer <-> lineorder ==> HJ1
  std::unique_ptr<planner::AbstractPlanNode> hash_join1;
  planner::OutputSchemaHelper hash_join_out1{&expr_maker, 1};
  {
    // Left columns.
    auto c_custkey = c_seq_scan_out.GetOutput("c_custkey");
    auto c_nation = c_seq_scan_out.GetOutput("c_nation");
    // Right columns.
    auto lo_orderdate = lo_seq_scan_out.GetOutput("lo_orderdate");
    auto lo_custkey = lo_seq_scan_out.GetOutput("lo_custkey");
    auto lo_suppkey = lo_seq_scan_out.GetOutput("lo_suppkey");
    auto lo_revenue = lo_seq_scan_out.GetOutput("lo_revenue");
    // Output Schema
    hash_join_out1.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out1.AddOutput("lo_suppkey", lo_suppkey);
    hash_join_out1.AddOutput("lo_revenue", lo_revenue);
    hash_join_out1.AddOutput("c_nation", c_nation);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join1 = builder.AddChild(std::move(c_seq_scan))
                     .AddChild(std::move(lo_seq_scan))
                     .SetOutputSchema(hash_join_out1.MakeSchema())
                     .AddLeftHashKey(c_custkey)
                     .AddRightHashKey(lo_custkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.CompareEq(c_custkey, lo_custkey))
                     .Build();
  }

  // supplier <-> HJ1   ==> HJ2
  std::unique_ptr<planner::AbstractPlanNode> hash_join2;
  planner::OutputSchemaHelper hash_join_out2{&expr_maker, 1};
  {
    // Left columns.
    auto s_suppkey = s_seq_scan_out.GetOutput("s_suppkey");
    auto s_nation = s_seq_scan_out.GetOutput("s_nation");
    // Right columns.
    auto lo_orderdate = hash_join_out1.GetOutput("lo_orderdate");
    auto lo_suppkey = hash_join_out1.GetOutput("lo_suppkey");
    auto lo_revenue = hash_join_out1.GetOutput("lo_revenue");
    auto c_nation = hash_join_out1.GetOutput("c_nation");
    // Output Schema
    hash_join_out2.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out2.AddOutput("lo_revenue", lo_revenue);
    hash_join_out2.AddOutput("c_nation", c_nation);
    hash_join_out2.AddOutput("s_nation", s_nation);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join2 = builder.AddChild(std::move(s_seq_scan))
                     .AddChild(std::move(hash_join1))
                     .SetOutputSchema(hash_join_out2.MakeSchema())
                     .AddLeftHashKey(s_suppkey)
                     .AddRightHashKey(lo_suppkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.CompareEq(s_suppkey, lo_suppkey))
                     .Build();
  }

  // date <-> HJ2       ==> HJ3
  std::unique_ptr<planner::AbstractPlanNode> hash_join3;
  planner::OutputSchemaHelper hash_join_out3{&expr_maker, 0};
  {
    // Left columns.
    auto d_datekey = d_seq_scan_out.GetOutput("d_datekey");
    auto d_year = d_seq_scan_out.GetOutput("d_year");
    // Right columns.
    auto lo_orderdate = hash_join_out2.GetOutput("lo_orderdate");
    auto lo_revenue = hash_join_out2.GetOutput("lo_revenue");
    auto c_nation = hash_join_out2.GetOutput("c_nation");
    auto s_nation = hash_join_out2.GetOutput("s_nation");
    // Output Schema
    hash_join_out3.AddOutput("c_nation", c_nation);
    hash_join_out3.AddOutput("s_nation", s_nation);
    hash_join_out3.AddOutput("d_year", d_year);
    hash_join_out3.AddOutput("lo_revenue", lo_revenue);
    // Build.
    planner::HashJoinPlanNode::Builder builder;
    hash_join3 = builder.AddChild(std::move(d_seq_scan))
                     .AddChild(std::move(hash_join2))
                     .SetOutputSchema(hash_join_out3.MakeSchema())
                     .AddLeftHashKey(d_datekey)
                     .AddRightHashKey(lo_orderdate)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.CompareEq(d_datekey, lo_orderdate))
                     .Build();
  }

  // Aggregate.
  std::unique_ptr<planner::AbstractPlanNode> agg;
  planner::OutputSchemaHelper agg_out{&expr_maker, 0};
  {
    // Read previous layer's output
    auto c_nation = hash_join_out3.GetOutput("c_nation");
    auto s_nation = hash_join_out3.GetOutput("s_nation");
    auto d_year = hash_join_out3.GetOutput("d_year");
    auto lo_revenue = hash_join_out3.GetOutput("lo_revenue");
    auto revenue = expr_maker.AggSum(lo_revenue);
    // Add them to the helper.
    agg_out.AddGroupByTerm("c_nation", c_nation);
    agg_out.AddGroupByTerm("s_nation", s_nation);
    agg_out.AddGroupByTerm("d_year", d_year);
    agg_out.AddAggTerm("revenue", revenue);
    // Make the output schema.
    agg_out.AddOutput("c_nation", agg_out.GetGroupByTermForOutput("c_nation"));
    agg_out.AddOutput("s_nation", agg_out.GetGroupByTermForOutput("s_nation"));
    agg_out.AddOutput("d_year", agg_out.GetGroupByTermForOutput("d_year"));
    agg_out.AddOutput("revenue", agg_out.GetAggTermForOutput("revenue"));
    // Build plan node.
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(agg_out.MakeSchema())
              .AddAggregateTerm(revenue)
              .AddGroupByTerm(c_nation)
              .AddGroupByTerm(s_nation)
              .AddGroupByTerm(d_year)
              .AddChild(std::move(hash_join3))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }

  // Sort.
  std::unique_ptr<planner::AbstractPlanNode> sort;
  planner::OutputSchemaHelper sort_out{&expr_maker, 0};
  {
    // Read previous layer's output
    auto c_nation = agg_out.GetOutput("c_nation");
    auto s_nation = agg_out.GetOutput("s_nation");
    auto d_year = agg_out.GetOutput("d_year");
    auto revenue = agg_out.GetOutput("revenue");
    // Setup output.
    sort_out.AddOutput("c_nation", c_nation);
    sort_out.AddOutput("s_nation", s_nation);
    sort_out.AddOutput("d_year", d_year);
    sort_out.AddOutput("revenue", revenue);
    // Build.
    sort = planner::OrderByPlanNode::Builder{}
               .SetOutputSchema(sort_out.MakeSchema())
               .AddChild(std::move(agg))
               .AddSortKey(d_year, planner::OrderByOrderingType::ASC)
               .AddSortKey(revenue, planner::OrderByOrderingType::DESC)
               .Build();
  }

  // Compile plan
  auto query = CompilationContext::Compile(*sort);

  // Consumer.
  NoOpResultConsumer consumer;

  // Run Once to force compilation
  {
    sql::MemoryPool memory(nullptr);
    sql::ExecutionContext exec_ctx(&memory, sort->GetOutputSchema(), &consumer);
    query->Run(&exec_ctx, kExecutionMode);
  }

  // Only time execution.
  for (auto _ : state) {
    sql::MemoryPool memory(nullptr);
    sql::ExecutionContext exec_ctx(&memory, sort->GetOutputSchema(), &consumer);
    query->Run(&exec_ctx, kExecutionMode);
  }
}

BENCHMARK_DEFINE_F(StarSchemaBenchmark, Q3_2)(benchmark::State &state) {
  auto catalog = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;
  // Date
  sql::Table *d_table = catalog->LookupTableByName("ssbm.date");
  const auto &d_schema = d_table->GetSchema();
  // Customer
  sql::Table *c_table = catalog->LookupTableByName("ssbm.customer");
  const auto &c_schema = c_table->GetSchema();
  // Supplier
  sql::Table *s_table = catalog->LookupTableByName("ssbm.supplier");
  const auto &s_schema = s_table->GetSchema();
  // LineOrder
  sql::Table *lo_table = catalog->LookupTableByName("ssbm.lineorder");
  const auto &lo_schema = lo_table->GetSchema();

  // Scan date
  std::unique_ptr<planner::AbstractPlanNode> d_seq_scan;
  planner::OutputSchemaHelper d_seq_scan_out{&expr_maker, 0};
  {
    auto d_datekey = expr_maker.CVE(d_schema.GetColumnInfo("d_datekey"));
    auto d_year = expr_maker.CVE(d_schema.GetColumnInfo("d_year"));
    // Make output schema.
    d_seq_scan_out.AddOutput("d_datekey", d_datekey);
    d_seq_scan_out.AddOutput("d_year", d_year);
    // Make predicate: d_year >= 1992 and d_year <= 1997
    auto predicate =
        expr_maker.CompareBetween(d_year, expr_maker.Constant(1992), expr_maker.Constant(1997));
    // Build plan node.
    d_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(d_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(d_table->GetId())
                     .Build();
  }

  // Scan customer
  std::unique_ptr<planner::AbstractPlanNode> c_seq_scan;
  planner::OutputSchemaHelper c_seq_scan_out{&expr_maker, 0};
  {
    auto c_custkey = expr_maker.CVE(c_schema.GetColumnInfo("c_custkey"));
    auto c_nation = expr_maker.CVE(c_schema.GetColumnInfo("c_nation"));
    auto c_city = expr_maker.CVE(c_schema.GetColumnInfo("c_city"));
    // Make the predicate: c_nation = 'UNITED STATES'
    auto predicate = expr_maker.CompareEq(c_nation, expr_maker.Constant("UNITED STATES"));
    // Make output schema.
    c_seq_scan_out.AddOutput("c_custkey", c_custkey);
    c_seq_scan_out.AddOutput("c_city", c_city);
    // Build plan node.
    c_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(c_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(c_table->GetId())
                     .Build();
  }

  // Scan supplier
  std::unique_ptr<planner::AbstractPlanNode> s_seq_scan;
  planner::OutputSchemaHelper s_seq_scan_out{&expr_maker, 0};
  {
    auto s_suppkey = expr_maker.CVE(s_schema.GetColumnInfo("s_suppkey"));
    auto s_nation = expr_maker.CVE(s_schema.GetColumnInfo("s_nation"));
    auto s_city = expr_maker.CVE(s_schema.GetColumnInfo("s_city"));
    // Make the predicate: s_nation = 'UNITED STATES'
    auto predicate = expr_maker.CompareEq(s_nation, expr_maker.Constant("UNITED STATES"));
    // Make output schema.
    s_seq_scan_out.AddOutput("s_suppkey", s_suppkey);
    s_seq_scan_out.AddOutput("s_city", s_city);
    // Build plan node.
    s_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(s_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(s_table->GetId())
                     .Build();
  }

  // Scan lineorder.
  std::unique_ptr<planner::AbstractPlanNode> lo_seq_scan;
  planner::OutputSchemaHelper lo_seq_scan_out{&expr_maker, 1};
  {
    auto lo_orderdate = expr_maker.CVE(lo_schema.GetColumnInfo("lo_orderdate"));
    auto lo_custkey = expr_maker.CVE(lo_schema.GetColumnInfo("lo_custkey"));
    auto lo_suppkey = expr_maker.CVE(lo_schema.GetColumnInfo("lo_suppkey"));
    auto lo_revenue = expr_maker.CVE(lo_schema.GetColumnInfo("lo_revenue"));
    // Make output schema.
    lo_seq_scan_out.AddOutput("lo_orderdate", lo_orderdate);
    lo_seq_scan_out.AddOutput("lo_custkey", lo_custkey);
    lo_seq_scan_out.AddOutput("lo_suppkey", lo_suppkey);
    lo_seq_scan_out.AddOutput("lo_revenue", lo_revenue);
    // Build plan node.
    planner::SeqScanPlanNode::Builder builder;
    lo_seq_scan = builder.SetOutputSchema(lo_seq_scan_out.MakeSchema())
                      .SetScanPredicate(nullptr)
                      .SetTableOid(lo_table->GetId())
                      .Build();
  }

  // customer <-> lineorder ==> HJ1
  std::unique_ptr<planner::AbstractPlanNode> hash_join1;
  planner::OutputSchemaHelper hash_join_out1{&expr_maker, 1};
  {
    // Left columns.
    auto c_custkey = c_seq_scan_out.GetOutput("c_custkey");
    auto c_city = c_seq_scan_out.GetOutput("c_city");
    // Right columns.
    auto lo_orderdate = lo_seq_scan_out.GetOutput("lo_orderdate");
    auto lo_custkey = lo_seq_scan_out.GetOutput("lo_custkey");
    auto lo_suppkey = lo_seq_scan_out.GetOutput("lo_suppkey");
    auto lo_revenue = lo_seq_scan_out.GetOutput("lo_revenue");
    // Output Schema
    hash_join_out1.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out1.AddOutput("lo_suppkey", lo_suppkey);
    hash_join_out1.AddOutput("lo_revenue", lo_revenue);
    hash_join_out1.AddOutput("c_city", c_city);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join1 = builder.AddChild(std::move(c_seq_scan))
                     .AddChild(std::move(lo_seq_scan))
                     .SetOutputSchema(hash_join_out1.MakeSchema())
                     .AddLeftHashKey(c_custkey)
                     .AddRightHashKey(lo_custkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.CompareEq(c_custkey, lo_custkey))
                     .Build();
  }

  // supplier <-> HJ1   ==> HJ2
  std::unique_ptr<planner::AbstractPlanNode> hash_join2;
  planner::OutputSchemaHelper hash_join_out2{&expr_maker, 1};
  {
    // Left columns.
    auto s_suppkey = s_seq_scan_out.GetOutput("s_suppkey");
    auto s_city = s_seq_scan_out.GetOutput("s_city");
    // Right columns.
    auto lo_orderdate = hash_join_out1.GetOutput("lo_orderdate");
    auto lo_suppkey = hash_join_out1.GetOutput("lo_suppkey");
    auto lo_revenue = hash_join_out1.GetOutput("lo_revenue");
    auto c_city = hash_join_out1.GetOutput("c_city");
    // Output Schema
    hash_join_out2.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out2.AddOutput("lo_revenue", lo_revenue);
    hash_join_out2.AddOutput("c_city", c_city);
    hash_join_out2.AddOutput("s_city", s_city);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join2 = builder.AddChild(std::move(s_seq_scan))
                     .AddChild(std::move(hash_join1))
                     .SetOutputSchema(hash_join_out2.MakeSchema())
                     .AddLeftHashKey(s_suppkey)
                     .AddRightHashKey(lo_suppkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.CompareEq(s_suppkey, lo_suppkey))
                     .Build();
  }

  // date <-> HJ2       ==> HJ3
  std::unique_ptr<planner::AbstractPlanNode> hash_join3;
  planner::OutputSchemaHelper hash_join_out3{&expr_maker, 0};
  {
    // Left columns.
    auto d_datekey = d_seq_scan_out.GetOutput("d_datekey");
    auto d_year = d_seq_scan_out.GetOutput("d_year");
    // Right columns.
    auto lo_orderdate = hash_join_out2.GetOutput("lo_orderdate");
    auto lo_revenue = hash_join_out2.GetOutput("lo_revenue");
    auto c_city = hash_join_out2.GetOutput("c_city");
    auto s_city = hash_join_out2.GetOutput("s_city");
    // Output Schema
    hash_join_out3.AddOutput("c_city", c_city);
    hash_join_out3.AddOutput("s_city", s_city);
    hash_join_out3.AddOutput("d_year", d_year);
    hash_join_out3.AddOutput("lo_revenue", lo_revenue);
    // Build.
    planner::HashJoinPlanNode::Builder builder;
    hash_join3 = builder.AddChild(std::move(d_seq_scan))
                     .AddChild(std::move(hash_join2))
                     .SetOutputSchema(hash_join_out3.MakeSchema())
                     .AddLeftHashKey(d_datekey)
                     .AddRightHashKey(lo_orderdate)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.CompareEq(d_datekey, lo_orderdate))
                     .Build();
  }

  // Aggregate.
  std::unique_ptr<planner::AbstractPlanNode> agg;
  planner::OutputSchemaHelper agg_out{&expr_maker, 0};
  {
    // Read previous layer's output
    auto c_city = hash_join_out3.GetOutput("c_city");
    auto s_city = hash_join_out3.GetOutput("s_city");
    auto d_year = hash_join_out3.GetOutput("d_year");
    auto lo_revenue = hash_join_out3.GetOutput("lo_revenue");
    auto revenue = expr_maker.AggSum(lo_revenue);
    // Add them to the helper.
    agg_out.AddGroupByTerm("c_city", c_city);
    agg_out.AddGroupByTerm("s_city", s_city);
    agg_out.AddGroupByTerm("d_year", d_year);
    agg_out.AddAggTerm("revenue", revenue);
    // Make the output schema.
    agg_out.AddOutput("c_city", agg_out.GetGroupByTermForOutput("c_city"));
    agg_out.AddOutput("s_city", agg_out.GetGroupByTermForOutput("s_city"));
    agg_out.AddOutput("d_year", agg_out.GetGroupByTermForOutput("d_year"));
    agg_out.AddOutput("revenue", agg_out.GetAggTermForOutput("revenue"));
    // Build plan node.
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(agg_out.MakeSchema())
              .AddAggregateTerm(revenue)
              .AddGroupByTerm(c_city)
              .AddGroupByTerm(s_city)
              .AddGroupByTerm(d_year)
              .AddChild(std::move(hash_join3))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }

  // Sort.
  std::unique_ptr<planner::AbstractPlanNode> sort;
  planner::OutputSchemaHelper sort_out{&expr_maker, 0};
  {
    // Read previous layer's output
    auto c_city = agg_out.GetOutput("c_city");
    auto s_city = agg_out.GetOutput("s_city");
    auto d_year = agg_out.GetOutput("d_year");
    auto revenue = agg_out.GetOutput("revenue");
    // Setup output.
    sort_out.AddOutput("c_city", c_city);
    sort_out.AddOutput("s_city", s_city);
    sort_out.AddOutput("d_year", d_year);
    sort_out.AddOutput("revenue", revenue);
    // Build.
    sort = planner::OrderByPlanNode::Builder{}
               .SetOutputSchema(sort_out.MakeSchema())
               .AddChild(std::move(agg))
               .AddSortKey(d_year, planner::OrderByOrderingType::ASC)
               .AddSortKey(revenue, planner::OrderByOrderingType::DESC)
               .Build();
  }

  // Compile plan
  auto query = CompilationContext::Compile(*sort);

  // Consumer.
  NoOpResultConsumer consumer;

  // Run Once to force compilation
  {
    sql::MemoryPool memory(nullptr);
    sql::ExecutionContext exec_ctx(&memory, sort->GetOutputSchema(), &consumer);
    query->Run(&exec_ctx, kExecutionMode);
  }

  // Only time execution.
  for (auto _ : state) {
    sql::MemoryPool memory(nullptr);
    sql::ExecutionContext exec_ctx(&memory, sort->GetOutputSchema(), &consumer);
    query->Run(&exec_ctx, kExecutionMode);
  }
}

BENCHMARK_DEFINE_F(StarSchemaBenchmark, Q3_3)(benchmark::State &state) {
  auto catalog = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;
  // Date
  sql::Table *d_table = catalog->LookupTableByName("ssbm.date");
  const auto &d_schema = d_table->GetSchema();
  // Customer
  sql::Table *c_table = catalog->LookupTableByName("ssbm.customer");
  const auto &c_schema = c_table->GetSchema();
  // Supplier
  sql::Table *s_table = catalog->LookupTableByName("ssbm.supplier");
  const auto &s_schema = s_table->GetSchema();
  // LineOrder
  sql::Table *lo_table = catalog->LookupTableByName("ssbm.lineorder");
  const auto &lo_schema = lo_table->GetSchema();

  // Scan date
  std::unique_ptr<planner::AbstractPlanNode> d_seq_scan;
  planner::OutputSchemaHelper d_seq_scan_out{&expr_maker, 0};
  {
    auto d_datekey = expr_maker.CVE(d_schema.GetColumnInfo("d_datekey"));
    auto d_year = expr_maker.CVE(d_schema.GetColumnInfo("d_year"));
    // Make output schema.
    d_seq_scan_out.AddOutput("d_datekey", d_datekey);
    d_seq_scan_out.AddOutput("d_year", d_year);
    // Make predicate: d_year >= 1992 and d_year <= 1997
    auto predicate =
        expr_maker.CompareBetween(d_year, expr_maker.Constant(1992), expr_maker.Constant(1997));
    // Build plan node.
    d_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(d_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(d_table->GetId())
                     .Build();
  }

  // Scan customer
  std::unique_ptr<planner::AbstractPlanNode> c_seq_scan;
  planner::OutputSchemaHelper c_seq_scan_out{&expr_maker, 0};
  {
    auto c_custkey = expr_maker.CVE(c_schema.GetColumnInfo("c_custkey"));
    auto c_city = expr_maker.CVE(c_schema.GetColumnInfo("c_city"));
    // Make the predicate: c_city='UNITED KI1' or c_city='UNITED KI5'
    auto predicate =
        expr_maker.ConjunctionOr(expr_maker.CompareEq(c_city, expr_maker.Constant("UNITED KI1")),
                                 expr_maker.CompareEq(c_city, expr_maker.Constant("UNITED KI5")));
    // Make output schema.
    c_seq_scan_out.AddOutput("c_custkey", c_custkey);
    c_seq_scan_out.AddOutput("c_city", c_city);
    // Build plan node.
    c_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(c_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(c_table->GetId())
                     .Build();
  }

  // Scan supplier
  std::unique_ptr<planner::AbstractPlanNode> s_seq_scan;
  planner::OutputSchemaHelper s_seq_scan_out{&expr_maker, 0};
  {
    auto s_suppkey = expr_maker.CVE(s_schema.GetColumnInfo("s_suppkey"));
    auto s_city = expr_maker.CVE(s_schema.GetColumnInfo("s_city"));
    // Make the predicate: s_city='UNITED KI1' or s_city='UNITED KI5'
    auto predicate =
        expr_maker.ConjunctionOr(expr_maker.CompareEq(s_city, expr_maker.Constant("UNITED KI1")),
                                 expr_maker.CompareEq(s_city, expr_maker.Constant("UNITED KI5")));
    // Make output schema.
    s_seq_scan_out.AddOutput("s_suppkey", s_suppkey);
    s_seq_scan_out.AddOutput("s_city", s_city);
    // Build plan node.
    s_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(s_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(s_table->GetId())
                     .Build();
  }

  // Scan lineorder.
  std::unique_ptr<planner::AbstractPlanNode> lo_seq_scan;
  planner::OutputSchemaHelper lo_seq_scan_out{&expr_maker, 1};
  {
    auto lo_orderdate = expr_maker.CVE(lo_schema.GetColumnInfo("lo_orderdate"));
    auto lo_custkey = expr_maker.CVE(lo_schema.GetColumnInfo("lo_custkey"));
    auto lo_suppkey = expr_maker.CVE(lo_schema.GetColumnInfo("lo_suppkey"));
    auto lo_revenue = expr_maker.CVE(lo_schema.GetColumnInfo("lo_revenue"));
    // Make output schema.
    lo_seq_scan_out.AddOutput("lo_orderdate", lo_orderdate);
    lo_seq_scan_out.AddOutput("lo_custkey", lo_custkey);
    lo_seq_scan_out.AddOutput("lo_suppkey", lo_suppkey);
    lo_seq_scan_out.AddOutput("lo_revenue", lo_revenue);
    // Build plan node.
    planner::SeqScanPlanNode::Builder builder;
    lo_seq_scan = builder.SetOutputSchema(lo_seq_scan_out.MakeSchema())
                      .SetScanPredicate(nullptr)
                      .SetTableOid(lo_table->GetId())
                      .Build();
  }

  // customer <-> lineorder ==> HJ1
  std::unique_ptr<planner::AbstractPlanNode> hash_join1;
  planner::OutputSchemaHelper hash_join_out1{&expr_maker, 1};
  {
    // Left columns.
    auto c_custkey = c_seq_scan_out.GetOutput("c_custkey");
    auto c_city = c_seq_scan_out.GetOutput("c_city");
    // Right columns.
    auto lo_orderdate = lo_seq_scan_out.GetOutput("lo_orderdate");
    auto lo_custkey = lo_seq_scan_out.GetOutput("lo_custkey");
    auto lo_suppkey = lo_seq_scan_out.GetOutput("lo_suppkey");
    auto lo_revenue = lo_seq_scan_out.GetOutput("lo_revenue");
    // Output Schema
    hash_join_out1.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out1.AddOutput("lo_suppkey", lo_suppkey);
    hash_join_out1.AddOutput("lo_revenue", lo_revenue);
    hash_join_out1.AddOutput("c_city", c_city);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join1 = builder.AddChild(std::move(c_seq_scan))
                     .AddChild(std::move(lo_seq_scan))
                     .SetOutputSchema(hash_join_out1.MakeSchema())
                     .AddLeftHashKey(c_custkey)
                     .AddRightHashKey(lo_custkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.CompareEq(c_custkey, lo_custkey))
                     .Build();
  }

  // supplier <-> HJ1   ==> HJ2
  std::unique_ptr<planner::AbstractPlanNode> hash_join2;
  planner::OutputSchemaHelper hash_join_out2{&expr_maker, 1};
  {
    // Left columns.
    auto s_suppkey = s_seq_scan_out.GetOutput("s_suppkey");
    auto s_city = s_seq_scan_out.GetOutput("s_city");
    // Right columns.
    auto lo_orderdate = hash_join_out1.GetOutput("lo_orderdate");
    auto lo_suppkey = hash_join_out1.GetOutput("lo_suppkey");
    auto lo_revenue = hash_join_out1.GetOutput("lo_revenue");
    auto c_city = hash_join_out1.GetOutput("c_city");
    // Output Schema
    hash_join_out2.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out2.AddOutput("lo_revenue", lo_revenue);
    hash_join_out2.AddOutput("c_city", c_city);
    hash_join_out2.AddOutput("s_city", s_city);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join2 = builder.AddChild(std::move(s_seq_scan))
                     .AddChild(std::move(hash_join1))
                     .SetOutputSchema(hash_join_out2.MakeSchema())
                     .AddLeftHashKey(s_suppkey)
                     .AddRightHashKey(lo_suppkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.CompareEq(s_suppkey, lo_suppkey))
                     .Build();
  }

  // date <-> HJ2       ==> HJ3
  std::unique_ptr<planner::AbstractPlanNode> hash_join3;
  planner::OutputSchemaHelper hash_join_out3{&expr_maker, 0};
  {
    // Left columns.
    auto d_datekey = d_seq_scan_out.GetOutput("d_datekey");
    auto d_year = d_seq_scan_out.GetOutput("d_year");
    // Right columns.
    auto lo_orderdate = hash_join_out2.GetOutput("lo_orderdate");
    auto lo_revenue = hash_join_out2.GetOutput("lo_revenue");
    auto c_city = hash_join_out2.GetOutput("c_city");
    auto s_city = hash_join_out2.GetOutput("s_city");
    // Output Schema
    hash_join_out3.AddOutput("c_city", c_city);
    hash_join_out3.AddOutput("s_city", s_city);
    hash_join_out3.AddOutput("d_year", d_year);
    hash_join_out3.AddOutput("lo_revenue", lo_revenue);
    // Build.
    planner::HashJoinPlanNode::Builder builder;
    hash_join3 = builder.AddChild(std::move(d_seq_scan))
                     .AddChild(std::move(hash_join2))
                     .SetOutputSchema(hash_join_out3.MakeSchema())
                     .AddLeftHashKey(d_datekey)
                     .AddRightHashKey(lo_orderdate)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.CompareEq(d_datekey, lo_orderdate))
                     .Build();
  }

  // Aggregate.
  std::unique_ptr<planner::AbstractPlanNode> agg;
  planner::OutputSchemaHelper agg_out{&expr_maker, 0};
  {
    // Read previous layer's output
    auto c_city = hash_join_out3.GetOutput("c_city");
    auto s_city = hash_join_out3.GetOutput("s_city");
    auto d_year = hash_join_out3.GetOutput("d_year");
    auto lo_revenue = hash_join_out3.GetOutput("lo_revenue");
    auto revenue = expr_maker.AggSum(lo_revenue);
    // Add them to the helper.
    agg_out.AddGroupByTerm("c_city", c_city);
    agg_out.AddGroupByTerm("s_city", s_city);
    agg_out.AddGroupByTerm("d_year", d_year);
    agg_out.AddAggTerm("revenue", revenue);
    // Make the output schema.
    agg_out.AddOutput("c_city", agg_out.GetGroupByTermForOutput("c_city"));
    agg_out.AddOutput("s_city", agg_out.GetGroupByTermForOutput("s_city"));
    agg_out.AddOutput("d_year", agg_out.GetGroupByTermForOutput("d_year"));
    agg_out.AddOutput("revenue", agg_out.GetAggTermForOutput("revenue"));
    // Build plan node.
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(agg_out.MakeSchema())
              .AddAggregateTerm(revenue)
              .AddGroupByTerm(c_city)
              .AddGroupByTerm(s_city)
              .AddGroupByTerm(d_year)
              .AddChild(std::move(hash_join3))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }

  // Sort.
  std::unique_ptr<planner::AbstractPlanNode> sort;
  planner::OutputSchemaHelper sort_out{&expr_maker, 0};
  {
    // Read previous layer's output
    auto c_city = agg_out.GetOutput("c_city");
    auto s_city = agg_out.GetOutput("s_city");
    auto d_year = agg_out.GetOutput("d_year");
    auto revenue = agg_out.GetOutput("revenue");
    // Setup output.
    sort_out.AddOutput("c_city", c_city);
    sort_out.AddOutput("s_city", s_city);
    sort_out.AddOutput("d_year", d_year);
    sort_out.AddOutput("revenue", revenue);
    // Build.
    sort = planner::OrderByPlanNode::Builder{}
               .SetOutputSchema(sort_out.MakeSchema())
               .AddChild(std::move(agg))
               .AddSortKey(d_year, planner::OrderByOrderingType::ASC)
               .AddSortKey(revenue, planner::OrderByOrderingType::DESC)
               .Build();
  }

  // Compile plan
  auto query = CompilationContext::Compile(*sort);

  // Consumer.
  NoOpResultConsumer consumer;

  // Run Once to force compilation
  {
    sql::MemoryPool memory(nullptr);
    sql::ExecutionContext exec_ctx(&memory, sort->GetOutputSchema(), &consumer);
    query->Run(&exec_ctx, kExecutionMode);
  }

  // Only time execution.
  for (auto _ : state) {
    sql::MemoryPool memory(nullptr);
    sql::ExecutionContext exec_ctx(&memory, sort->GetOutputSchema(), &consumer);
    query->Run(&exec_ctx, kExecutionMode);
  }
}

BENCHMARK_DEFINE_F(StarSchemaBenchmark, Q3_4)(benchmark::State &state) {
  auto catalog = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;
  // Date
  sql::Table *d_table = catalog->LookupTableByName("ssbm.date");
  const auto &d_schema = d_table->GetSchema();
  // Customer
  sql::Table *c_table = catalog->LookupTableByName("ssbm.customer");
  const auto &c_schema = c_table->GetSchema();
  // Supplier
  sql::Table *s_table = catalog->LookupTableByName("ssbm.supplier");
  const auto &s_schema = s_table->GetSchema();
  // LineOrder
  sql::Table *lo_table = catalog->LookupTableByName("ssbm.lineorder");
  const auto &lo_schema = lo_table->GetSchema();

  // Scan date
  std::unique_ptr<planner::AbstractPlanNode> d_seq_scan;
  planner::OutputSchemaHelper d_seq_scan_out{&expr_maker, 0};
  {
    auto d_datekey = expr_maker.CVE(d_schema.GetColumnInfo("d_datekey"));
    auto d_year = expr_maker.CVE(d_schema.GetColumnInfo("d_year"));
    auto d_yearmonth = expr_maker.CVE(d_schema.GetColumnInfo("d_yearmonth"));
    // Make output schema.
    d_seq_scan_out.AddOutput("d_datekey", d_datekey);
    d_seq_scan_out.AddOutput("d_year", d_year);
    // Make predicate: d_yearmonth = 'Dec1997'
    auto predicate = expr_maker.CompareEq(d_yearmonth, expr_maker.Constant("Dec1997"));
    // Build plan node.
    d_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(d_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(d_table->GetId())
                     .Build();
  }

  // Scan customer
  std::unique_ptr<planner::AbstractPlanNode> c_seq_scan;
  planner::OutputSchemaHelper c_seq_scan_out{&expr_maker, 0};
  {
    auto c_custkey = expr_maker.CVE(c_schema.GetColumnInfo("c_custkey"));
    auto c_city = expr_maker.CVE(c_schema.GetColumnInfo("c_city"));
    // Make the predicate: c_city='UNITED KI1' or c_city='UNITED KI5'
    auto predicate =
        expr_maker.ConjunctionOr(expr_maker.CompareEq(c_city, expr_maker.Constant("UNITED KI1")),
                                 expr_maker.CompareEq(c_city, expr_maker.Constant("UNITED KI5")));
    // Make output schema.
    c_seq_scan_out.AddOutput("c_custkey", c_custkey);
    c_seq_scan_out.AddOutput("c_city", c_city);
    // Build plan node.
    c_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(c_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(c_table->GetId())
                     .Build();
  }

  // Scan supplier
  std::unique_ptr<planner::AbstractPlanNode> s_seq_scan;
  planner::OutputSchemaHelper s_seq_scan_out{&expr_maker, 0};
  {
    auto s_suppkey = expr_maker.CVE(s_schema.GetColumnInfo("s_suppkey"));
    auto s_city = expr_maker.CVE(s_schema.GetColumnInfo("s_city"));
    // Make the predicate: s_city='UNITED KI1' or s_city='UNITED KI5'
    auto predicate =
        expr_maker.ConjunctionOr(expr_maker.CompareEq(s_city, expr_maker.Constant("UNITED KI1")),
                                 expr_maker.CompareEq(s_city, expr_maker.Constant("UNITED KI5")));
    // Make output schema.
    s_seq_scan_out.AddOutput("s_suppkey", s_suppkey);
    s_seq_scan_out.AddOutput("s_city", s_city);
    // Build plan node.
    s_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(s_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(s_table->GetId())
                     .Build();
  }

  // Scan lineorder.
  std::unique_ptr<planner::AbstractPlanNode> lo_seq_scan;
  planner::OutputSchemaHelper lo_seq_scan_out{&expr_maker, 1};
  {
    auto lo_orderdate = expr_maker.CVE(lo_schema.GetColumnInfo("lo_orderdate"));
    auto lo_custkey = expr_maker.CVE(lo_schema.GetColumnInfo("lo_custkey"));
    auto lo_suppkey = expr_maker.CVE(lo_schema.GetColumnInfo("lo_suppkey"));
    auto lo_revenue = expr_maker.CVE(lo_schema.GetColumnInfo("lo_revenue"));
    // Make output schema.
    lo_seq_scan_out.AddOutput("lo_orderdate", lo_orderdate);
    lo_seq_scan_out.AddOutput("lo_custkey", lo_custkey);
    lo_seq_scan_out.AddOutput("lo_suppkey", lo_suppkey);
    lo_seq_scan_out.AddOutput("lo_revenue", lo_revenue);
    // Build plan node.
    planner::SeqScanPlanNode::Builder builder;
    lo_seq_scan = builder.SetOutputSchema(lo_seq_scan_out.MakeSchema())
                      .SetScanPredicate(nullptr)
                      .SetTableOid(lo_table->GetId())
                      .Build();
  }

  // customer <-> lineorder ==> HJ1
  std::unique_ptr<planner::AbstractPlanNode> hash_join1;
  planner::OutputSchemaHelper hash_join_out1{&expr_maker, 1};
  {
    // Left columns.
    auto c_custkey = c_seq_scan_out.GetOutput("c_custkey");
    auto c_city = c_seq_scan_out.GetOutput("c_city");
    // Right columns.
    auto lo_orderdate = lo_seq_scan_out.GetOutput("lo_orderdate");
    auto lo_custkey = lo_seq_scan_out.GetOutput("lo_custkey");
    auto lo_suppkey = lo_seq_scan_out.GetOutput("lo_suppkey");
    auto lo_revenue = lo_seq_scan_out.GetOutput("lo_revenue");
    // Output Schema
    hash_join_out1.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out1.AddOutput("lo_suppkey", lo_suppkey);
    hash_join_out1.AddOutput("lo_revenue", lo_revenue);
    hash_join_out1.AddOutput("c_city", c_city);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join1 = builder.AddChild(std::move(c_seq_scan))
                     .AddChild(std::move(lo_seq_scan))
                     .SetOutputSchema(hash_join_out1.MakeSchema())
                     .AddLeftHashKey(c_custkey)
                     .AddRightHashKey(lo_custkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.CompareEq(c_custkey, lo_custkey))
                     .Build();
  }

  // supplier <-> HJ1   ==> HJ2
  std::unique_ptr<planner::AbstractPlanNode> hash_join2;
  planner::OutputSchemaHelper hash_join_out2{&expr_maker, 1};
  {
    // Left columns.
    auto s_suppkey = s_seq_scan_out.GetOutput("s_suppkey");
    auto s_city = s_seq_scan_out.GetOutput("s_city");
    // Right columns.
    auto lo_orderdate = hash_join_out1.GetOutput("lo_orderdate");
    auto lo_suppkey = hash_join_out1.GetOutput("lo_suppkey");
    auto lo_revenue = hash_join_out1.GetOutput("lo_revenue");
    auto c_city = hash_join_out1.GetOutput("c_city");
    // Output Schema
    hash_join_out2.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out2.AddOutput("lo_revenue", lo_revenue);
    hash_join_out2.AddOutput("c_city", c_city);
    hash_join_out2.AddOutput("s_city", s_city);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join2 = builder.AddChild(std::move(s_seq_scan))
                     .AddChild(std::move(hash_join1))
                     .SetOutputSchema(hash_join_out2.MakeSchema())
                     .AddLeftHashKey(s_suppkey)
                     .AddRightHashKey(lo_suppkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.CompareEq(s_suppkey, lo_suppkey))
                     .Build();
  }

  // date <-> HJ2       ==> HJ3
  std::unique_ptr<planner::AbstractPlanNode> hash_join3;
  planner::OutputSchemaHelper hash_join_out3{&expr_maker, 0};
  {
    // Left columns.
    auto d_datekey = d_seq_scan_out.GetOutput("d_datekey");
    auto d_year = d_seq_scan_out.GetOutput("d_year");
    // Right columns.
    auto lo_orderdate = hash_join_out2.GetOutput("lo_orderdate");
    auto lo_revenue = hash_join_out2.GetOutput("lo_revenue");
    auto c_city = hash_join_out2.GetOutput("c_city");
    auto s_city = hash_join_out2.GetOutput("s_city");
    // Output Schema
    hash_join_out3.AddOutput("c_city", c_city);
    hash_join_out3.AddOutput("s_city", s_city);
    hash_join_out3.AddOutput("d_year", d_year);
    hash_join_out3.AddOutput("lo_revenue", lo_revenue);
    // Build.
    planner::HashJoinPlanNode::Builder builder;
    hash_join3 = builder.AddChild(std::move(d_seq_scan))
                     .AddChild(std::move(hash_join2))
                     .SetOutputSchema(hash_join_out3.MakeSchema())
                     .AddLeftHashKey(d_datekey)
                     .AddRightHashKey(lo_orderdate)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.CompareEq(d_datekey, lo_orderdate))
                     .Build();
  }

  // Aggregate.
  std::unique_ptr<planner::AbstractPlanNode> agg;
  planner::OutputSchemaHelper agg_out{&expr_maker, 0};
  {
    // Read previous layer's output
    auto c_city = hash_join_out3.GetOutput("c_city");
    auto s_city = hash_join_out3.GetOutput("s_city");
    auto d_year = hash_join_out3.GetOutput("d_year");
    auto lo_revenue = hash_join_out3.GetOutput("lo_revenue");
    auto revenue = expr_maker.AggSum(lo_revenue);
    // Add them to the helper.
    agg_out.AddGroupByTerm("c_city", c_city);
    agg_out.AddGroupByTerm("s_city", s_city);
    agg_out.AddGroupByTerm("d_year", d_year);
    agg_out.AddAggTerm("revenue", revenue);
    // Make the output schema.
    agg_out.AddOutput("c_city", agg_out.GetGroupByTermForOutput("c_city"));
    agg_out.AddOutput("s_city", agg_out.GetGroupByTermForOutput("s_city"));
    agg_out.AddOutput("d_year", agg_out.GetGroupByTermForOutput("d_year"));
    agg_out.AddOutput("revenue", agg_out.GetAggTermForOutput("revenue"));
    // Build plan node.
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(agg_out.MakeSchema())
              .AddAggregateTerm(revenue)
              .AddGroupByTerm(c_city)
              .AddGroupByTerm(s_city)
              .AddGroupByTerm(d_year)
              .AddChild(std::move(hash_join3))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }

  // Sort.
  std::unique_ptr<planner::AbstractPlanNode> sort;
  planner::OutputSchemaHelper sort_out{&expr_maker, 0};
  {
    // Read previous layer's output
    auto c_city = agg_out.GetOutput("c_city");
    auto s_city = agg_out.GetOutput("s_city");
    auto d_year = agg_out.GetOutput("d_year");
    auto revenue = agg_out.GetOutput("revenue");
    // Setup output.
    sort_out.AddOutput("c_city", c_city);
    sort_out.AddOutput("s_city", s_city);
    sort_out.AddOutput("d_year", d_year);
    sort_out.AddOutput("revenue", revenue);
    // Build.
    sort = planner::OrderByPlanNode::Builder{}
               .SetOutputSchema(sort_out.MakeSchema())
               .AddChild(std::move(agg))
               .AddSortKey(d_year, planner::OrderByOrderingType::ASC)
               .AddSortKey(revenue, planner::OrderByOrderingType::DESC)
               .Build();
  }

  // Compile plan
  auto query = CompilationContext::Compile(*sort);

  // Consumer.
  NoOpResultConsumer consumer;

  // Run Once to force compilation
  {
    sql::MemoryPool memory(nullptr);
    sql::ExecutionContext exec_ctx(&memory, sort->GetOutputSchema(), &consumer);
    query->Run(&exec_ctx, kExecutionMode);
  }

  // Only time execution.
  for (auto _ : state) {
    sql::MemoryPool memory(nullptr);
    sql::ExecutionContext exec_ctx(&memory, sort->GetOutputSchema(), &consumer);
    query->Run(&exec_ctx, kExecutionMode);
  }
}

BENCHMARK_DEFINE_F(StarSchemaBenchmark, Q4_1)(benchmark::State &state) {
  auto catalog = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;
  // Date
  sql::Table *d_table = catalog->LookupTableByName("ssbm.date");
  const auto &d_schema = d_table->GetSchema();
  // Part
  sql::Table *p_table = catalog->LookupTableByName("ssbm.part");
  const auto &p_schema = p_table->GetSchema();
  // Customer
  sql::Table *c_table = catalog->LookupTableByName("ssbm.customer");
  const auto &c_schema = c_table->GetSchema();
  // Supplier
  sql::Table *s_table = catalog->LookupTableByName("ssbm.supplier");
  const auto &s_schema = s_table->GetSchema();
  // LineOrder
  sql::Table *lo_table = catalog->LookupTableByName("ssbm.lineorder");
  const auto &lo_schema = lo_table->GetSchema();

  // Scan date
  std::unique_ptr<planner::AbstractPlanNode> d_seq_scan;
  planner::OutputSchemaHelper d_seq_scan_out{&expr_maker, 1};
  {
    auto d_datekey = expr_maker.CVE(d_schema.GetColumnInfo("d_datekey"));
    auto d_year = expr_maker.CVE(d_schema.GetColumnInfo("d_year"));
    // Make output schema.
    d_seq_scan_out.AddOutput("d_datekey", d_datekey);
    d_seq_scan_out.AddOutput("d_year", d_year);
    // Build plan node.
    d_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(d_seq_scan_out.MakeSchema())
                     .SetScanPredicate(nullptr)
                     .SetTableOid(d_table->GetId())
                     .Build();
  }

  // Scan customer
  std::unique_ptr<planner::AbstractPlanNode> c_seq_scan;
  planner::OutputSchemaHelper c_seq_scan_out{&expr_maker, 0};
  {
    auto c_custkey = expr_maker.CVE(c_schema.GetColumnInfo("c_custkey"));
    auto c_nation = expr_maker.CVE(c_schema.GetColumnInfo("c_nation"));
    auto c_region = expr_maker.CVE(c_schema.GetColumnInfo("c_region"));
    // Make the predicate: c_region = 'AMERICA'
    auto predicate = expr_maker.CompareEq(c_region, expr_maker.Constant("AMERICA"));
    // Make output schema.
    c_seq_scan_out.AddOutput("c_custkey", c_custkey);
    c_seq_scan_out.AddOutput("c_nation", c_nation);
    // Build plan node.
    c_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(c_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(c_table->GetId())
                     .Build();
  }

  // Scan supplier
  std::unique_ptr<planner::AbstractPlanNode> s_seq_scan;
  planner::OutputSchemaHelper s_seq_scan_out{&expr_maker, 0};
  {
    auto s_suppkey = expr_maker.CVE(s_schema.GetColumnInfo("s_suppkey"));
    auto s_region = expr_maker.CVE(s_schema.GetColumnInfo("s_region"));
    // Make the predicate: s_region = 'AMERICA'
    auto predicate = expr_maker.CompareEq(s_region, expr_maker.Constant("AMERICA"));
    // Make output schema.
    s_seq_scan_out.AddOutput("s_suppkey", s_suppkey);
    // Build plan node.
    s_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(s_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(s_table->GetId())
                     .Build();
  }

  // Scan part
  std::unique_ptr<planner::AbstractPlanNode> p_seq_scan;
  planner::OutputSchemaHelper p_seq_scan_out{&expr_maker, 0};
  {
    auto p_partkey = expr_maker.CVE(p_schema.GetColumnInfo("p_partkey"));
    auto p_mfgr = expr_maker.CVE(p_schema.GetColumnInfo("p_mfgr"));
    // Make the predicate: p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2'
    auto predicate =
        expr_maker.ConjunctionOr(expr_maker.CompareEq(p_mfgr, expr_maker.Constant("MFGR#1")),
                                 expr_maker.CompareEq(p_mfgr, expr_maker.Constant("MFGR#2")));
    // Make output schema.
    p_seq_scan_out.AddOutput("p_partkey", p_partkey);
    // Build plan node.
    p_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(p_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(p_table->GetId())
                     .Build();
  }

  // Scan lineorder.
  std::unique_ptr<planner::AbstractPlanNode> lo_seq_scan;
  planner::OutputSchemaHelper lo_seq_scan_out{&expr_maker, 1};
  {
    auto lo_orderdate = expr_maker.CVE(lo_schema.GetColumnInfo("lo_orderdate"));
    auto lo_custkey = expr_maker.CVE(lo_schema.GetColumnInfo("lo_custkey"));
    auto lo_suppkey = expr_maker.CVE(lo_schema.GetColumnInfo("lo_suppkey"));
    auto lo_partkey = expr_maker.CVE(lo_schema.GetColumnInfo("lo_partkey"));
    auto lo_revenue = expr_maker.CVE(lo_schema.GetColumnInfo("lo_revenue"));
    auto lo_supplycost = expr_maker.CVE(lo_schema.GetColumnInfo("lo_supplycost"));
    // Make output schema.
    lo_seq_scan_out.AddOutput("lo_orderdate", lo_orderdate);
    lo_seq_scan_out.AddOutput("lo_custkey", lo_custkey);
    lo_seq_scan_out.AddOutput("lo_suppkey", lo_suppkey);
    lo_seq_scan_out.AddOutput("lo_partkey", lo_partkey);
    lo_seq_scan_out.AddOutput("lo_revenue", lo_revenue);
    lo_seq_scan_out.AddOutput("lo_supplycost", lo_supplycost);
    // Build plan node.
    planner::SeqScanPlanNode::Builder builder;
    lo_seq_scan = builder.SetOutputSchema(lo_seq_scan_out.MakeSchema())
                      .SetScanPredicate(nullptr)
                      .SetTableOid(lo_table->GetId())
                      .Build();
  }

  // part <-> lineorder ==> HJ1
  std::unique_ptr<planner::AbstractPlanNode> hash_join1;
  planner::OutputSchemaHelper hash_join_out1{&expr_maker, 1};
  {
    // Left columns.
    auto p_partkey = p_seq_scan_out.GetOutput("p_partkey");
    // Right columns.
    auto lo_orderdate = lo_seq_scan_out.GetOutput("lo_orderdate");
    auto lo_custkey = lo_seq_scan_out.GetOutput("lo_custkey");
    auto lo_suppkey = lo_seq_scan_out.GetOutput("lo_suppkey");
    auto lo_partkey = lo_seq_scan_out.GetOutput("lo_partkey");
    auto lo_revenue = lo_seq_scan_out.GetOutput("lo_revenue");
    auto lo_supplycost = lo_seq_scan_out.GetOutput("lo_supplycost");
    // Output Schema
    hash_join_out1.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out1.AddOutput("lo_custkey", lo_custkey);
    hash_join_out1.AddOutput("lo_suppkey", lo_suppkey);
    hash_join_out1.AddOutput("lo_revenue", lo_revenue);
    hash_join_out1.AddOutput("lo_supplycost", lo_supplycost);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join1 = builder.AddChild(std::move(p_seq_scan))
                     .AddChild(std::move(lo_seq_scan))
                     .SetOutputSchema(hash_join_out1.MakeSchema())
                     .AddLeftHashKey(p_partkey)
                     .AddRightHashKey(lo_partkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.CompareEq(p_partkey, lo_partkey))
                     .Build();
  }

  // customer <-> HJ1   ==> HJ2
  std::unique_ptr<planner::AbstractPlanNode> hash_join2;
  planner::OutputSchemaHelper hash_join_out2{&expr_maker, 1};
  {
    // Left columns.
    auto c_custkey = c_seq_scan_out.GetOutput("c_custkey");
    auto c_nation = c_seq_scan_out.GetOutput("c_nation");
    // Right columns.
    auto lo_orderdate = hash_join_out1.GetOutput("lo_orderdate");
    auto lo_custkey = hash_join_out1.GetOutput("lo_custkey");
    auto lo_suppkey = hash_join_out1.GetOutput("lo_suppkey");
    auto lo_revenue = hash_join_out1.GetOutput("lo_revenue");
    auto lo_supplycost = hash_join_out1.GetOutput("lo_supplycost");
    // Output Schema
    hash_join_out2.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out2.AddOutput("lo_suppkey", lo_suppkey);
    hash_join_out2.AddOutput("lo_revenue", lo_revenue);
    hash_join_out2.AddOutput("lo_supplycost", lo_supplycost);
    hash_join_out2.AddOutput("c_nation", c_nation);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join2 = builder.AddChild(std::move(c_seq_scan))
                     .AddChild(std::move(hash_join1))
                     .SetOutputSchema(hash_join_out2.MakeSchema())
                     .AddLeftHashKey(c_custkey)
                     .AddRightHashKey(lo_custkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.CompareEq(c_custkey, lo_custkey))
                     .Build();
  }

  // supplier <-> HJ2       ==> HJ3
  std::unique_ptr<planner::AbstractPlanNode> hash_join3;
  planner::OutputSchemaHelper hash_join_out3{&expr_maker, 0};
  {
    // Left columns.
    auto s_suppkey = s_seq_scan_out.GetOutput("s_suppkey");
    // Right columns.
    auto lo_orderdate = hash_join_out2.GetOutput("lo_orderdate");
    auto lo_suppkey = hash_join_out2.GetOutput("lo_suppkey");
    auto lo_revenue = hash_join_out2.GetOutput("lo_revenue");
    auto lo_supplycost = hash_join_out2.GetOutput("lo_supplycost");
    auto c_nation = hash_join_out2.GetOutput("c_nation");
    // Output Schema
    hash_join_out3.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out3.AddOutput("lo_revenue", lo_revenue);
    hash_join_out3.AddOutput("lo_supplycost", lo_supplycost);
    hash_join_out3.AddOutput("c_nation", c_nation);
    // Build.
    planner::HashJoinPlanNode::Builder builder;
    hash_join3 = builder.AddChild(std::move(s_seq_scan))
                     .AddChild(std::move(hash_join2))
                     .SetOutputSchema(hash_join_out3.MakeSchema())
                     .AddLeftHashKey(s_suppkey)
                     .AddRightHashKey(lo_suppkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.CompareEq(s_suppkey, lo_suppkey))
                     .Build();
  }

  // HJ3 <-> date       ==> HJ4
  std::unique_ptr<planner::AbstractPlanNode> hash_join4;
  planner::OutputSchemaHelper hash_join_out4{&expr_maker, 0};
  {
    // Left columns.
    auto lo_orderdate = hash_join_out3.GetOutput("lo_orderdate");
    auto lo_revenue = hash_join_out3.GetOutput("lo_revenue");
    auto lo_supplycost = hash_join_out3.GetOutput("lo_supplycost");
    auto c_nation = hash_join_out3.GetOutput("c_nation");
    // Right columns.
    auto d_datekey = d_seq_scan_out.GetOutput("d_datekey");
    auto d_year = d_seq_scan_out.GetOutput("d_year");
    // Output Schema
    hash_join_out4.AddOutput("d_year", d_year);
    hash_join_out4.AddOutput("c_nation", c_nation);
    hash_join_out4.AddOutput("lo_revenue", lo_revenue);
    hash_join_out4.AddOutput("lo_supplycost", lo_supplycost);
    // Build.
    planner::HashJoinPlanNode::Builder builder;
    hash_join4 = builder.AddChild(std::move(hash_join3))
                     .AddChild(std::move(d_seq_scan))
                     .SetOutputSchema(hash_join_out4.MakeSchema())
                     .AddLeftHashKey(lo_orderdate)
                     .AddRightHashKey(d_datekey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.CompareEq(lo_orderdate, d_datekey))
                     .Build();
  }

  // Aggregate.
  std::unique_ptr<planner::AbstractPlanNode> agg;
  planner::OutputSchemaHelper agg_out{&expr_maker, 0};
  {
    // Read previous layer's output
    auto d_year = hash_join_out4.GetOutput("d_year");
    auto c_nation = hash_join_out4.GetOutput("c_nation");
    auto lo_revenue = hash_join_out4.GetOutput("lo_revenue");
    auto lo_supplycost = hash_join_out4.GetOutput("lo_supplycost");
    auto profit = expr_maker.AggSum(expr_maker.OpMin(lo_revenue, lo_supplycost));
    // Add them to the helper.
    agg_out.AddGroupByTerm("d_year", d_year);
    agg_out.AddGroupByTerm("c_nation", c_nation);
    agg_out.AddAggTerm("profit", profit);
    // Make the output schema.
    agg_out.AddOutput("d_year", agg_out.GetGroupByTermForOutput("d_year"));
    agg_out.AddOutput("c_nation", agg_out.GetGroupByTermForOutput("c_nation"));
    agg_out.AddOutput("profit", agg_out.GetAggTermForOutput("profit"));
    // Build plan node.
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(agg_out.MakeSchema())
              .AddGroupByTerm(d_year)
              .AddGroupByTerm(c_nation)
              .AddAggregateTerm(profit)
              .AddChild(std::move(hash_join4))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }

  // Sort.
  std::unique_ptr<planner::AbstractPlanNode> sort;
  planner::OutputSchemaHelper sort_out{&expr_maker, 0};
  {
    // Read previous layer's output
    auto d_year = agg_out.GetOutput("d_year");
    auto c_nation = agg_out.GetOutput("c_nation");
    auto profit = agg_out.GetOutput("profit");
    // Setup output.
    sort_out.AddOutput("d_year", d_year);
    sort_out.AddOutput("c_nation", c_nation);
    sort_out.AddOutput("profit", profit);
    // Build.
    sort = planner::OrderByPlanNode::Builder{}
               .SetOutputSchema(sort_out.MakeSchema())
               .AddChild(std::move(agg))
               .AddSortKey(d_year, planner::OrderByOrderingType::ASC)
               .AddSortKey(c_nation, planner::OrderByOrderingType::ASC)
               .Build();
  }

  // Compile plan
  auto query = CompilationContext::Compile(*sort);

  // Consumer.
  NoOpResultConsumer consumer;

  // Run Once to force compilation
  {
    sql::MemoryPool memory(nullptr);
    sql::ExecutionContext exec_ctx(&memory, sort->GetOutputSchema(), &consumer);
    query->Run(&exec_ctx, kExecutionMode);
  }

  // Only time execution.
  for (auto _ : state) {
    sql::MemoryPool memory(nullptr);
    sql::ExecutionContext exec_ctx(&memory, sort->GetOutputSchema(), &consumer);
    query->Run(&exec_ctx, kExecutionMode);
  }
}

BENCHMARK_DEFINE_F(StarSchemaBenchmark, Q4_2)(benchmark::State &state) {
  auto catalog = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;
  // Date
  sql::Table *d_table = catalog->LookupTableByName("ssbm.date");
  const auto &d_schema = d_table->GetSchema();
  // Part
  sql::Table *p_table = catalog->LookupTableByName("ssbm.part");
  const auto &p_schema = p_table->GetSchema();
  // Customer
  sql::Table *c_table = catalog->LookupTableByName("ssbm.customer");
  const auto &c_schema = c_table->GetSchema();
  // Supplier
  sql::Table *s_table = catalog->LookupTableByName("ssbm.supplier");
  const auto &s_schema = s_table->GetSchema();
  // LineOrder
  sql::Table *lo_table = catalog->LookupTableByName("ssbm.lineorder");
  const auto &lo_schema = lo_table->GetSchema();

  // Scan date
  std::unique_ptr<planner::AbstractPlanNode> d_seq_scan;
  planner::OutputSchemaHelper d_seq_scan_out{&expr_maker, 1};
  {
    auto d_datekey = expr_maker.CVE(d_schema.GetColumnInfo("d_datekey"));
    auto d_year = expr_maker.CVE(d_schema.GetColumnInfo("d_year"));
    // Make predicate: d_year = 1997 or d_year = 1998
    auto predicate =
        expr_maker.ConjunctionOr(expr_maker.CompareEq(d_year, expr_maker.Constant(1997)),
                                 expr_maker.CompareEq(d_year, expr_maker.Constant(1998)));
    // Make output schema.
    d_seq_scan_out.AddOutput("d_datekey", d_datekey);
    d_seq_scan_out.AddOutput("d_year", d_year);
    // Build plan node.
    d_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(d_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(d_table->GetId())
                     .Build();
  }

  // Scan customer
  std::unique_ptr<planner::AbstractPlanNode> c_seq_scan;
  planner::OutputSchemaHelper c_seq_scan_out{&expr_maker, 0};
  {
    auto c_custkey = expr_maker.CVE(c_schema.GetColumnInfo("c_custkey"));
    auto c_region = expr_maker.CVE(c_schema.GetColumnInfo("c_region"));
    // Make the predicate: c_region = 'AMERICA'
    auto predicate = expr_maker.CompareEq(c_region, expr_maker.Constant("AMERICA"));
    // Make output schema.
    c_seq_scan_out.AddOutput("c_custkey", c_custkey);
    // Build plan node.
    c_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(c_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(c_table->GetId())
                     .Build();
  }

  // Scan supplier
  std::unique_ptr<planner::AbstractPlanNode> s_seq_scan;
  planner::OutputSchemaHelper s_seq_scan_out{&expr_maker, 0};
  {
    auto s_suppkey = expr_maker.CVE(s_schema.GetColumnInfo("s_suppkey"));
    auto s_region = expr_maker.CVE(s_schema.GetColumnInfo("s_region"));
    auto s_nation = expr_maker.CVE(s_schema.GetColumnInfo("s_nation"));
    // Make the predicate: s_region = 'AMERICA'
    auto predicate = expr_maker.CompareEq(s_region, expr_maker.Constant("AMERICA"));
    // Make output schema.
    s_seq_scan_out.AddOutput("s_suppkey", s_suppkey);
    s_seq_scan_out.AddOutput("s_nation", s_nation);
    // Build plan node.
    s_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(s_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(s_table->GetId())
                     .Build();
  }

  // Scan part
  std::unique_ptr<planner::AbstractPlanNode> p_seq_scan;
  planner::OutputSchemaHelper p_seq_scan_out{&expr_maker, 0};
  {
    auto p_partkey = expr_maker.CVE(p_schema.GetColumnInfo("p_partkey"));
    auto p_mfgr = expr_maker.CVE(p_schema.GetColumnInfo("p_mfgr"));
    auto p_category = expr_maker.CVE(p_schema.GetColumnInfo("p_category"));
    // Make the predicate: p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2'
    auto predicate =
        expr_maker.ConjunctionOr(expr_maker.CompareEq(p_mfgr, expr_maker.Constant("MFGR#1")),
                                 expr_maker.CompareEq(p_mfgr, expr_maker.Constant("MFGR#2")));
    // Make output schema.
    p_seq_scan_out.AddOutput("p_partkey", p_partkey);
    p_seq_scan_out.AddOutput("p_category", p_category);
    // Build plan node.
    p_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(p_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(p_table->GetId())
                     .Build();
  }

  // Scan lineorder.
  std::unique_ptr<planner::AbstractPlanNode> lo_seq_scan;
  planner::OutputSchemaHelper lo_seq_scan_out{&expr_maker, 1};
  {
    auto lo_orderdate = expr_maker.CVE(lo_schema.GetColumnInfo("lo_orderdate"));
    auto lo_custkey = expr_maker.CVE(lo_schema.GetColumnInfo("lo_custkey"));
    auto lo_suppkey = expr_maker.CVE(lo_schema.GetColumnInfo("lo_suppkey"));
    auto lo_partkey = expr_maker.CVE(lo_schema.GetColumnInfo("lo_partkey"));
    auto lo_revenue = expr_maker.CVE(lo_schema.GetColumnInfo("lo_revenue"));
    auto lo_supplycost = expr_maker.CVE(lo_schema.GetColumnInfo("lo_supplycost"));
    // Make output schema.
    lo_seq_scan_out.AddOutput("lo_orderdate", lo_orderdate);
    lo_seq_scan_out.AddOutput("lo_custkey", lo_custkey);
    lo_seq_scan_out.AddOutput("lo_suppkey", lo_suppkey);
    lo_seq_scan_out.AddOutput("lo_partkey", lo_partkey);
    lo_seq_scan_out.AddOutput("lo_revenue", lo_revenue);
    lo_seq_scan_out.AddOutput("lo_supplycost", lo_supplycost);
    // Build plan node.
    planner::SeqScanPlanNode::Builder builder;
    lo_seq_scan = builder.SetOutputSchema(lo_seq_scan_out.MakeSchema())
                      .SetScanPredicate(nullptr)
                      .SetTableOid(lo_table->GetId())
                      .Build();
  }

  // part <-> lineorder ==> HJ1
  std::unique_ptr<planner::AbstractPlanNode> hash_join1;
  planner::OutputSchemaHelper hash_join_out1{&expr_maker, 1};
  {
    // Left columns.
    auto p_partkey = p_seq_scan_out.GetOutput("p_partkey");
    auto p_category = p_seq_scan_out.GetOutput("p_category");
    // Right columns.
    auto lo_orderdate = lo_seq_scan_out.GetOutput("lo_orderdate");
    auto lo_custkey = lo_seq_scan_out.GetOutput("lo_custkey");
    auto lo_suppkey = lo_seq_scan_out.GetOutput("lo_suppkey");
    auto lo_partkey = lo_seq_scan_out.GetOutput("lo_partkey");
    auto lo_revenue = lo_seq_scan_out.GetOutput("lo_revenue");
    auto lo_supplycost = lo_seq_scan_out.GetOutput("lo_supplycost");
    // Output Schema
    hash_join_out1.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out1.AddOutput("lo_custkey", lo_custkey);
    hash_join_out1.AddOutput("lo_suppkey", lo_suppkey);
    hash_join_out1.AddOutput("p_category", p_category);
    hash_join_out1.AddOutput("lo_revenue", lo_revenue);
    hash_join_out1.AddOutput("lo_supplycost", lo_supplycost);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join1 = builder.AddChild(std::move(p_seq_scan))
                     .AddChild(std::move(lo_seq_scan))
                     .SetOutputSchema(hash_join_out1.MakeSchema())
                     .AddLeftHashKey(p_partkey)
                     .AddRightHashKey(lo_partkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.CompareEq(p_partkey, lo_partkey))
                     .Build();
  }

  // customer <-> HJ1   ==> HJ2
  std::unique_ptr<planner::AbstractPlanNode> hash_join2;
  planner::OutputSchemaHelper hash_join_out2{&expr_maker, 1};
  {
    // Left columns.
    auto c_custkey = c_seq_scan_out.GetOutput("c_custkey");
    // Right columns.
    auto lo_orderdate = hash_join_out1.GetOutput("lo_orderdate");
    auto lo_custkey = hash_join_out1.GetOutput("lo_custkey");
    auto lo_suppkey = hash_join_out1.GetOutput("lo_suppkey");
    auto lo_revenue = hash_join_out1.GetOutput("lo_revenue");
    auto lo_supplycost = hash_join_out1.GetOutput("lo_supplycost");
    auto p_category = hash_join_out1.GetOutput("p_category");
    // Output Schema
    hash_join_out2.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out2.AddOutput("lo_suppkey", lo_suppkey);
    hash_join_out2.AddOutput("p_category", p_category);
    hash_join_out2.AddOutput("lo_revenue", lo_revenue);
    hash_join_out2.AddOutput("lo_supplycost", lo_supplycost);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join2 = builder.AddChild(std::move(c_seq_scan))
                     .AddChild(std::move(hash_join1))
                     .SetOutputSchema(hash_join_out2.MakeSchema())
                     .AddLeftHashKey(c_custkey)
                     .AddRightHashKey(lo_custkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.CompareEq(c_custkey, lo_custkey))
                     .Build();
  }

  // supplier <-> HJ2       ==> HJ3
  std::unique_ptr<planner::AbstractPlanNode> hash_join3;
  planner::OutputSchemaHelper hash_join_out3{&expr_maker, 0};
  {
    // Left columns.
    auto s_suppkey = s_seq_scan_out.GetOutput("s_suppkey");
    auto s_nation = s_seq_scan_out.GetOutput("s_nation");
    // Right columns.
    auto lo_orderdate = hash_join_out2.GetOutput("lo_orderdate");
    auto lo_suppkey = hash_join_out2.GetOutput("lo_suppkey");
    auto lo_revenue = hash_join_out2.GetOutput("lo_revenue");
    auto lo_supplycost = hash_join_out2.GetOutput("lo_supplycost");
    auto p_category = hash_join_out2.GetOutput("p_category");
    // Output Schema
    hash_join_out3.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out3.AddOutput("s_nation", s_nation);
    hash_join_out3.AddOutput("p_category", p_category);
    hash_join_out3.AddOutput("lo_revenue", lo_revenue);
    hash_join_out3.AddOutput("lo_supplycost", lo_supplycost);
    // Build.
    planner::HashJoinPlanNode::Builder builder;
    hash_join3 = builder.AddChild(std::move(s_seq_scan))
                     .AddChild(std::move(hash_join2))
                     .SetOutputSchema(hash_join_out3.MakeSchema())
                     .AddLeftHashKey(s_suppkey)
                     .AddRightHashKey(lo_suppkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.CompareEq(s_suppkey, lo_suppkey))
                     .Build();
  }

  // HJ3 <-> date       ==> HJ4
  std::unique_ptr<planner::AbstractPlanNode> hash_join4;
  planner::OutputSchemaHelper hash_join_out4{&expr_maker, 0};
  {
    // Left columns.
    auto lo_orderdate = hash_join_out3.GetOutput("lo_orderdate");
    auto lo_revenue = hash_join_out3.GetOutput("lo_revenue");
    auto lo_supplycost = hash_join_out3.GetOutput("lo_supplycost");
    auto s_nation = hash_join_out3.GetOutput("s_nation");
    auto p_category = hash_join_out3.GetOutput("p_category");
    // Right columns.
    auto d_datekey = d_seq_scan_out.GetOutput("d_datekey");
    auto d_year = d_seq_scan_out.GetOutput("d_year");
    // Output Schema
    hash_join_out4.AddOutput("d_year", d_year);
    hash_join_out4.AddOutput("s_nation", s_nation);
    hash_join_out4.AddOutput("p_category", p_category);
    hash_join_out4.AddOutput("lo_revenue", lo_revenue);
    hash_join_out4.AddOutput("lo_supplycost", lo_supplycost);
    // Build.
    planner::HashJoinPlanNode::Builder builder;
    hash_join4 = builder.AddChild(std::move(hash_join3))
                     .AddChild(std::move(d_seq_scan))
                     .SetOutputSchema(hash_join_out4.MakeSchema())
                     .AddLeftHashKey(lo_orderdate)
                     .AddRightHashKey(d_datekey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.CompareEq(lo_orderdate, d_datekey))
                     .Build();
  }

  // Aggregate.
  std::unique_ptr<planner::AbstractPlanNode> agg;
  planner::OutputSchemaHelper agg_out{&expr_maker, 0};
  {
    // Read previous layer's output
    auto d_year = hash_join_out4.GetOutput("d_year");
    auto s_nation = hash_join_out4.GetOutput("s_nation");
    auto p_category = hash_join_out4.GetOutput("p_category");
    auto lo_revenue = hash_join_out4.GetOutput("lo_revenue");
    auto lo_supplycost = hash_join_out4.GetOutput("lo_supplycost");
    auto profit = expr_maker.AggSum(expr_maker.OpMin(lo_revenue, lo_supplycost));
    // Add them to the helper.
    agg_out.AddGroupByTerm("d_year", d_year);
    agg_out.AddGroupByTerm("s_nation", s_nation);
    agg_out.AddGroupByTerm("p_category", p_category);
    agg_out.AddAggTerm("profit", profit);
    // Make the output schema.
    agg_out.AddOutput("d_year", agg_out.GetGroupByTermForOutput("d_year"));
    agg_out.AddOutput("s_nation", agg_out.GetGroupByTermForOutput("s_nation"));
    agg_out.AddOutput("p_category", agg_out.GetGroupByTermForOutput("p_category"));
    agg_out.AddOutput("profit", agg_out.GetAggTermForOutput("profit"));
    // Build plan node.
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(agg_out.MakeSchema())
              .AddGroupByTerm(d_year)
              .AddGroupByTerm(s_nation)
              .AddGroupByTerm(p_category)
              .AddAggregateTerm(profit)
              .AddChild(std::move(hash_join4))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }

  // Sort.
  std::unique_ptr<planner::AbstractPlanNode> sort;
  planner::OutputSchemaHelper sort_out{&expr_maker, 0};
  {
    // Read previous layer's output
    auto d_year = agg_out.GetOutput("d_year");
    auto s_nation = agg_out.GetOutput("s_nation");
    auto p_category = agg_out.GetOutput("p_category");
    auto profit = agg_out.GetOutput("profit");
    // Setup output.
    sort_out.AddOutput("d_year", d_year);
    sort_out.AddOutput("s_nation", s_nation);
    sort_out.AddOutput("p_category", p_category);
    sort_out.AddOutput("profit", profit);
    // Build.
    sort = planner::OrderByPlanNode::Builder{}
               .SetOutputSchema(sort_out.MakeSchema())
               .AddChild(std::move(agg))
               .AddSortKey(d_year, planner::OrderByOrderingType::ASC)
               .AddSortKey(s_nation, planner::OrderByOrderingType::ASC)
               .AddSortKey(p_category, planner::OrderByOrderingType::ASC)
               .Build();
  }

  // Compile plan
  auto query = CompilationContext::Compile(*sort);

  // Consumer.
  NoOpResultConsumer consumer;

  // Run Once to force compilation
  {
    sql::MemoryPool memory(nullptr);
    sql::ExecutionContext exec_ctx(&memory, sort->GetOutputSchema(), &consumer);
    query->Run(&exec_ctx, kExecutionMode);
  }

  // Only time execution.
  for (auto _ : state) {
    sql::MemoryPool memory(nullptr);
    sql::ExecutionContext exec_ctx(&memory, sort->GetOutputSchema(), &consumer);
    query->Run(&exec_ctx, kExecutionMode);
  }
}

BENCHMARK_DEFINE_F(StarSchemaBenchmark, Q4_3)(benchmark::State &state) {
  auto catalog = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;
  // Date
  sql::Table *d_table = catalog->LookupTableByName("ssbm.date");
  const auto &d_schema = d_table->GetSchema();
  // Part
  sql::Table *p_table = catalog->LookupTableByName("ssbm.part");
  const auto &p_schema = p_table->GetSchema();
  // Customer
  sql::Table *c_table = catalog->LookupTableByName("ssbm.customer");
  const auto &c_schema = c_table->GetSchema();
  // Supplier
  sql::Table *s_table = catalog->LookupTableByName("ssbm.supplier");
  const auto &s_schema = s_table->GetSchema();
  // LineOrder
  sql::Table *lo_table = catalog->LookupTableByName("ssbm.lineorder");
  const auto &lo_schema = lo_table->GetSchema();

  // Scan date
  std::unique_ptr<planner::AbstractPlanNode> d_seq_scan;
  planner::OutputSchemaHelper d_seq_scan_out{&expr_maker, 1};
  {
    auto d_datekey = expr_maker.CVE(d_schema.GetColumnInfo("d_datekey"));
    auto d_year = expr_maker.CVE(d_schema.GetColumnInfo("d_year"));
    // Make predicate: d_year = 1997 or d_year = 1998
    auto predicate =
        expr_maker.ConjunctionOr(expr_maker.CompareEq(d_year, expr_maker.Constant(1997)),
                                 expr_maker.CompareEq(d_year, expr_maker.Constant(1998)));
    // Make output schema.
    d_seq_scan_out.AddOutput("d_datekey", d_datekey);
    d_seq_scan_out.AddOutput("d_year", d_year);
    // Build plan node.
    d_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(d_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(d_table->GetId())
                     .Build();
  }

  // Scan customer
  std::unique_ptr<planner::AbstractPlanNode> c_seq_scan;
  planner::OutputSchemaHelper c_seq_scan_out{&expr_maker, 0};
  {
    auto c_custkey = expr_maker.CVE(c_schema.GetColumnInfo("c_custkey"));
    auto c_region = expr_maker.CVE(c_schema.GetColumnInfo("c_region"));
    // Make the predicate: c_region = 'AMERICA'
    auto predicate = expr_maker.CompareEq(c_region, expr_maker.Constant("AMERICA"));
    // Make output schema.
    c_seq_scan_out.AddOutput("c_custkey", c_custkey);
    // Build plan node.
    c_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(c_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(c_table->GetId())
                     .Build();
  }

  // Scan supplier
  std::unique_ptr<planner::AbstractPlanNode> s_seq_scan;
  planner::OutputSchemaHelper s_seq_scan_out{&expr_maker, 0};
  {
    auto s_suppkey = expr_maker.CVE(s_schema.GetColumnInfo("s_suppkey"));
    auto s_city = expr_maker.CVE(s_schema.GetColumnInfo("s_city"));
    auto s_nation = expr_maker.CVE(s_schema.GetColumnInfo("s_nation"));
    // Make the predicate: s_nation = 'UNITED STATES'
    auto predicate = expr_maker.CompareEq(s_nation, expr_maker.Constant("UNITED STATES"));
    // Make output schema.
    s_seq_scan_out.AddOutput("s_suppkey", s_suppkey);
    s_seq_scan_out.AddOutput("s_city", s_city);
    // Build plan node.
    s_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(s_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(s_table->GetId())
                     .Build();
  }

  // Scan part
  std::unique_ptr<planner::AbstractPlanNode> p_seq_scan;
  planner::OutputSchemaHelper p_seq_scan_out{&expr_maker, 0};
  {
    auto p_partkey = expr_maker.CVE(p_schema.GetColumnInfo("p_partkey"));
    auto p_brand1 = expr_maker.CVE(p_schema.GetColumnInfo("p_brand1"));
    auto p_category = expr_maker.CVE(p_schema.GetColumnInfo("p_category"));
    // Make the predicate: p_category = 'MFGR#14'
    auto predicate = expr_maker.CompareEq(p_category, expr_maker.Constant("MFGR#14"));
    // Make output schema.
    p_seq_scan_out.AddOutput("p_partkey", p_partkey);
    p_seq_scan_out.AddOutput("p_brand1", p_brand1);
    // Build plan node.
    p_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(p_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(p_table->GetId())
                     .Build();
  }

  // Scan lineorder.
  std::unique_ptr<planner::AbstractPlanNode> lo_seq_scan;
  planner::OutputSchemaHelper lo_seq_scan_out{&expr_maker, 1};
  {
    auto lo_orderdate = expr_maker.CVE(lo_schema.GetColumnInfo("lo_orderdate"));
    auto lo_custkey = expr_maker.CVE(lo_schema.GetColumnInfo("lo_custkey"));
    auto lo_suppkey = expr_maker.CVE(lo_schema.GetColumnInfo("lo_suppkey"));
    auto lo_partkey = expr_maker.CVE(lo_schema.GetColumnInfo("lo_partkey"));
    auto lo_revenue = expr_maker.CVE(lo_schema.GetColumnInfo("lo_revenue"));
    auto lo_supplycost = expr_maker.CVE(lo_schema.GetColumnInfo("lo_supplycost"));
    // Make output schema.
    lo_seq_scan_out.AddOutput("lo_orderdate", lo_orderdate);
    lo_seq_scan_out.AddOutput("lo_custkey", lo_custkey);
    lo_seq_scan_out.AddOutput("lo_suppkey", lo_suppkey);
    lo_seq_scan_out.AddOutput("lo_partkey", lo_partkey);
    lo_seq_scan_out.AddOutput("lo_revenue", lo_revenue);
    lo_seq_scan_out.AddOutput("lo_supplycost", lo_supplycost);
    // Build plan node.
    planner::SeqScanPlanNode::Builder builder;
    lo_seq_scan = builder.SetOutputSchema(lo_seq_scan_out.MakeSchema())
                      .SetScanPredicate(nullptr)
                      .SetTableOid(lo_table->GetId())
                      .Build();
  }

  // part <-> lineorder ==> HJ1
  std::unique_ptr<planner::AbstractPlanNode> hash_join1;
  planner::OutputSchemaHelper hash_join_out1{&expr_maker, 1};
  {
    // Left columns.
    auto p_partkey = p_seq_scan_out.GetOutput("p_partkey");
    auto p_brand1 = p_seq_scan_out.GetOutput("p_brand1");
    // Right columns.
    auto lo_orderdate = lo_seq_scan_out.GetOutput("lo_orderdate");
    auto lo_custkey = lo_seq_scan_out.GetOutput("lo_custkey");
    auto lo_suppkey = lo_seq_scan_out.GetOutput("lo_suppkey");
    auto lo_partkey = lo_seq_scan_out.GetOutput("lo_partkey");
    auto lo_revenue = lo_seq_scan_out.GetOutput("lo_revenue");
    auto lo_supplycost = lo_seq_scan_out.GetOutput("lo_supplycost");
    // Output Schema
    hash_join_out1.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out1.AddOutput("lo_custkey", lo_custkey);
    hash_join_out1.AddOutput("lo_suppkey", lo_suppkey);
    hash_join_out1.AddOutput("p_brand1", p_brand1);
    hash_join_out1.AddOutput("lo_revenue", lo_revenue);
    hash_join_out1.AddOutput("lo_supplycost", lo_supplycost);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join1 = builder.AddChild(std::move(p_seq_scan))
                     .AddChild(std::move(lo_seq_scan))
                     .SetOutputSchema(hash_join_out1.MakeSchema())
                     .AddLeftHashKey(p_partkey)
                     .AddRightHashKey(lo_partkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.CompareEq(p_partkey, lo_partkey))
                     .Build();
  }

  // customer <-> HJ1   ==> HJ2
  std::unique_ptr<planner::AbstractPlanNode> hash_join2;
  planner::OutputSchemaHelper hash_join_out2{&expr_maker, 1};
  {
    // Left columns.
    auto c_custkey = c_seq_scan_out.GetOutput("c_custkey");
    // Right columns.
    auto lo_orderdate = hash_join_out1.GetOutput("lo_orderdate");
    auto lo_custkey = hash_join_out1.GetOutput("lo_custkey");
    auto lo_suppkey = hash_join_out1.GetOutput("lo_suppkey");
    auto lo_revenue = hash_join_out1.GetOutput("lo_revenue");
    auto lo_supplycost = hash_join_out1.GetOutput("lo_supplycost");
    auto p_brand1 = hash_join_out1.GetOutput("p_brand1");
    // Output Schema
    hash_join_out2.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out2.AddOutput("lo_suppkey", lo_suppkey);
    hash_join_out2.AddOutput("p_brand1", p_brand1);
    hash_join_out2.AddOutput("lo_revenue", lo_revenue);
    hash_join_out2.AddOutput("lo_supplycost", lo_supplycost);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join2 = builder.AddChild(std::move(c_seq_scan))
                     .AddChild(std::move(hash_join1))
                     .SetOutputSchema(hash_join_out2.MakeSchema())
                     .AddLeftHashKey(c_custkey)
                     .AddRightHashKey(lo_custkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.CompareEq(c_custkey, lo_custkey))
                     .Build();
  }

  // supplier <-> HJ2       ==> HJ3
  std::unique_ptr<planner::AbstractPlanNode> hash_join3;
  planner::OutputSchemaHelper hash_join_out3{&expr_maker, 0};
  {
    // Left columns.
    auto s_suppkey = s_seq_scan_out.GetOutput("s_suppkey");
    auto s_city = s_seq_scan_out.GetOutput("s_city");
    // Right columns.
    auto lo_orderdate = hash_join_out2.GetOutput("lo_orderdate");
    auto lo_suppkey = hash_join_out2.GetOutput("lo_suppkey");
    auto lo_revenue = hash_join_out2.GetOutput("lo_revenue");
    auto lo_supplycost = hash_join_out2.GetOutput("lo_supplycost");
    auto p_brand1 = hash_join_out2.GetOutput("p_brand1");
    // Output Schema
    hash_join_out3.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out3.AddOutput("s_city", s_city);
    hash_join_out3.AddOutput("p_brand1", p_brand1);
    hash_join_out3.AddOutput("lo_revenue", lo_revenue);
    hash_join_out3.AddOutput("lo_supplycost", lo_supplycost);
    // Build.
    planner::HashJoinPlanNode::Builder builder;
    hash_join3 = builder.AddChild(std::move(s_seq_scan))
                     .AddChild(std::move(hash_join2))
                     .SetOutputSchema(hash_join_out3.MakeSchema())
                     .AddLeftHashKey(s_suppkey)
                     .AddRightHashKey(lo_suppkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.CompareEq(s_suppkey, lo_suppkey))
                     .Build();
  }

  // HJ3 <-> date       ==> HJ4
  std::unique_ptr<planner::AbstractPlanNode> hash_join4;
  planner::OutputSchemaHelper hash_join_out4{&expr_maker, 0};
  {
    // Left columns.
    auto lo_orderdate = hash_join_out3.GetOutput("lo_orderdate");
    auto lo_revenue = hash_join_out3.GetOutput("lo_revenue");
    auto lo_supplycost = hash_join_out3.GetOutput("lo_supplycost");
    auto s_city = hash_join_out3.GetOutput("s_city");
    auto p_brand1 = hash_join_out3.GetOutput("p_brand1");
    // Right columns.
    auto d_datekey = d_seq_scan_out.GetOutput("d_datekey");
    auto d_year = d_seq_scan_out.GetOutput("d_year");
    // Output Schema
    hash_join_out4.AddOutput("d_year", d_year);
    hash_join_out4.AddOutput("s_city", s_city);
    hash_join_out4.AddOutput("p_brand1", p_brand1);
    hash_join_out4.AddOutput("lo_revenue", lo_revenue);
    hash_join_out4.AddOutput("lo_supplycost", lo_supplycost);
    // Build.
    planner::HashJoinPlanNode::Builder builder;
    hash_join4 = builder.AddChild(std::move(hash_join3))
                     .AddChild(std::move(d_seq_scan))
                     .SetOutputSchema(hash_join_out4.MakeSchema())
                     .AddLeftHashKey(lo_orderdate)
                     .AddRightHashKey(d_datekey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.CompareEq(lo_orderdate, d_datekey))
                     .Build();
  }

  // Aggregate.
  std::unique_ptr<planner::AbstractPlanNode> agg;
  planner::OutputSchemaHelper agg_out{&expr_maker, 0};
  {
    // Read previous layer's output
    auto d_year = hash_join_out4.GetOutput("d_year");
    auto s_city = hash_join_out4.GetOutput("s_city");
    auto p_brand1 = hash_join_out4.GetOutput("p_brand1");
    auto lo_revenue = hash_join_out4.GetOutput("lo_revenue");
    auto lo_supplycost = hash_join_out4.GetOutput("lo_supplycost");
    auto profit = expr_maker.AggSum(expr_maker.OpMin(lo_revenue, lo_supplycost));
    // Add them to the helper.
    agg_out.AddGroupByTerm("d_year", d_year);
    agg_out.AddGroupByTerm("s_city", s_city);
    agg_out.AddGroupByTerm("p_brand1", p_brand1);
    agg_out.AddAggTerm("profit", profit);
    // Make the output schema.
    agg_out.AddOutput("d_year", agg_out.GetGroupByTermForOutput("d_year"));
    agg_out.AddOutput("s_city", agg_out.GetGroupByTermForOutput("s_city"));
    agg_out.AddOutput("p_brand1", agg_out.GetGroupByTermForOutput("p_brand1"));
    agg_out.AddOutput("profit", agg_out.GetAggTermForOutput("profit"));
    // Build plan node.
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(agg_out.MakeSchema())
              .AddGroupByTerm(d_year)
              .AddGroupByTerm(s_city)
              .AddGroupByTerm(p_brand1)
              .AddAggregateTerm(profit)
              .AddChild(std::move(hash_join4))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }

  // Sort.
  std::unique_ptr<planner::AbstractPlanNode> sort;
  planner::OutputSchemaHelper sort_out{&expr_maker, 0};
  {
    // Read previous layer's output
    auto d_year = agg_out.GetOutput("d_year");
    auto s_city = agg_out.GetOutput("s_city");
    auto p_brand1 = agg_out.GetOutput("p_brand1");
    auto profit = agg_out.GetOutput("profit");
    // Setup output.
    sort_out.AddOutput("d_year", d_year);
    sort_out.AddOutput("s_city", s_city);
    sort_out.AddOutput("p_brand1", p_brand1);
    sort_out.AddOutput("profit", profit);
    // Build.
    sort = planner::OrderByPlanNode::Builder{}
               .SetOutputSchema(sort_out.MakeSchema())
               .AddChild(std::move(agg))
               .AddSortKey(d_year, planner::OrderByOrderingType::ASC)
               .AddSortKey(s_city, planner::OrderByOrderingType::ASC)
               .AddSortKey(p_brand1, planner::OrderByOrderingType::ASC)
               .Build();
  }

  // Compile plan
  auto query = CompilationContext::Compile(*sort);

  // Consumer.
  NoOpResultConsumer consumer;

  // Run Once to force compilation
  {
    sql::MemoryPool memory(nullptr);
    sql::ExecutionContext exec_ctx(&memory, sort->GetOutputSchema(), &consumer);
    query->Run(&exec_ctx, kExecutionMode);
  }

  // Only time execution.
  for (auto _ : state) {
    sql::MemoryPool memory(nullptr);
    sql::ExecutionContext exec_ctx(&memory, sort->GetOutputSchema(), &consumer);
    query->Run(&exec_ctx, kExecutionMode);
  }
}

// ---------------------------------------------------------
//
// Benchmark Configs
//
// ---------------------------------------------------------

BENCHMARK_REGISTER_F(StarSchemaBenchmark, Q1_1)->Unit(benchmark::kMillisecond);
BENCHMARK_REGISTER_F(StarSchemaBenchmark, Q1_2)->Unit(benchmark::kMillisecond);
BENCHMARK_REGISTER_F(StarSchemaBenchmark, Q1_3)->Unit(benchmark::kMillisecond);
BENCHMARK_REGISTER_F(StarSchemaBenchmark, Q2_1)->Unit(benchmark::kMillisecond);
BENCHMARK_REGISTER_F(StarSchemaBenchmark, Q2_2)->Unit(benchmark::kMillisecond);
BENCHMARK_REGISTER_F(StarSchemaBenchmark, Q2_3)->Unit(benchmark::kMillisecond);
BENCHMARK_REGISTER_F(StarSchemaBenchmark, Q3_1)->Unit(benchmark::kMillisecond);
BENCHMARK_REGISTER_F(StarSchemaBenchmark, Q3_2)->Unit(benchmark::kMillisecond);
BENCHMARK_REGISTER_F(StarSchemaBenchmark, Q3_3)->Unit(benchmark::kMillisecond);
BENCHMARK_REGISTER_F(StarSchemaBenchmark, Q3_4)->Unit(benchmark::kMillisecond);
BENCHMARK_REGISTER_F(StarSchemaBenchmark, Q4_1)->Unit(benchmark::kMillisecond);
BENCHMARK_REGISTER_F(StarSchemaBenchmark, Q4_2)->Unit(benchmark::kMillisecond);
BENCHMARK_REGISTER_F(StarSchemaBenchmark, Q4_3)->Unit(benchmark::kMillisecond);

}  // namespace tpl::sql::codegen
