#include "util/sql_test_harness.h"

#include <memory>

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

#include "vm/llvm_engine.h"

// Tests
#include "sql/planner/expression_maker.h"

namespace tpl::sql::codegen {

class SeqScanTranslatorTest : public SqlBasedTest {
 protected:
  void SetUp() override { SqlBasedTest::SetUp(); }
  static void SetUpTestSuite() { tpl::vm::LLVMEngine::Initialize(); }
  static void TearDownTestSuite() { tpl::vm::LLVMEngine::Shutdown(); }
};

TEST_F(SeqScanTranslatorTest, SimpleScan) {
  /*
   * SELECT * FROM test_1 WHERE colA <= 44 AND colB > 2
   */

  auto schema = std::make_unique<Schema>(std::vector<Schema::ColumnInfo>{
      Schema::ColumnInfo("a", IntegerType::InstanceNonNullable()),
      Schema::ColumnInfo("b", IntegerType::InstanceNonNullable()),
  });

  auto pool = std::make_unique<MemoryPool>(nullptr);
  auto printer = std::make_unique<sql::PrintingConsumer>(std::cout, *schema);
  auto exec_ctx = std::make_unique<sql::ExecutionContext>(pool.get(), schema.get(), printer.get());
  auto expr_maker = planner::ExpressionMaker();

  auto plan =
      // The sequential scan input
      planner::SeqScanPlanNode::Builder()
          .SetTableOid(1)
          .SetScanPredicate(expr_maker.ConjunctionAnd(
              // col0 <= 44
              expr_maker.CompareLe(expr_maker.CVE(0, TypeId::Integer), expr_maker.Constant(44)),
              // col1 > 2
              expr_maker.CompareGt(expr_maker.CVE(1, TypeId::Integer), expr_maker.Constant(2))))
          .SetOutputSchema(
              planner::OutputSchema::Builder()
                  .AddColumn(TypeId::Integer, false, expr_maker.CVE(0, TypeId::Integer))
                  .AddColumn(TypeId::Integer, false, expr_maker.CVE(1, TypeId::Integer))
                  .Build())
          .Build();

  auto query = CompilationContext::Compile(*plan);
  query->Run(exec_ctx.get());
}

TEST_F(SeqScanTranslatorTest, SimpleSort) {
  /*
   * SELECT * FROM test_1 ORDER BY colA
   */
  auto schema = std::make_unique<Schema>(std::vector<Schema::ColumnInfo>{
      Schema::ColumnInfo("a", IntegerType::InstanceNonNullable()),
      Schema::ColumnInfo("b", IntegerType::InstanceNonNullable()),
  });

  auto pool = std::make_unique<MemoryPool>(nullptr);
  auto printer = std::make_unique<sql::PrintingConsumer>(std::cout, *schema);
  auto exec_ctx = std::make_unique<sql::ExecutionContext>(pool.get(), schema.get(), printer.get());
  auto expr_maker = planner::ExpressionMaker();

  auto plan =
      planner::OrderByPlanNode::Builder()
          .AddSortKey(expr_maker.DVE(TypeId::Integer, 0, 0), planner::OrderByOrderingType::ASC)
          .SetLimit(100)
          .AddChild(
              // The sequential scan input
              planner::SeqScanPlanNode::Builder()
                  .SetTableOid(1)
                  .SetOutputSchema(
                      planner::OutputSchema::Builder()
                          .AddColumn(TypeId::Integer, false, expr_maker.CVE(0, TypeId::Integer))
                          .AddColumn(TypeId::Integer, false, expr_maker.CVE(1, TypeId::Integer))
                          .Build())
                  .Build())
          .SetOutputSchema(
              planner::OutputSchema::Builder()
                  .AddColumn(TypeId::Integer, false, expr_maker.DVE(TypeId::Integer, 0, 0))
                  .AddColumn(TypeId::Integer, false, expr_maker.DVE(TypeId::Integer, 0, 1))
                  .Build())
          .Build();

  auto query = CompilationContext::Compile(*plan);
  query->Run(exec_ctx.get());
}

TEST_F(SeqScanTranslatorTest, SimpleNLJ) {
  /*
   * SELECT * FROM test_1 t1 INNER JOIN test_2 t2 on t1.colA = t2.col2;
   */
  auto schema = std::make_unique<Schema>(std::vector<Schema::ColumnInfo>{
      Schema::ColumnInfo("a", IntegerType::InstanceNonNullable()),
      Schema::ColumnInfo("b", IntegerType::InstanceNonNullable()),
  });

  auto pool = std::make_unique<MemoryPool>(nullptr);
  auto printer = std::make_unique<sql::PrintingConsumer>(std::cout, *schema);
  auto exec_ctx = std::make_unique<sql::ExecutionContext>(pool.get(), schema.get(), printer.get());
  auto expr_maker = planner::ExpressionMaker();

  auto plan = planner::NestedLoopJoinPlanNode::Builder()
                  // Left scan on 'small_1'
                  .AddChild(planner::SeqScanPlanNode::Builder()
                                .SetTableOid(4)
                                .SetScanPredicate(
                                    // col0 <= 44
                                    expr_maker.CompareLe(expr_maker.CVE(0, TypeId::Integer),
                                                         expr_maker.Constant(44)))
                                .SetOutputSchema(planner::OutputSchema::Builder()
                                                     .AddColumn(TypeId::Integer, false,
                                                                expr_maker.CVE(0, TypeId::Integer))
                                                     .Build())
                                .Build())
                  // Right scan on 'small_1'
                  .AddChild(planner::SeqScanPlanNode::Builder()
                                .SetTableOid(4)
                                .SetOutputSchema(planner::OutputSchema::Builder()
                                                     .AddColumn(TypeId::Integer, false,
                                                                expr_maker.CVE(1, TypeId::Integer))
                                                     .Build())
                                .Build())
                  // test_1.colA = test_2.col1
                  .SetJoinPredicate(expr_maker.CompareEq(expr_maker.DVE(TypeId::Integer, 0, 0),
                                                         expr_maker.DVE(TypeId::Integer, 1, 0)))
                  .SetOutputSchema(
                      planner::OutputSchema::Builder()
                          .AddColumn(TypeId::Integer, false, expr_maker.DVE(TypeId::Integer, 0, 0))
                          .AddColumn(TypeId::Integer, false, expr_maker.DVE(TypeId::Integer, 1, 0))
                          .Build())
                  .Build();

  auto query = CompilationContext::Compile(*plan);
  query->Run(exec_ctx.get());
}

TEST_F(SeqScanTranslatorTest, SimpleHashJoin) {
  /*
   * SELECT * FROM test_1 INNER JOIN test_2 on test_1.colA = test_2.col2
   */
  auto schema = std::make_unique<Schema>(std::vector<Schema::ColumnInfo>{
      Schema::ColumnInfo("a", IntegerType::InstanceNonNullable()),
      Schema::ColumnInfo("b", IntegerType::InstanceNonNullable()),
  });

  auto pool = std::make_unique<MemoryPool>(nullptr);
  auto printer = std::make_unique<sql::PrintingConsumer>(std::cout, *schema);
  auto exec_ctx = std::make_unique<sql::ExecutionContext>(pool.get(), schema.get(), printer.get());
  auto expr_maker = planner::ExpressionMaker();

  auto plan =
      planner::HashJoinPlanNode::Builder()
          // Scan 'test_1'
          .AddChild(
              planner::SeqScanPlanNode::Builder()
                  .SetTableOid(1)
                  .SetOutputSchema(
                      planner::OutputSchema::Builder()
                          .AddColumn(TypeId::Integer, false, expr_maker.CVE(0, TypeId::Integer))
                          .AddColumn(TypeId::Integer, false, expr_maker.CVE(1, TypeId::Integer))
                          .Build())
                  .Build())
          // Scan 'test_2'
          .AddChild(
              planner::SeqScanPlanNode::Builder()
                  .SetTableOid(2)
                  .SetOutputSchema(
                      planner::OutputSchema::Builder()
                          .AddColumn(TypeId::Integer, false, expr_maker.CVE(0, TypeId::Integer))
                          .AddColumn(TypeId::Integer, false, expr_maker.CVE(1, TypeId::Integer))
                          .Build())
                  .Build())
          .AddLeftHashKey(expr_maker.DVE(TypeId::Integer, 0, 0))
          .AddRightHashKey(expr_maker.DVE(TypeId::Integer, 1, 0))
          .SetJoinPredicate(expr_maker.CompareEq(expr_maker.DVE(TypeId::Integer, 0, 0),
                                                 expr_maker.DVE(TypeId::Integer, 1, 0)))
          .SetOutputSchema(
              planner::OutputSchema::Builder()
                  .AddColumn(TypeId::Integer, false, expr_maker.DVE(TypeId::Integer, 0, 0))
                  .AddColumn(TypeId::Integer, false, expr_maker.DVE(TypeId::Integer, 0, 1))
                  .AddColumn(TypeId::Integer, false, expr_maker.DVE(TypeId::Integer, 1, 0))
                  .AddColumn(TypeId::Integer, false, expr_maker.DVE(TypeId::Integer, 1, 0))
                  .Build())
          .Build();

  auto query = CompilationContext::Compile(*plan);
  query->Run(exec_ctx.get());
}

TEST_F(SeqScanTranslatorTest, SimpleAggregation) {
  /*
   * SELECT   a, b, c, d, e, SUM(b), MIN(c), MAX(d), SUM(f), MIN(f), MAX(g), AVG(g)
   * FROM     all_types
   * WHERE    b < 1000
   * GROUP BY b;
   */
  auto schema = std::make_unique<Schema>(std::vector<Schema::ColumnInfo>{
      Schema::ColumnInfo("a", IntegerType::InstanceNonNullable()),
      Schema::ColumnInfo("b", IntegerType::InstanceNonNullable()),
  });

  auto pool = std::make_unique<MemoryPool>(nullptr);
  auto printer = std::make_unique<sql::PrintingConsumer>(std::cout, *schema);
  auto exec_ctx = std::make_unique<sql::ExecutionContext>(pool.get(), schema.get(), printer.get());
  auto expr_maker = planner::ExpressionMaker();

  auto plan =
      planner::AggregatePlanNode::Builder()
          // Scan 'all_types'
          .AddChild(
              planner::SeqScanPlanNode::Builder()
                  .SetTableOid(3)
                  .SetScanPredicate(expr_maker.CompareEq(expr_maker.CVE(1, TypeId::TinyInt),
                                                         expr_maker.Constant(44)))
                  .SetOutputSchema(
                      planner::OutputSchema::Builder()
                          .AddColumn(TypeId::Boolean, false, expr_maker.CVE(0, TypeId::Boolean))
                          .AddColumn(TypeId::TinyInt, false, expr_maker.CVE(1, TypeId::TinyInt))
                          .AddColumn(TypeId::SmallInt, false, expr_maker.CVE(2, TypeId::SmallInt))
                          .AddColumn(TypeId::Integer, false, expr_maker.CVE(3, TypeId::Integer))
                          .AddColumn(TypeId::BigInt, false, expr_maker.CVE(4, TypeId::BigInt))
                          .AddColumn(TypeId::Float, false, expr_maker.CVE(5, TypeId::Float))
                          .AddColumn(TypeId::Double, false, expr_maker.CVE(6, TypeId::Double))
                          .Build())
                  .Build())
          .AddGroupByTerm(expr_maker.DVE(TypeId::TinyInt, 0, 1))
          .AddAggregateTerm(expr_maker.AggSum(expr_maker.DVE(TypeId::BigInt, 0, 1)))
          .AddAggregateTerm(expr_maker.AggMin(expr_maker.DVE(TypeId::BigInt, 0, 2)))
          .AddAggregateTerm(expr_maker.AggMax(expr_maker.DVE(TypeId::BigInt, 0, 3)))
          .AddAggregateTerm(expr_maker.AggSum(expr_maker.DVE(TypeId::Double, 0, 5)))
          .AddAggregateTerm(expr_maker.AggMin(expr_maker.DVE(TypeId::Double, 0, 5)))
          .AddAggregateTerm(expr_maker.AggMax(expr_maker.DVE(TypeId::Double, 0, 6)))
          .AddAggregateTerm(expr_maker.AggAvg(expr_maker.DVE(TypeId::Double, 0, 6)))
          .SetOutputSchema(
              planner::OutputSchema::Builder()
                  .AddColumn(TypeId::Integer, false, expr_maker.DVE(TypeId::Integer, 0, 0))
                  .AddColumn(TypeId::Integer, false, expr_maker.DVE(TypeId::Integer, 0, 1))
                  .AddColumn(TypeId::Integer, false, expr_maker.DVE(TypeId::Integer, 1, 0))
                  .AddColumn(TypeId::Integer, false, expr_maker.DVE(TypeId::Integer, 1, 0))
                  .Build())
          .Build();

  auto query = CompilationContext::Compile(*plan);
  query->Run(exec_ctx.get());
}

}  // namespace tpl::sql::codegen
