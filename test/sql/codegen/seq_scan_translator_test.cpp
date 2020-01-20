#include "util/test_harness.h"

#include <memory>

#include "sql/codegen/compilation_context.h"
#include "sql/execution_context.h"
#include "sql/planner/plannodes/hash_join_plan_node.h"
#include "sql/planner/plannodes/order_by_plan_node.h"
#include "sql/planner/plannodes/seq_scan_plan_node.h"
#include "sql/printing_consumer.h"
#include "sql/schema.h"

// Tests
#include "sql/planner/expression_maker.h"

namespace tpl::sql::codegen {

class SeqScanTranslatorTest : public TplTest {};

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
          .AddSortKey(expr_maker.CVE(0, TypeId::Integer), planner::OrderByOrderingType::ASC)
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

#if 0
TEST_F(SeqScanTranslatorTest, SimpleHashJoin) {
  /*
   * SELECT * FROM test_1 INNER JOIN test_2 on test_1.colA = test_2.colB
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
          .AddChild(
              // test_1
              planner::SeqScanPlanNode::Builder()
                  .SetTableOid(1)
                  .SetOutputSchema(
                      planner::OutputSchema::Builder()
                          .AddColumn(TypeId::Integer, false, expr_maker.CVE(0, TypeId::Integer))
                          .AddColumn(TypeId::Integer, false, expr_maker.CVE(1, TypeId::Integer))
                          .Build())
                  .Build())
          .AddChild(
              // test_2
              planner::SeqScanPlanNode::Builder()
                  .SetTableOid(2)
                  .SetOutputSchema(
                      planner::OutputSchema::Builder()
                          .AddColumn(TypeId::Integer, false, expr_maker.CVE(0, TypeId::Integer))
                          .AddColumn(TypeId::Integer, false, expr_maker.CVE(1, TypeId::Integer))
                          .Build())
                  .Build())
          .AddLeftHashKey(expr_maker.DVE(TypeId::Integer, 0, 0))
          .AddRightHashKey(expr_maker.DVE(TypeId::Integer, 1, 1))
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
#endif

}  // namespace tpl::sql::codegen
