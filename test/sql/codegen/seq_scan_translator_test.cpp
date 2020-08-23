#include <memory>

#include "sql/catalog.h"
#include "sql/codegen/compilation_context.h"
#include "sql/planner/plannodes/projection_plan_node.h"
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

class SeqScanTranslatorTest : public CodegenBasedTest {};

TEST_F(SeqScanTranslatorTest, ScanTest) {
  // SELECT col1, col2, col1 * col2, col1 >= 100*col2 FROM test_1 WHERE col1 < 500 AND col2 >= 5;
  auto accessor = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;
  sql::Table *table = accessor->LookupTableByName("test_1");
  const auto &table_schema = table->GetSchema();

  const int32_t col1_filter_val = 500, col2_filter_val = 5;

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
    auto comp1 = expr_maker.CompareLt(col1, expr_maker.Constant(col1_filter_val));
    auto comp2 = expr_maker.CompareGe(col2, expr_maker.Constant(col2_filter_val));
    auto predicate = expr_maker.ConjunctionAnd(comp1, comp2);
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(std::move(schema))
                   .SetScanPredicate(predicate)
                   .SetTableOid(table->GetId())
                   .Build();
  }

  // Compile.
  auto query = CompilationContext::Compile(*seq_scan);

  // Run and check.
  ExecuteAndCheckInAllModes(query.get(), [&]() {
    // Checkers:
    // 1. Filtered column (0) is less than 500 (i.e., the filtering value);
    // 2. Filtered column (1) is less than 5 (i.e., the filtering value);
    std::vector<std::unique_ptr<OutputChecker>> checks;
    checks.push_back(
        std::make_unique<SingleColumnValueChecker<Integer>>(std::less<>(), 0, col1_filter_val));
    checks.push_back(std::make_unique<SingleColumnValueChecker<Integer>>(std::greater_equal<>(), 1,
                                                                         col2_filter_val));
    return std::make_unique<MultiChecker>(std::move(checks));
  });
}

TEST_F(SeqScanTranslatorTest, ScanWithNullCheckTest) {
  // SELECT colb FROM test_1 WHERE cola IS NULL;
  auto accessor = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;

  std::unique_ptr<planner::AbstractPlanNode> seq_scan;
  planner::OutputSchemaHelper seq_scan_out{&expr_maker, 0};
  {
    sql::Table *table = accessor->LookupTableByName("test_1");
    const auto &table_schema = table->GetSchema();
    // Get table columns.
    auto colb = expr_maker.CVE(table_schema.GetColumnInfo("colB").oid, sql::TypeId::Integer);
    seq_scan_out.AddOutput("colb", colb);
    // Predicate.
    auto predicate =
        expr_maker.Operator(planner::ExpressionType::OPERATOR_IS_NULL, TypeId::Boolean, colb);
    // Build plan.
    seq_scan = planner::SeqScanPlanNode::Builder()
                   .SetOutputSchema(seq_scan_out.MakeSchema())
                   .SetScanPredicate(predicate)
                   .SetTableOid(table->GetId())
                   .Build();
  }

  // Compile.
  auto query = CompilationContext::Compile(*seq_scan);

  // Run and check.
  ExecuteAndCheckInAllModes(query.get(), [&]() {
    // Checkers:
    // 1. 'cola' is non-nullable, so no rows should be selected.
    std::vector<std::unique_ptr<OutputChecker>> checks;
    checks.push_back(std::make_unique<TupleCounterChecker>(0));
    return std::make_unique<MultiChecker>(std::move(checks));
  });
}

TEST_F(SeqScanTranslatorTest, ScanWithNonVectorizedFilterTest) {
  // SELECT col1, col2, col1 * col2, col1 >= 100*col2 FROM test_1
  // WHERE (col1 < 500 AND col2 >= 5) OR (500 <= col1 <= 1000 AND (col2 = 3 OR col2 = 7));
  // The filter is not in DNF form and can't be vectorized.
  auto accessor = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;
  sql::Table *table = accessor->LookupTableByName("test_1");
  const auto &table_schema = table->GetSchema();
  std::unique_ptr<planner::AbstractPlanNode> seq_scan;
  planner::OutputSchemaHelper seq_scan_out(&expr_maker, 0);
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

  // Compile.
  auto query = CompilationContext::Compile(*seq_scan);

  // Run and check.
  ExecuteAndCheckInAllModes(query.get(), [&]() {
    // Check filtering value.
    std::vector<std::unique_ptr<OutputChecker>> checks;
    checks.push_back(std::make_unique<GenericChecker>(
        [](const std::vector<const sql::Val *> &vals) {
          // Read cols
          auto col1 = static_cast<const sql::Integer *>(vals[0]);
          auto col2 = static_cast<const sql::Integer *>(vals[1]);
          ASSERT_FALSE(col1->is_null);
          ASSERT_FALSE(col2->is_null);
          // Check predicate.
          ASSERT_TRUE((col1->val < 500 && col2->val >= 5) ||
                      (col1->val >= 500 && col1->val < 1000 && (col2->val == 7 || col2->val == 3)));
        },
        nullptr));
    return std::make_unique<MultiChecker>(std::move(checks));
  });
}

TEST_F(SeqScanTranslatorTest, ScanWithProjection) {
  // SELECT col1, col2, col1 * col1, col1 >= 100*col2 FROM test_1 WHERE col1 < 500;
  // This test first selects col1 and col2, and then constructs the 4 output columns.

  auto accessor = sql::Catalog::Instance();
  sql::Table *table = accessor->LookupTableByName("test_1");
  const auto &table_schema = table->GetSchema();

  planner::ExpressionMaker expr_maker;
  std::unique_ptr<planner::AbstractPlanNode> seq_scan;
  planner::OutputSchemaHelper seq_scan_out(&expr_maker, 0);
  {
    // Get Table columns
    auto col1 = expr_maker.CVE(table_schema.GetColumnInfo("colA").oid, sql::TypeId::Integer);
    auto col2 = expr_maker.CVE(table_schema.GetColumnInfo("colB").oid, sql::TypeId::Integer);
    // Make New Column
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

  std::unique_ptr<planner::AbstractPlanNode> proj;
  planner::OutputSchemaHelper proj_out(&expr_maker, 0);
  {
    auto col1 = seq_scan_out.GetOutput("col1");
    auto col2 = seq_scan_out.GetOutput("col2");
    auto col3 = expr_maker.OpMul(col1, col1);
    auto col4 = expr_maker.CompareGe(col1, expr_maker.OpMul(expr_maker.Constant(100), col2));
    proj_out.AddOutput("col1", col1);
    proj_out.AddOutput("col2", col2);
    proj_out.AddOutput("col3", col3);
    proj_out.AddOutput("col4", col4);
    auto schema = proj_out.MakeSchema();
    planner::ProjectionPlanNode::Builder builder;
    proj = builder.SetOutputSchema(std::move(schema)).AddChild(std::move(seq_scan)).Build();
  }

  // Compile.
  auto query = CompilationContext::Compile(*proj);

  // Run and check.
  ExecuteAndCheckInAllModes(query.get(), [&]() {
    // Make the output checkers:
    // 1. There has to be exactly 500 rows, due to the filter.
    // 2. Check col3 and col4 values for each row.
    std::vector<std::unique_ptr<OutputChecker>> checks;
    checks.push_back(std::make_unique<SingleColumnValueChecker<Integer>>(std::less<>(), 0, 500));
    checks.push_back(std::make_unique<GenericChecker>(
        [](const std::vector<const sql::Val *> &vals) {
          auto col1 = static_cast<const sql::Integer *>(vals[0]);
          auto col2 = static_cast<const sql::Integer *>(vals[1]);
          auto col3 = static_cast<const sql::Integer *>(vals[2]);
          auto col4 = static_cast<const sql::BoolVal *>(vals[3]);
          // col3 = col1*col1
          EXPECT_FALSE(col3->is_null);
          const auto root = std::round(std::sqrt(col3->val));
          EXPECT_EQ(col3->val, root * root);
          // col4 = col1 >= 100*col2
          EXPECT_FALSE(col4->is_null);
          EXPECT_EQ(col4->val, col1->val >= 100 * col2->val);
        },
        nullptr));
    return std::make_unique<MultiChecker>(std::move(checks));
  });
}

TEST_F(SeqScanTranslatorTest, ScanWithAllColumnTypes) {
  // SELECT * FROM all_types WHERE a=TRUE;
  // 'a' if filled with 50% true and 50% false.

  auto accessor = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;
  sql::Table *table = accessor->LookupTableByName("all_types");

  std::unique_ptr<planner::AbstractPlanNode> seq_scan;
  planner::OutputSchemaHelper seq_scan_out{&expr_maker, 0};
  {
    const auto &table_schema = table->GetSchema();
    // Get all columns.
    auto a = expr_maker.CVE(table_schema.GetColumnInfo("a").oid, sql::TypeId::Boolean);
    auto b = expr_maker.CVE(table_schema.GetColumnInfo("b").oid, sql::TypeId::TinyInt);
    auto c = expr_maker.CVE(table_schema.GetColumnInfo("c").oid, sql::TypeId::SmallInt);
    auto d = expr_maker.CVE(table_schema.GetColumnInfo("d").oid, sql::TypeId::Integer);
    auto e = expr_maker.CVE(table_schema.GetColumnInfo("e").oid, sql::TypeId::BigInt);
    auto f = expr_maker.CVE(table_schema.GetColumnInfo("f").oid, sql::TypeId::Float);
    auto g = expr_maker.CVE(table_schema.GetColumnInfo("g").oid, sql::TypeId::Double);
    // Set outputs.
    seq_scan_out.AddOutput("a", a);
    seq_scan_out.AddOutput("b", b);
    seq_scan_out.AddOutput("c", c);
    seq_scan_out.AddOutput("d", d);
    seq_scan_out.AddOutput("e", e);
    seq_scan_out.AddOutput("f", f);
    seq_scan_out.AddOutput("g", g);
    // Build plan.
    planner::SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(seq_scan_out.MakeSchema())
                   .SetScanPredicate(expr_maker.CompareEq(a, expr_maker.ConstantBool(true)))
                   .SetTableOid(table->GetId())
                   .Build();
  }

  // Compile.
  auto query = CompilationContext::Compile(*seq_scan);

  // Run and check.
  ExecuteAndCheckInAllModes(query.get(), [&]() {
    // Checkers:
    // 1. Total number of rows should be N/2 where N=table size.
    // 2. All 'a' values should be true.
    std::vector<std::unique_ptr<OutputChecker>> checks;
    checks.push_back(
        std::make_unique<SingleColumnValueChecker<sql::BoolVal>>(std::equal_to<>(), 0, true));
    checks.push_back(std::make_unique<TupleCounterChecker>(table->GetTupleCount() / 2));
    return std::make_unique<MultiChecker>(std::move(checks));
  });
}

}  // namespace tpl::sql::codegen
