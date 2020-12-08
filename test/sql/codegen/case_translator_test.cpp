#include <memory>

#include "sql/catalog.h"
#include "sql/planner/plannodes/seq_scan_plan_node.h"
#include "sql/schema.h"
#include "sql/table.h"

// Tests
#include "sql/codegen/output_checker.h"
#include "sql/planner/expression_maker.h"
#include "sql/planner/output_schema_util.h"
#include "util/codegen_test_harness.h"

namespace tpl::sql::codegen {

using namespace std::chrono_literals;

using Expr = planner::ExpressionMaker::Expression;

class CaseTranslatorTest : public CodegenBasedTest {};

TEST_F(CaseTranslatorTest, CasesWithDefault) {
  // SELECT CASE
  //            WHEN colB < 2 THEN 'VERY LOW'
  //            WHEN colB < 5 THEN 'LOW'
  //            WHEN colB < 7 THEN 'HIGH'
  //            ELSE 'VERY HIGH'
  //        END as category
  // FROM test_1;

  auto accessor = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;
  sql::Table *table = accessor->LookupTableByName("test_1");
  const auto &table_schema = table->GetSchema();

  static constexpr int32_t kColBFilterVal1 = 2;
  static constexpr int32_t kColBFilterVal2 = 5;
  static constexpr int32_t kColBFilterVal3 = 7;

  std::unique_ptr<planner::AbstractPlanNode> seq_scan;
  planner::OutputSchemaHelper seq_scan_out(&expr_maker, 0);
  {
    // Columns.
    auto colb = expr_maker.CVE(table_schema.GetColumnInfo("colB").oid, sql::TypeId::Integer);

    // Case.
    std::vector<std::pair<Expr, Expr>> clauses;
    clauses.emplace_back(expr_maker.CompareLt(colb, expr_maker.Constant(kColBFilterVal1)),
                         expr_maker.Constant("VERY LOW"));
    clauses.emplace_back(expr_maker.CompareLt(colb, expr_maker.Constant(kColBFilterVal2)),
                         expr_maker.Constant("LOW"));
    clauses.emplace_back(expr_maker.CompareLt(colb, expr_maker.Constant(kColBFilterVal3)),
                         expr_maker.Constant("HIGH"));

    // Outputs.
    seq_scan_out.AddOutput("colb", colb);
    seq_scan_out.AddOutput("category", expr_maker.Case(clauses, expr_maker.Constant("VERY HIGH")));

    // Build.
    planner::SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(seq_scan_out.MakeSchema())
                   .SetScanPredicate(nullptr)
                   .SetTableOid(table->GetId())
                   .Build();
  }

  // Run and check.
  ExecuteAndCheckInAllModes(*seq_scan, [&]() {
    // Check category column based on colb.
    std::vector<std::unique_ptr<OutputChecker>> checks;
    checks.emplace_back(std::make_unique<GenericChecker>(
        [&](const auto &vals) {
          auto colb = static_cast<const Integer *>(vals[0]);
          auto category = static_cast<const StringVal *>(vals[1]);
          auto expected = colb->val < kColBFilterVal1   ? "VERY LOW"
                          : colb->val < kColBFilterVal2 ? "LOW"
                          : colb->val < kColBFilterVal3 ? "HIGH"
                                                        : "VERY HIGH";
          EXPECT_FALSE(vals[0]->is_null);
          EXPECT_FALSE(vals[1]->is_null);
          EXPECT_EQ(expected, category->val.GetStringView())
              << "colb=" << colb->val << " expected category=" << expected
              << ", but received category=" << category->val.GetStringView();
        },
        nullptr));
    return std::make_unique<MultiChecker>(std::move(checks));
  });
}

}  // namespace tpl::sql::codegen
