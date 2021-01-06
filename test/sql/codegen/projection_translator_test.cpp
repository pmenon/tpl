#include <memory>

#include "sql/catalog.h"
#include "sql/planner/plannodes/projection_plan_node.h"
#include "sql/printing_consumer.h"
#include "sql/schema.h"
#include "sql/table.h"

// Tests
#include "sql/codegen/output_checker.h"
#include "sql/planner/expression_maker.h"
#include "sql/planner/output_schema_util.h"
#include "util/codegen_test_harness.h"

namespace tpl::sql::codegen {

class ProjectionTranslatorTest : public CodegenBasedTest {};

TEST_F(ProjectionTranslatorTest, SimpleArithmeticProjections) {
  // Self join:
  // SELECT COS(1.0), SIN(90), 1+2, 4.0*5.0;

  planner::ExpressionMaker expr_maker;

  // Projection Node.
  std::unique_ptr<planner::AbstractPlanNode> projection;
  planner::OutputSchemaHelper projection_out(&expr_maker, 0);
  {
    auto col1 = expr_maker.UnaryOperator(KnownOperator::Cos, Type::DoubleType(false),
                                         expr_maker.Constant(1.0F));
    auto col2 = expr_maker.UnaryOperator(KnownOperator::Sin, Type::DoubleType(false),
                                         expr_maker.Constant(90.0F));
    auto col3 = expr_maker.OpSum(expr_maker.Constant(1), expr_maker.Constant(2));
    auto col4 = expr_maker.OpMul(expr_maker.Constant(4.0F), expr_maker.Constant(5.0F));
    projection_out.AddOutput("col1", col1);
    projection_out.AddOutput("col2", col2);
    projection_out.AddOutput("col3", col3);
    projection_out.AddOutput("col4", col4);
    projection =
        planner::ProjectionPlanNode::Builder().SetOutputSchema(projection_out.MakeSchema()).Build();
  }

  // Run and check.
  ExecuteAndCheckInAllModes(*projection, []() {
    // Checkers:
    // 1. Only 1 output row.
    // 2. First column must be std::cos(1.0)
    // 2. Second column must be std::sin(90.0)
    // 3. Third column must be 1+2=3
    // 4. Fourth column must be 4.0*5.0=20.0
    // clang-format off
    std::vector<std::unique_ptr<OutputChecker>> checks;
    checks.emplace_back(std::make_unique<TupleCounterChecker>(1));
    checks.emplace_back(std::make_unique<SingleColumnValueChecker<sql::Real>>(std::equal_to<>(), 0, std::cos(1.0)));
    checks.emplace_back(std::make_unique<SingleColumnValueChecker<sql::Real>>(std::equal_to<>(), 1, std::sin(90.0)));
    checks.emplace_back(std::make_unique<SingleColumnValueChecker<sql::Integer>>(std::equal_to<>(), 2, 3));
    checks.emplace_back(std::make_unique<SingleColumnValueChecker<sql::Real>>(std::equal_to<>(), 3, 20.0));
    // clang-format on
    return std::make_unique<MultiChecker>(std::move(checks));
  });
}

}  // namespace tpl::sql::codegen
