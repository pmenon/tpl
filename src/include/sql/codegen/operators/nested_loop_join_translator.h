#pragma once

#include "sql/codegen/operators/operator_translator.h"

namespace tpl::sql::planner {
class NestedLoopJoinPlanNode;
}  // namespace tpl::sql::planner

namespace tpl::sql::codegen {

/**
 * A translator for nested-loop joins.
 */
class NestedLoopJoinTranslator : public OperatorTranslator {
 public:
  /**
   * Create a new translator for the given nested-loop join plan. The translator occurs within the
   * provided compilation context, and the operator is a step in the provided pipeline.
   * @param plan The plan.
   * @param compilation_context The context of compilation this translation is occurring in.
   * @param pipeline The pipeline this operator is participating in.
   */
  NestedLoopJoinTranslator(const planner::NestedLoopJoinPlanNode &plan,
                           CompilationContext *compilation_context, Pipeline *pipeline);

  /**
   * Generate the join condition from the two child inputs.
   * @param context The context of the work.
   */
  void Consume(ConsumerContext *context, FunctionBuilder *function) const override;

  /**
   * NLJ plans do not touch base tables.
   */
  edsl::ValueVT GetTableColumn(uint16_t col_oid) const override {
    UNREACHABLE("Nested-loop joins do not produce columns from base tables.");
  }

 private:
  // Get the NLJ plan node.
  const planner::NestedLoopJoinPlanNode &GetNLJPlan() const {
    return GetPlanAs<planner::NestedLoopJoinPlanNode>();
  }
};

}  // namespace tpl::sql::codegen
