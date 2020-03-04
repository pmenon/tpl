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
   * NLJ plans do not allocate state. Thus, this method is a no-op.
   */
  void InitializeQueryState(FunctionBuilder *) const override {}

  /**
   * NLJ plans do not allocate state. Thus, this method is a no-op.
   */
  void TearDownQueryState(FunctionBuilder *) const override {}

  /**
   * NLJ plans do not allocate state. Thus, this method is a no-op.
   */
  void InitializePipelineState(UNUSED const Pipeline &, UNUSED FunctionBuilder *) const override {}

  /**
   * NLJ plans do not perform any pre-pipeline work. This, this method is a no-op.
   */
  void BeginPipelineWork(UNUSED const Pipeline &, UNUSED FunctionBuilder *) const override {}

  /**
   * Generate the join condition from the two child inputs.
   * @param work_context The context of the work.
   */
  void PerformPipelineWork(WorkContext *work_context, FunctionBuilder *function) const override;

  /**
   * NLJ plans do not perform any post-pipeline work. Thus, this method is a no-op.
   */
  void FinishPipelineWork(UNUSED const Pipeline &, UNUSED FunctionBuilder *) const override {}

  /**
   * NLJ plans do not allocate state. Thus, this method is a no-op.
   */
  void TearDownPipelineState(UNUSED const Pipeline &, UNUSED FunctionBuilder *) const override {}

  /**
   * NLJ plans are not the root of a pipeline. Thus, this method should never be called.
   */
  util::RegionVector<ast::FieldDecl *> GetWorkerParams() const override {
    UNREACHABLE("NLJ are never the root of a plan and, thus, cannot be launched in parallel.");
  }

  /**
   * NLJ plans are not the root of a pipeline. Thus, this method should never be called.
   */
  void LaunchWork(UNUSED FunctionBuilder *, UNUSED ast::Identifier) const override {
    UNREACHABLE("NLJ are never the root of a plan and, thus, cannot be launched in parallel.");
  }

  /**
   * @return The value of the ou
   */
  ast::Expr *GetChildOutput(WorkContext *work_context, uint32_t child_idx,
                            uint32_t attr_idx) const override;

  ast::Expr *GetTableColumn(uint16_t col_oid) const override {
    UNREACHABLE("Nested-loop joins do not produce columns from base tables.");
  }
};

}  // namespace tpl::sql::codegen
