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
  void InitializeQueryState() const override {}

  /**
   * NLJ plans do not allocate state. Thus, this method is a no-op.
   */
  void TearDownQueryState() const override {}

  /**
   * NLJ plans do not allocate state. Thus, this method is a no-op.
   */
  void DeclarePipelineState(PipelineContext *pipeline_context) override {}

  /**
   * NLJ plans do not allocate state. Thus, this method is a no-op.
   */
  void InitializePipelineState(const PipelineContext &pipeline_context) const override {}

  /**
   * NLJ plans do not perform any pre-pipeline work. This, this method is a no-op.
   */
  void BeginPipelineWork(const PipelineContext &pipeline_context) const override {}

  /**
   * Generate the join condition from the two child inputs.
   * @param consumer_context The consumer context.
   */
  void DoPipelineWork(ConsumerContext *consumer_context) const override;

  /**
   * NLJ plans are not the root of a pipeline. Thus, this method should never be called.
   */
  util::RegionVector<ast::FieldDecl *> GetWorkerParams() const override {
    UNREACHABLE("Impossible");
  }

  /**
   * NLJ plans are not the root of a pipeline. Thus, this method should never be called.
   */
  void LaunchWork(ast::Identifier work_func_name) const override { UNREACHABLE("Impossible"); }

  /**
   * NLJ plans do not perform any post-pipeline work. Thus, this method is a no-op.
   */
  void FinishPipelineWork(const PipelineContext &pipeline_context) const override {}

  /**
   * NLJ plans do not allocate state. Thus, this method is a no-op.
   */
  void TearDownPipelineState(const PipelineContext &pipeline_context) const override {}
};

}  // namespace tpl::sql::codegen
