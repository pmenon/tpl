#pragma once

#include "sql/codegen/operators/operator_translator.h"

namespace tpl::sql::planner {
class ProjectionPlanNode;
}  // namespace tpl::sql::planner

namespace tpl::sql::codegen {

/**
 * Translator for projections.
 */
class ProjectionTranslator : public OperatorTranslator {
 public:
  /**
   * Create a translator for the given plan.
   * @param plan The plan.
   * @param compilation_context The context this translator belongs to.
   * @param pipeline The pipeline this translator is participating in.
   */
  ProjectionTranslator(const planner::ProjectionPlanNode &plan,
                       CompilationContext *compilation_context, Pipeline *pipeline);

  /**
   * Projections do not require any query state.
   */
  void InitializeQueryState() const override {}

  /**
   * Projections do not require any query state. Hence, this method is a no-op.
   */
  void TearDownQueryState() const override {}

  /**
   * Projections do not require any pipeline state. Hence, this method is a no-op.
   */
  void DeclarePipelineState(PipelineContext *pipeline_context) override {}

  /**
   * Projections do not require any pipeline state. Hence, this method is a no-op.
   */
  void InitializePipelineState(const PipelineContext &pipeline_context) const override {}

  /**
   * Projections do not require any pre-pipeline work.
   */
  void BeginPipelineWork(const PipelineContext &pipeline_context) const override {}

  /**
   * Push the context through this operator to the next in the pipeline.
   * @param consumer_context The context.
   */
  void DoPipelineWork(ConsumerContext *consumer_context) const override;

  /**
   * Projections do not require any post-pipeline work.
   */
  void FinishPipelineWork(const PipelineContext &pipeline_context) const override {}

  /**
   * Projections do not require any pipeline state. Hence, this method is a no-op.
   */
  void TearDownPipelineState(const PipelineContext &pipeline_context) const override {}

  /**
   * Pipeline are never pipeline roots. Hence, this should never be called.
   */
  util::RegionVector<ast::FieldDecl *> GetWorkerParams() const override {
    UNREACHABLE("Projections do not launch pipeline");
  }

  /**
   * Pipelines are never pipeline roots. Hence, this should never be called.
   */
  void LaunchWork(ast::Identifier work_func_name) const override {
    UNREACHABLE("Projections do not launch pipelines");
  }

  /**
   * @return The output of the given child's attribute in the provided context.
   */
  ast::Expr *GetChildOutput(ConsumerContext *consumer_context, uint32_t child_idx,
                            uint32_t attr_idx) const override;

  /**
   * Projections do not produce columns from base tables.
   */
  ast::Expr *GetTableColumn(uint16_t col_oid) const override {
    UNREACHABLE("Projections do not produce columns from base tables.");
  }
};

}  // namespace tpl::sql::codegen
