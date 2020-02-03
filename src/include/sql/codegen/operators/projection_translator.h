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
  void InitializeQueryState(FunctionBuilder *) const override {}

  /**
   * Projections do not require any query state. Hence, this method is a no-op.
   */
  void TearDownQueryState(FunctionBuilder *) const override {}

  /**
   * Projections do not require any pipeline state. Hence, this method is a no-op.
   */
  void InitializePipelineState(const Pipeline &, FunctionBuilder *) const override {}

  /**
   * Projections do not require any pre-pipeline work.
   */
  void BeginPipelineWork(const Pipeline &, FunctionBuilder *) const override {}

  /**
   * Push the context through this operator to the next in the pipeline.
   * @param work_context The context.
   */
  void PerformPipelineWork(WorkContext *work_context, FunctionBuilder *function) const override;

  /**
   * Projections do not require any post-pipeline work.
   */
  void FinishPipelineWork(const Pipeline &, FunctionBuilder *) const override {}

  /**
   * Projections do not require any pipeline state. Hence, this method is a no-op.
   */
  void TearDownPipelineState(const Pipeline &, FunctionBuilder *) const override {}

  /**
   * Pipeline are never pipeline roots. Hence, this should never be called.
   */
  util::RegionVector<ast::FieldDecl *> GetWorkerParams() const override {
    UNREACHABLE("Projections do not launch pipeline");
  }

  /**
   * Pipelines are never pipeline roots. Hence, this should never be called.
   */
  void LaunchWork(FunctionBuilder *, ast::Identifier) const override {
    UNREACHABLE("Projections do not launch pipelines");
  }

  /**
   * @return The output of the given child's attribute in the provided context.
   */
  ast::Expr *GetChildOutput(WorkContext *work_context, uint32_t child_idx,
                            uint32_t attr_idx) const override;

  /**
   * Projections do not produce columns from base tables.
   */
  ast::Expr *GetTableColumn(uint16_t col_oid) const override {
    UNREACHABLE("Projections do not produce columns from base tables.");
  }
};

}  // namespace tpl::sql::codegen
