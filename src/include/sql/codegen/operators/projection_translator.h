#pragma once

#include "sql/codegen/operators/operator_translator.h"
#include "sql/codegen/pipeline_driver.h"

namespace tpl::sql::planner {
class ProjectionPlanNode;
}  // namespace tpl::sql::planner

namespace tpl::sql::codegen {

/**
 * Translator for projections.
 */
class ProjectionTranslator : public OperatorTranslator, PipelineDriver {
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
   * Push the context through this operator to the next in the pipeline.
   * @param context The context.
   */
  void Consume(ConsumerContext *context, FunctionBuilder *function) const override;

  /**
   * Projections only drive the pipeline if they have no children.
   * @param pipeline_ctx The pipeline.
   */
  void DrivePipeline(const PipelineContext &pipeline_ctx) const override;

 public:
  /**
   * Projections do not produce columns from base tables.
   */
  ast::Expr *GetTableColumn(uint16_t col_oid) const override {
    UNREACHABLE("Projections do not produce columns from base tables.");
  }
};

}  // namespace tpl::sql::codegen
