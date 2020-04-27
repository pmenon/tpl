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
   * Push the context through this operator to the next in the pipeline.
   * @param context The context.
   */
  void PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const override;

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
   * Projections do not produce columns from base tables.
   */
  ast::Expr *GetTableColumn(uint16_t col_oid) const override {
    UNREACHABLE("Projections do not produce columns from base tables.");
  }
};

}  // namespace tpl::sql::codegen
