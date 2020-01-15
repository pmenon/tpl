#pragma once

#include <string_view>

#include "sql/codegen/ast_fwd.h"
#include "sql/codegen/operators/operator_translator.h"

namespace tpl::sql::planner {
class SeqScanPlanNode;
}  // namespace tpl::sql::planner

namespace tpl::sql::codegen {

/**
 * A translator for sequential table scans.
 */
class SeqScanTranslator : public OperatorTranslator {
 public:
  /**
   * Create a translator for the given plan.
   * @param plan The plan.
   * @param compilation_context The context this translator belongs to.
   * @param pipeline The pipeline this translator is participating in.
   */
  SeqScanTranslator(const planner::SeqScanPlanNode &plan, CompilationContext *compilation_context,
                    Pipeline *pipeline);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(SeqScanTranslator);

  /**
   * Sequential scans don't have any state.
   */
  void InitializeQueryState() const override {}

  /**
   * Sequential scans don't have any state.
   */
  void TearDownQueryState() const override {}

  /**
   * Sequential scans don't create any pipeline state.
   */
  void DeclarePipelineState(PipelineContext *pipeline_context) override {}

  /**
   * Sequential scans don't create any pipeline state.
   */
  void InitializePipelineState(const PipelineContext &pipeline_context) const override {}

  /**
   * Sequential scans don't require any pre-pipeline logic.
   */
  void BeginPipelineWork(const PipelineContext &pipeline_context) const override {}

  /**
   * Generate the scan.
   * @param ctx The consumer context.
   */
  void DoPipelineWork(ConsumerContext *ctx) const override;

  /**
   * @return The pipeline work function parameters. Just the *TVI.
   */
  util::RegionVector<ast::FieldDecl *> GetWorkerParams() const override;

  /**
   * Launch a parallel table scan.
   * @param work_func_name The worker function that'll be called during the parallel scan.
   */
  void LaunchWork(ast::Identifier work_func_name) const override;

  /**
   * Sequential scans don't rely on any post-pipeline logic.
   */
  void FinishPipelineWork(const PipelineContext &pipeline_context) const override {}

  /**
   * Sequential scans don't create any pipeline state.
   */
  void TearDownPipelineState(const PipelineContext &pipeline_context) const override {}

 private:
  // Get the name of the table being scanned.
  std::string_view GetTableName() const;

  // Perform a table scan using the provided table vector iterator pointer.
  void DoScanTable(ConsumerContext *ctx, ast::Expr *tvi) const;
};

}  // namespace tpl::sql::codegen
