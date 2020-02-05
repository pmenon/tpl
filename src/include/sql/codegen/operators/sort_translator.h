#pragma once

#include <vector>

#include "sql/codegen/operators/operator_translator.h"
#include "sql/codegen/pipeline.h"
#include "sql/codegen/state_descriptor.h"

namespace tpl::sql::planner {
class OrderByPlanNode;
}  // namespace tpl::sql::planner

namespace tpl::sql::codegen {

class FunctionBuilder;

/**
 * A translator for order-by plans.
 */
class SortTranslator : public OperatorTranslator {
 public:
  /**
   * Create a translator for the given order-by plan node.
   * @param plan The plan.
   * @param compilation_context The context this translator belongs to.
   * @param pipeline The pipeline this translator is participating in.
   */
  SortTranslator(const planner::OrderByPlanNode &plan, CompilationContext *compilation_context,
                 Pipeline *pipeline);

  /**
   * Define the sort-row structure that's materialized in the sorter.
   * @param decls The top-level declarations.
   */
  void DefineHelperStructs(util::RegionVector<ast::StructDecl *> *decls) override;

  /**
   * Define the sorting function.
   * @param decls The top-level declarations.
   */
  void DefineHelperFunctions(util::RegionVector<ast::FunctionDecl *> *decls) override;

  /**
   * Initialize the sorter instance.
   */
  void InitializeQueryState(FunctionBuilder *function) const override;

  /**
   * Tear-down the sorter instance.
   */
  void TearDownQueryState(FunctionBuilder *function) const override;

  /**
   * If the given pipeline is for the build-size and is parallel, initialize the thread-local sorter
   * instance we declared inside.
   * @param pipeline_context The pipeline context.
   */
  void InitializePipelineState(const Pipeline &pipeline, FunctionBuilder *function) const override;

  /**
   * If the given pipeline is for the build-size and is parallel, destroy the thread-local sorter
   * instance we declared inside.
   * @param pipeline_context The pipeline context.
   */
  void TearDownPipelineState(const Pipeline &pipeline, FunctionBuilder *function) const override;

  /**
   * Sorters don't require any pre-pipeline logic.
   * @param pipeline_context The pipeline context.
   */
  void BeginPipelineWork(const Pipeline &pipeline, FunctionBuilder *function) const override {}

  /**
   * Implement either the build-side or scan-side of the sort depending on the pipeline this context
   * contains.
   * @param ctx The context of the work.
   */
  void PerformPipelineWork(WorkContext *ctx, FunctionBuilder *function) const override;

  /**
   * If the given pipeline is for the build-side, we'll need to issue a sort. If the pipeline is
   * parallel, we'll issue a parallel sort. If the sort is only for a top-k, we'll also only issue
   * a top-k sort.
   * @param pipeline_context The pipeline context.
   */
  void FinishPipelineWork(const Pipeline &pipeline, FunctionBuilder *function) const override;

  /**
   * Sorters are never launched in parallel, so this should never occur..
   */
  util::RegionVector<ast::FieldDecl *> GetWorkerParams() const override {
    UNREACHABLE("Impossible");
  }

  /**
   * Sorters are never launched in parallel, so this should never occur.
   */
  void LaunchWork(FunctionBuilder *function, ast::Identifier work_func_name) const override {
    UNREACHABLE("Impossible");
  }

  /**
   * @return
   */
  ast::Expr *GetChildOutput(WorkContext *work_context, uint32_t child_idx,
                            uint32_t attr_idx) const override;

  /**
   * Order-by operators do not produce columns from base tables.
   */
  ast::Expr *GetTableColumn(uint16_t col_oid) const override {
    UNREACHABLE("Order-by operators do not produce columns from base tables");
  }

 private:
  // Get the bottom/build pipeline.
  Pipeline *GetBuildPipeline() { return &child_pipeline_; }
  const Pipeline &GetBuildPipeline() const { return child_pipeline_; }

  // Get the top/scan pipeline.
  Pipeline *GetScanPipeline() { return GetPipeline(); }
  const Pipeline &GetScanPipeline() const { return *GetPipeline(); }

  // Check if the given pipelines are t
  bool IsBuildPipeline(const Pipeline &pipeline) const { return &GetBuildPipeline() == &pipeline; }
  bool IsScanPipeline(const Pipeline &pipeline) const { return &GetScanPipeline() == &pipeline; }

  // Initialize and destroy the given sorter.
  void InitializeSorter(FunctionBuilder *function, ast::Expr *sorter_ptr) const;
  void TearDownSorter(FunctionBuilder *function, ast::Expr *sorter_ptr) const;

  // Access the attribute at the given index within the provided sort row.
  ast::Expr *GetSortRowAttribute(ast::Identifier sort_row, uint32_t attr_idx) const;

  // Called to scan the global sorter instance.
  void ScanSorter(WorkContext *ctx, FunctionBuilder *function) const;

  // Insert tuple data into the provided sort row.
  void FillSortRow(WorkContext *ctx, FunctionBuilder *function) const;

  // Called to insert the tuple in the context into the sorter instance.
  void InsertIntoSorter(WorkContext *ctx, FunctionBuilder *function) const;

  // Generate comparison function.
  void GenerateComparisonFunction(FunctionBuilder *function);

 private:
  // The name of the materialized sort row when inserting into sorter or pulling
  // from an iterator.
  ast::Identifier sort_row_var_;
  ast::Identifier sort_row_type_;
  ast::Identifier lhs_row_, rhs_row_;
  ast::Identifier compare_func_;

  // Build-side pipeline.
  Pipeline child_pipeline_;

  // Where the global and thread-local sorter instances are.
  StateDescriptor::Entry global_sorter_;
  StateDescriptor::Entry local_sorter_;

  enum class CurrentRow { Child, Lhs, Rhs };
  CurrentRow current_row_;
};

}  // namespace tpl::sql::codegen
