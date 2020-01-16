#pragma once

#include <vector>

#include "sql/codegen/operators/operator_translator.h"
#include "sql/codegen/pipeline.h"
#include "sql/codegen/query_state.h"

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
   * @param top_level_decls The top-level declarations.
   */
  void DefineHelperStructs(TopLevelDeclarations *top_level_decls) override;

  /**
   * Define the sorting function.
   * @param top_level_decls The top-level declarations.
   */
  void DefineHelperFunctions(TopLevelDeclarations *top_level_decls) override;

  /**
   * Initialize the sorter instance.
   */
  void InitializeQueryState() const override;

  /**
   * Tear-down the sorter instance.
   */
  void TearDownQueryState() const override;

  /**
   * If the given pipeline is for the build-side and is parallel, a thread-local sorter instance is
   * declared in the pipeline state.
   * @param pipeline_context The pipeline context.
   */
  void DeclarePipelineState(PipelineContext *pipeline_context) override;

  /**
   * If the given pipeline is for the build-size and is parallel, initialize the thread-local sorter
   * instance we declared inside.
   * @param pipeline_context The pipeline context.
   */
  void InitializePipelineState(const PipelineContext &pipeline_context) const override;

  /**
   * If the given pipeline is for the build-size and is parallel, destroy the thread-local sorter
   * instance we declared inside.
   * @param pipeline_context The pipeline context.
   */
  void TearDownPipelineState(const PipelineContext &pipeline_context) const override;

  /**
   * Sorters don't require any pre-pipeline logic.
   * @param pipeline_context The pipeline context.
   */
  void BeginPipelineWork(const PipelineContext &pipeline_context) const override {}

  /**
   * Implement either the build-side or scan-side of the sort depending on the pipeline this context
   * contains.
   * @param consumer_context The consumer context.
   */
  void DoPipelineWork(ConsumerContext *consumer_context) const override;

  /**
   * If the given pipeline is for the build-side, we'll need to issue a sort. If the pipeline is
   * parallel, we'll issue a parallel sort. If the sort is only for a top-k, we'll also only issue
   * a top-k sort.
   * @param pipeline_context The pipeline context.
   */
  void FinishPipelineWork(const PipelineContext &pipeline_context) const override;

  /**
   * Sorters are never launched in parallel, so this should never occur..
   */
  util::RegionVector<ast::FieldDecl *> GetWorkerParams() const override {
    UNREACHABLE("Impossible");
  }

  /**
   * Sorters are never launched in parallel, so this should never occur.
   */
  void LaunchWork(ast::Identifier work_func_name) const override { UNREACHABLE("Impossible"); }

 private:
  const planner::OrderByPlanNode &GetTypedPlan() const {
    return GetPlanAs<planner::OrderByPlanNode>();
  }

  // Return true if the given pipeline is for the bottom/built portion of the sort.
  bool IsBottomPipeline(const Pipeline &pipeline) const { return &child_pipeline_ == &pipeline; }

  // Return true if the given pipeline is for the top/scan portion of the sort.
  bool IsTopPipeline(const Pipeline &pipeline) const { return !IsBottomPipeline(pipeline); }

  // Initialize and destroy the given sorter.
  void InitializeSorter(ast::Expr *sorter_ptr) const;
  void TearDownSorter(ast::Expr *sorter_ptr) const;

  // Called to scan the global sorter instance.
  void ScanSorter(ConsumerContext *consumer_context) const;

  // Insert tuple data into the provided sort row.
  void FillSortRow(ConsumerContext *consumer_context, ast::Expr *sort_row) const;

  // Called to insert the tuple in the context into the sorter instance.
  void InsertIntoSorter(ConsumerContext *consumer_context) const;

  // Generate comparison function.
  void GenerateComparisonFunction(FunctionBuilder *builder, ast::Expr *lhs_row,
                                  ast::Expr *rhs_row) const;

  class SortRowAccess;

  // Create attribute accessors for all attributes for the provided sort row and insert them into
  // the context. The output attributes vector stores the accessors which the context references.
  void PopulateContextWithSortAttributes(ConsumerContext *consumer_context, ast::Expr *sort_row,
                                         std::vector<SortRowAccess> *attrs) const;

 private:
  // Build-side pipeline.
  Pipeline child_pipeline_;

  // Where the global and thread-local sorter instances are.
  QueryState::Slot sorter_slot_;
  PipelineContext::Slot tl_sorter_slot_;

  // The struct representing the row-wise materialization into the sorter
  // instance. Each field in the row represents a column that is sent by the
  // child operator.
  ast::StructDecl *sort_row_;

  // The comparison function.
  ast::FunctionDecl *cmp_func_;
};

}  // namespace tpl::sql::codegen
