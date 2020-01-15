#pragma once

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

  void DefineHelperStructs(TopLevelDeclarations *top_level_decls) override;
  void DefineHelperFunctions(TopLevelDeclarations *top_level_decls) override;
  void InitializeQueryState() const override;
  void TearDownQueryState() const override;
  void DeclarePipelineState(PipelineContext *pipeline_context) override;
  void InitializePipelineState(const PipelineContext &pipeline_context) const override {}
  void TearDownPipelineState(const PipelineContext &pipeline_context) const override {}
  void BeginPipelineWork(const PipelineContext &pipeline_context) const override {}
  void DoPipelineWork(ConsumerContext *ctx) const override;
  void FinishPipelineWork(const PipelineContext &pipeline_context) const override;
  util::RegionVector<ast::FieldDecl *> GetWorkerParams() const override;
  void LaunchWork(ast::Identifier work_func_name) const override {}

 private:
  const planner::OrderByPlanNode &GetTypedPlan() const {
    return GetPlanAs<planner::OrderByPlanNode>();
  }

  // Return true if the given pipeline is for the bottom/built portion of the sort.
  bool IsBottomPipeline(const Pipeline &pipeline) const { return &child_pipeline_ == &pipeline; }

  // Return true if the given pipeline is for the top/scan portion of the sort.
  bool IsTopPipeline(const Pipeline &pipeline) const { return !IsBottomPipeline(pipeline); }

  // Called to scan the global sorter instance.
  void ScanSorter(ConsumerContext *ctx) const;

  // Called to insert the tuple in the context into the sorter instance.
  void InsertIntoSorter(ConsumerContext *ctx) const;

  // Generate comparison function.
  void GenerateComparisonFunction(FunctionBuilder *builder, ast::Expr *lhs_row,
                                  ast::Expr *rhs_row) const;

 private:
  // Build-side pipeline.
  Pipeline child_pipeline_;

  // Where the global and thread-local sorter instances are.
  QueryState::Slot sorter_;
  PipelineContext::Slot tl_sorter_;

  // The struct representing the row-wise materialization into the sorter
  // instance. Each field in the row represents a column that is sent by the
  // child operator.
  ast::StructDecl *sort_row_;

  // The comparison function.
  ast::FunctionDecl *cmp_func_;
};

}  // namespace tpl::sql::codegen
