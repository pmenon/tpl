#pragma once

#include <vector>

#include "sql/codegen/edsl/struct.h"
#include "sql/codegen/execution_state.h"
#include "sql/codegen/operators/operator_translator.h"
#include "sql/codegen/pipeline.h"
#include "sql/codegen/pipeline_driver.h"

namespace tpl::sql::planner {
class OrderByPlanNode;
}  // namespace tpl::sql::planner

namespace tpl::sql::codegen {

class FunctionBuilder;

/**
 * A translator for order-by plans.
 */
class SortTranslator : public OperatorTranslator, public PipelineDriver {
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
   * Link the build and produce pipelines.
   */
  void DeclarePipelineDependencies() const override;

  /**
   * Define the structure of the rows that are materialized in the sorter, and the sort function.
   * @param container The container for query-level types and functions.
   */
  void DefineStructsAndFunctions() override;

  /**
   * Initialize the sorter instance.
   */
  void InitializeQueryState(FunctionBuilder *function) const override;

  /**
   * Tear-down the sorter instance.
   */
  void TearDownQueryState(FunctionBuilder *function) const override;

  /**
   * Declare local sorter, if sorting is parallel.
   * @param pipeline_ctx The pipeline context.
   */
  void DeclarePipelineState(PipelineContext *pipeline_ctx) override;

  /**
   * If the given pipeline is for the build-size and is parallel, initialize the thread-local sorter
   * instance we declared inside.
   * @param pipeline_context The pipeline context.
   */
  void InitializePipelineState(const PipelineContext &pipeline_ctx,
                               FunctionBuilder *function) const override;

  /**
   * If the given pipeline is for the build-size and is parallel, destroy the thread-local sorter
   * instance we declared inside.
   * @param pipeline_context The pipeline context.
   */
  void TearDownPipelineState(const PipelineContext &pipeline_ctx,
                             FunctionBuilder *function) const override;

  /**
   * Implement either the build-side or scan-side of the sort depending on the pipeline this context
   * contains.
   * @param context The context of the work.
   */
  void Consume(ConsumerContext *context, FunctionBuilder *function) const override;

  /**
   * If the given pipeline is for the build-side, we'll need to issue a sort. If the pipeline is
   * parallel, we'll issue a parallel sort. If the sort is only for a top-k, we'll also only issue
   * a top-k sort.
   * @param pipeline_context The pipeline context.
   */
  void FinishPipelineWork(const PipelineContext &pipeline_ctx,
                          FunctionBuilder *function) const override;

  /**
   * Produce sorted results.
   * @param pipeline_ctx The pipeline context.
   */
  void DrivePipeline(const PipelineContext &pipeline_ctx) const override;

  /**
   * @return The value (vector) of the attribute at the given index (@em attr_idx) produced by the
   *         child at the given index (@em child_idx).
   */
  edsl::ValueVT GetChildOutput(ConsumerContext *context, uint32_t child_idx,
                               uint32_t attr_idx) const override;

  /**
   * Order-by operators do not produce columns from base tables.
   */
  edsl::ValueVT GetTableColumn(uint16_t col_oid) const override {
    UNREACHABLE("Order-by operators do not produce columns from base tables");
  }

 private:
  // Access the attribute at the given index within the provided sort row.
  edsl::ReferenceVT GetSortRowAttribute(const edsl::ReferenceVT &row_ptr, uint32_t attr_idx) const;

  // Called to scan the global sorter instance.
  void ScanSorter(ConsumerContext *context, FunctionBuilder *function,
                  const edsl::Value<ast::x::Sorter *> &sorter) const;

  // Insert tuple data into the provided sort row.
  void FillSortRow(ConsumerContext *ctx, FunctionBuilder *function) const;

  // Called to insert the tuple in the context into the sorter instance.
  void InsertIntoSorter(ConsumerContext *context, FunctionBuilder *function,
                        const edsl::Value<ast::x::Sorter *> &sorter) const;

  // Generate the struct used to represent the sorting row.
  void GenerateSortRowStructType();

  // Generate the sorting function.
  void GenerateComparisonFunction();
  void GenerateComparisonLogic(FunctionBuilder *function);

 private:
  // The struct representing the attributes in a sort-row.
  edsl::Struct row_struct_;
  // The name of the comparison function.
  ast::Identifier compare_func_;

  // The main sorting row, and the left and right inputs to the sorting function.
  std::unique_ptr<edsl::VariableVT> row_, lhs_row_, rhs_row_;

  // Build-side pipeline.
  Pipeline build_pipeline_;

  // Where the global and thread-local sorter instances are.
  ExecutionState::Slot global_sorter_;
  ExecutionState::Slot local_sorter_;

  enum class CurrentRow { Child, Lhs, Rhs };
  CurrentRow current_row_;
};

}  // namespace tpl::sql::codegen
