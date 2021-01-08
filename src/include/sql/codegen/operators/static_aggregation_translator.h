#pragma once

#include <memory>
#include <vector>

#include "sql/codegen/edsl/struct.h"
#include "sql/codegen/edsl/value.h"
#include "sql/codegen/edsl/value_vt.h"
#include "sql/codegen/operators/operator_translator.h"
#include "sql/codegen/pipeline.h"
#include "sql/codegen/pipeline_driver.h"

namespace tpl::sql::planner {
class AggregatePlanNode;
}  // namespace tpl::sql::planner

namespace tpl::sql::codegen {

class FunctionBuilder;

/**
 * A translator for static aggregations.
 */
class StaticAggregationTranslator : public OperatorTranslator, public PipelineDriver {
 public:
  /**
   * Create a new translator for the given static aggregation  plan. The translator occurs within
   * the provided compilation context, and the operator is a step in the provided pipeline.
   * @param plan The plan.
   * @param compilation_context The context of compilation this translation is occurring in.
   * @param pipeline The pipeline this operator is participating in.
   */
  StaticAggregationTranslator(const planner::AggregatePlanNode &plan,
                              CompilationContext *compilation_context, Pipeline *pipeline);

  /**
   * Link the build and produce pipelines.
   */
  void DeclarePipelineDependencies() const override;

  /**
   * Define the structure of the aggregates. When parallel, generate the partial merging function.
   * @param container The container for query-level types and functions.
   */
  void DefineStructsAndFunctions() override;

  /**
   * Initialize the aggregates.
   * @param function The function being built.
   */
  void InitializeQueryState(FunctionBuilder *function) const override;

  /**
   * Declare local static aggregate, if parallel.
   * @param pipeline_ctx The pipeline context.
   */
  void DeclarePipelineState(PipelineContext *pipeline_ctx) override;

  /**
   * Define the aggregate merging logic, if the aggregation is parallel.
   * @param pipeline The pipeline.
   */
  void DefinePipelineFunctions(const PipelineContext &pipeline_ctx) override;

  /**
   * If the provided pipeline is the build-side, initialize the declare partial aggregate.
   * @param pipeline The pipeline whose state is being initialized.
   * @param function The function being built.
   */
  void InitializePipelineState(const PipelineContext &pipeline_ctx,
                               FunctionBuilder *function) const override;

  /**
   * Main aggregation logic.
   * @param context The context of the work.
   * @param function The function being built.
   */
  void Consume(ConsumerContext *context, FunctionBuilder *function) const override;

  /**
   * Finish the provided pipeline.
   * @param pipeline The pipeline whose post-work logic is being generated.
   * @param function The function being built.
   */
  void FinishPipelineWork(const PipelineContext &pipeline_ctx,
                          FunctionBuilder *function) const override;

  /**
   * Produce static global aggregates.
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
   * Static aggregations do not touch base table columns.
   */
  edsl::ValueVT GetTableColumn(uint16_t col_oid) const override {
    UNREACHABLE("Static aggregations do not produce columns from base tables.");
  }

 private:
  // Access the plan.
  const planner::AggregatePlanNode &GetAggPlan() const {
    return GetPlanAs<planner::AggregatePlanNode>();
  }

  void GeneratePayloadStruct();

  void InitializeAggregates(FunctionBuilder *fn, const edsl::ReferenceVT &agg) const;

  void UpdateAggregate(ConsumerContext *ctx, FunctionBuilder *fn,
                       const edsl::ReferenceVT &agg) const;

  void ProduceAggregates(ConsumerContext *ctx, FunctionBuilder *fn) const;

  void GenerateAggregateMergeFunction(const PipelineContext &pipeline_ctx) const;

 private:
  // A variable storing a pointer to an aggregation payload struct.
  // Used across functions, so stored here for convenience.
  std::unique_ptr<edsl::VariableVT> agg_row_;
  // The structure storing aggregation keys and values.
  edsl::Struct payload_struct_;
  // The name of the merging function.
  ast::Identifier merge_func_;
  // The build pipeline.
  Pipeline build_pipeline_;
  // States.
  ExecutionState::RTSlot global_aggs_;
  ExecutionState::RTSlot local_aggs_;
};

}  // namespace tpl::sql::codegen
