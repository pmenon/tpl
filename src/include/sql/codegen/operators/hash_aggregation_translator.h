#pragma once

#include "sql/codegen/compilation_unit.h"
#include "sql/codegen/consumer_context.h"
#include "sql/codegen/operators/operator_translator.h"
#include "sql/codegen/pipeline.h"
#include "sql/codegen/pipeline_driver.h"
#include "sql/codegen/state_descriptor.h"

namespace tpl::sql::planner {
class AggregatePlanNode;
}  // namespace tpl::sql::planner

namespace tpl::sql::codegen {

class FunctionBuilder;

/**
 * A translator for hash-based aggregations.
 */
class HashAggregationTranslator : public OperatorTranslator, public PipelineDriver {
 public:
  /**
   * Create a new translator for the given aggregation plan.
   * @param plan The plan.
   * @param compilation_context The context of compilation this translation is occurring in.
   * @param pipeline The pipeline this operator is participating in.
   */
  HashAggregationTranslator(const planner::AggregatePlanNode &plan,
                            CompilationContext *compilation_context, Pipeline *pipeline);

  /**
   * Link the build and produce pipelines.
   */
  void DeclarePipelineDependencies() const override;

  /**
   * Define the aggregation row structure, and all key-check functions.
   * @param container The container for query-level types and functions.
   */
  void DefineStructsAndFunctions() override;

  /**
   * Initialize the global aggregation hash table.
   */
  void InitializeQueryState(FunctionBuilder *function) const override;

  /**
   * Destroy the global aggregation hash table.
   */
  void TearDownQueryState(FunctionBuilder *function) const override;

  /**
   * Define the key equality functions and merging functions, if the aggregation is parallel.
   * @param pipeline The pipeline.
   */
  void DefinePipelineFunctions(const PipelineContext &pipeline_ctx) override;

  /**
   * Declare the pipeline-local aggregation hash table, if the pipeline is parallel.
   * @param pipeline_ctx The pipeline context.
   */
  void DeclarePipelineState(PipelineContext *pipeline_ctx) override;

  /**
   * Initialize the thread-local aggregation hash table, if needed.
   * @param pipeline_context The pipeline context.
   */
  void InitializePipelineState(const PipelineContext &pipeline_ctx,
                               FunctionBuilder *function) const override;

  /**
   * Tear-down and destroy the thread-local aggregation hash table, if needed.
   * @param pipeline_context The pipeline context.
   */
  void TearDownPipelineState(const PipelineContext &pipeline_ctx,
                             FunctionBuilder *function) const override;

  /**
   * If the context pipeline is for the build-side, we'll aggregate the input into the aggregation
   * hash table. Otherwise, we'll perform a scan over the resulting aggregates in the aggregation
   * hash table.
   * @param context The context.
   */
  void Consume(ConsumerContext *context, FunctionBuilder *function) const override;

  /**
   * If the provided context is for the build pipeline and we're performing a parallel aggregation,
   * then we'll need to move thread-local aggregation hash table partitions into the main
   * aggregation hash table.
   * @param pipeline_context The pipeline context.
   */
  void FinishPipelineWork(const PipelineContext &pipeline_ctx,
                          FunctionBuilder *function) const override;

  /**
   * Launch the pipeline to produce the aggregates.
   * @param pipeline_ctx The pipeline context.
   */
  void DrivePipeline(const PipelineContext &pipeline_ctx) const override;

  /**
   * @return The value (vector) of the attribute at the given index (@em attr_idx) produced by the
   *         child at the given index (@em child_idx).
   */
  ast::Expr *GetChildOutput(ConsumerContext *context, uint32_t child_idx,
                            uint32_t attr_idx) const override;

  /**
   * Hash-based aggregations do not produce columns from base tables.
   */
  ast::Expr *GetTableColumn(uint16_t col_oid) const override {
    UNREACHABLE("Hash-based aggregations do not produce columns from base tables.");
  }

 private:
  // Access the plan.
  const planner::AggregatePlanNode &GetAggPlan() const {
    return GetPlanAs<planner::AggregatePlanNode>();
  }

  // Declare the payload and input structures. Called from DefineHelperStructs().
  ast::StructDecl *GeneratePayloadStruct();
  ast::StructDecl *GenerateInputValuesStruct();

  // Generate the overflow partition merging process.
  ast::FunctionDecl *GenerateKeyCheckFunction();
  ast::FunctionDecl *GeneratePartialKeyCheckFunction();
  ast::FunctionDecl *GenerateMergeOverflowPartitionsFunction();
  template <typename F1, typename F2>
  void MergeOverflowPartitions(FunctionBuilder *function, F1 hash_table_provider, F2 iter_provider);

  // Initialize and destroy the input aggregation hash table. These are called
  // from InitializeQueryState() and InitializePipelineState().
  void InitializeAggregationHashTable(FunctionBuilder *function, ast::Expr *agg_ht) const;
  void TearDownAggregationHashTable(FunctionBuilder *function, ast::Expr *agg_ht) const;

  // Access an attribute at the given index in the provided aggregate row.
  ast::Expr *GetGroupByTerm(ast::Identifier agg_row, uint32_t attr_idx) const;
  ast::Expr *GetAggregateTerm(ast::Identifier agg_row, uint32_t attr_idx) const;
  ast::Expr *GetAggregateTermPtr(ast::Identifier agg_row, uint32_t attr_idx) const;

  // These functions define steps in the "build" phase of the aggregation.
  // 1. Filling input values.
  // 2. Probing aggregation hash table.
  //   2a. Hashing input.
  //   2b. Performing lookup.
  // 3. Initializing new aggregates.
  // 4. Advancing existing aggregates.
  ast::Identifier FillInputValues(FunctionBuilder *function, ConsumerContext *context) const;
  ast::Identifier HashInputKeys(FunctionBuilder *function, ast::Identifier agg_values) const;
  template <typename F>
  ast::Identifier PerformLookup(FunctionBuilder *function, F agg_ht_provider,
                                ast::Identifier hash_val, ast::Identifier agg_values) const;
  template <typename F>
  void ConstructNewAggregate(FunctionBuilder *function, F agg_ht_provider,
                             ast::Identifier agg_payload, ast::Identifier agg_values,
                             ast::Identifier hash_val) const;
  void AdvanceAggregate(FunctionBuilder *function, ast::Identifier agg_payload,
                        ast::Identifier agg_values) const;

  // Merge the input row into the aggregation hash table.
  template <typename F>
  void UpdateAggregates(ConsumerContext *context, FunctionBuilder *function,
                        F agg_ht_provider) const;

  // Scan the final aggregation hash table.
  template <typename F>
  void ScanAggregationHashTable(ConsumerContext *context, FunctionBuilder *function,
                                F agg_ht_provider) const;

 private:
  // The name of the variable used to:
  // 1. Materialize an input row and insert into the aggregation hash table.
  // 2. Read from an iterator when iterating over all aggregates.
  ast::Identifier agg_row_var_;
  // The names of the payload and input values struct.
  ast::Identifier agg_payload_type_;
  ast::Identifier agg_values_type_;
  // The names of the full key-check function, the partial key check function
  // and the overflow partition merging functions, respectively.
  ast::Identifier key_check_fn_;
  ast::Identifier key_check_partial_fn_;
  ast::Identifier merge_partitions_fn_;

  // The build pipeline.
  Pipeline build_pipeline_;

  // The global and thread-local aggregation hash tables.
  StateDescriptor::Slot global_agg_ht_;
  StateDescriptor::Slot local_agg_ht_;
};

}  // namespace tpl::sql::codegen
