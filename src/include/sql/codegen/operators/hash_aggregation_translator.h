#pragma once

#include <memory>

#include "sql/codegen/compilation_unit.h"
#include "sql/codegen/consumer_context.h"
#include "sql/codegen/edsl/struct.h"
#include "sql/codegen/edsl/value.h"
#include "sql/codegen/edsl/value_vt.h"
#include "sql/codegen/execution_state.h"
#include "sql/codegen/operators/operator_translator.h"
#include "sql/codegen/pipeline.h"
#include "sql/codegen/pipeline_driver.h"

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
  edsl::ValueVT GetChildOutput(ConsumerContext *context, uint32_t child_idx,
                               uint32_t attr_idx) const override;

  /**
   * Hash-based aggregations do not produce columns from base tables.
   */
  edsl::ValueVT GetTableColumn(uint16_t col_oid) const override {
    UNREACHABLE("Hash-based aggregations do not produce columns from base tables.");
  }

 private:
  // Access the plan.
  const planner::AggregatePlanNode &GetAggPlan() const {
    return GetPlanAs<planner::AggregatePlanNode>();
  }

  std::size_t KeyId(std::size_t id) const;
  std::size_t AggId(std::size_t id) const;

  // Declare the payload and input structures. Called from DefineHelperStructs().
  void GeneratePayloadStruct();
  void GenerateInputValuesStruct();

  // Generate the overflow partition merging process.
  void GenerateKeyCheckFunction();
  void GeneratePartialKeyCheckFunction();
  void GenerateMergeOverflowPartitionsFunction();

  // These functions define steps in the "build" phase of the aggregation.
  // 1. Filling input values.
  // 2. Probing aggregation hash table.
  //   2a. Hashing input.
  //   2b. Performing lookup.
  // 3. Initializing new aggregates.
  // 4. Advancing existing aggregates.
  edsl::VariableVT FillInputValues(FunctionBuilder *function, ConsumerContext *context) const;
  edsl::Variable<hash_t> HashInputKeys(FunctionBuilder *function,
                                       const edsl::ValueVT &input_values) const;
  edsl::VariableVT PerformLookup(FunctionBuilder *function,
                                 const edsl::Value<ast::x::AggregationHashTable *> &hash_table,
                                 const edsl::Value<hash_t> &hash_val,
                                 const edsl::ReferenceVT &agg_values) const;
  void ConstructNewAggregate(FunctionBuilder *function,
                             const edsl::Value<ast::x::AggregationHashTable *> &hash_table,
                             const edsl::ReferenceVT &agg_payload,
                             const edsl::ValueVT &input_values,
                             const edsl::Value<hash_t> &hash_val) const;
  void AdvanceAggregate(FunctionBuilder *function, const edsl::ReferenceVT &agg_payload,
                        const edsl::ReferenceVT &input_values) const;

  // Merge the input row into the aggregation hash table.
  void UpdateAggregates(ConsumerContext *context, FunctionBuilder *function,
                        const edsl::Variable<ast::x::AggregationHashTable *> &hash_table) const;

  // Scan the final aggregation hash table.
  void ScanAggregationHashTable(
      ConsumerContext *context, FunctionBuilder *function,
      const edsl::Variable<ast::x::AggregationHashTable *> &hash_table) const;

 private:
  // The name of the variable used to:
  // 1. Materialize an input row and insert into the aggregation hash table.
  // 2. Read from an iterator when iterating over all aggregates.
  std::unique_ptr<edsl::VariableVT> agg_row_;
  // The names of the payload and input values struct.
  edsl::Struct agg_payload_;
  edsl::Struct agg_values_;
  // The names of the full key-check function, the partial key check function
  // and the overflow partition merging functions, respectively.
  ast::Identifier key_check_fn_;
  ast::Identifier key_check_partial_fn_;
  ast::Identifier merge_partitions_fn_;

  // The build pipeline.
  Pipeline build_pipeline_;

  // The global and thread-local aggregation hash tables.
  ExecutionState::Slot<ast::x::AggregationHashTable> global_agg_ht_;
  ExecutionState::Slot<ast::x::AggregationHashTable> local_agg_ht_;
};

}  // namespace tpl::sql::codegen
