#pragma once

#include "sql/codegen/operators/operator_translator.h"
#include "sql/codegen/pipeline.h"
#include "sql/codegen/state_descriptor.h"
#include "sql/codegen/work_context.h"

namespace tpl::sql::planner {
class AggregatePlanNode;
}  // namespace tpl::sql::planner

namespace tpl::sql::codegen {

class FunctionBuilder;

/**
 * A translator for hash-based aggregations.
 */
class HashAggregationTranslator : public OperatorTranslator {
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
   * Define the aggregation row structure.
   * @param decls Where the defined structure will be registered.
   */
  void DefineHelperStructs(util::RegionVector<ast::StructDecl *> *decls) override;

  /**
   * If the build-pipeline is parallel, we'll need to define the partition-merging function.
   * @param decls Where the defined functions will be registered.
   */
  void DefineHelperFunctions(util::RegionVector<ast::FunctionDecl *> *decls) override;

  /**
   * Initialize the global aggregation hash table.
   */
  void InitializeQueryState() const override;

  /**
   * Destroy the global aggregation hash table.
   */
  void TearDownQueryState() const override;

  /**
   * Declare a thread-local aggregation hash table if the context is for the build and is parallel.
   * @param pipeline_context The pipeline context.
   */
  void DeclarePipelineState(PipelineContext *pipeline_context) override;

  /**
   * Initialize the thread-local aggregation hash table, if needed.
   * @param pipeline_context The pipeline context.
   */
  void InitializePipelineState(const PipelineContext &pipeline_context) const override;

  /**
   * Tear-down and destroy the thread-local aggregation hash table, if needed.
   * @param pipeline_context The pipeline context.
   */
  void TearDownPipelineState(const PipelineContext &pipeline_context) const override;

  /**
   * Hash aggregations don't require any pre-pipeline work.
   */
  void BeginPipelineWork(const PipelineContext &pipeline_context) const override {}

  /**
   * If the context pipeline is for the build-side, we'll aggregate the input into the aggregation
   * hash table. Otherwise, we'll perform a scan over the resulting aggregates in the aggregation
   * hash table.
   * @param work_context The context.
   */
  void PerformPipelineWork(WorkContext *work_context) const override;

  /**
   * If the provided context is for the build pipeline and we're performing a parallel aggregation,
   * then we'll need to move thread-local aggregation hash table partitions into the main
   * aggregation hash table.
   * @param pipeline_context The pipeline context.
   */
  void FinishPipelineWork(const PipelineContext &pipeline_context) const override;

  /**
   * We'll issue a parallel partitioned scan over the aggregation hash table. In this case, the
   * last argument to the worker function will be the aggregation hash table we're scanning.
   * @return The set of additional worker parameters.
   */
  util::RegionVector<ast::FieldDecl *> GetWorkerParams() const override;

  /**
   * If the aggregation is parallelized, we'll launch ara parallel partitioned scan over the
   * aggregation hash table.
   * @param work_func_name The name of the worker function to invoke.
   */
  void LaunchWork(ast::Identifier work_func_name) const override;

  /**
   * @return The output of the child of this aggregation in the given context.
   */
  ast::Expr *GetChildOutput(WorkContext *work_context, uint32_t child_idx,
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

  // Check if the input pipeline is either the build-side or producer-side.
  bool IsBuildPipeline(const Pipeline &pipeline) const { return &build_pipeline_ == &pipeline; }
  bool IsProducePipeline(const Pipeline &pipeline) const { return GetPipeline() == &pipeline; }

  // Declare the payload and input structures. Called from DefineHelperStructs().
  void DefinePayloadStruct(util::RegionVector<ast::StructDecl *> *decls);
  void DefineInputValuesStruct(util::RegionVector<ast::StructDecl *> *decls);

  // Generate the overflow partition merging process.
  void GenerateKeyCheckFunction(util::RegionVector<ast::FunctionDecl *> *decls);
  void GeneratePartialKeyCheckFunction(util::RegionVector<ast::FunctionDecl *> *decls);
  void GenerateMergeOverflowPartitionsFunction(util::RegionVector<ast::FunctionDecl *> *decls);
  void MergeOverflowPartitions(FunctionBuilder *function, ast::Expr *agg_ht, ast::Expr *iter);

  // Initialize and destroy the input aggregation hash table. These are called
  // from InitializeQueryState() and InitializePipelineState().
  void InitializeAggregationHashTable(ast::Expr *agg_ht) const;
  void TearDownAggregationHashTable(ast::Expr *agg_ht) const;

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
  ast::VariableDecl *FillInputValues(FunctionBuilder *function, WorkContext *ctx) const;
  ast::VariableDecl *HashInputKeys(FunctionBuilder *function, ast::Identifier agg_values) const;
  ast::VariableDecl *PerformLookup(FunctionBuilder *function, ast::Expr *agg_ht,
                                   ast::Identifier hash_val, ast::Identifier agg_values) const;
  void ConstructNewAggregate(FunctionBuilder *function, ast::Expr *agg_ht,
                             ast::Identifier agg_payload, ast::Identifier agg_values,
                             ast::Identifier hash_val) const;
  void AdvanceAggregate(FunctionBuilder *function, ast::Identifier agg_payload,
                        ast::Identifier agg_values) const;

  // Merge the input row into the aggregation hash table.
  void UpdateAggregates(WorkContext *work_context, ast::Expr *agg_ht) const;

  // Scan the final aggregation hash table.
  void ScanAggregationHashTable(WorkContext *work_context, ast::Expr *agg_ht) const;

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
  StateDescriptor::Slot global_agg_ht_slot_;
  StateDescriptor::Slot local_agg_ht_slot_;
};

}  // namespace tpl::sql::codegen
