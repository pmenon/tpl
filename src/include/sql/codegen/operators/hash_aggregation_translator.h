#pragma once

#include "sql/codegen/consumer_context.h"
#include "sql/codegen/operators/operator_translator.h"
#include "sql/codegen/pipeline.h"
#include "sql/codegen/query_state.h"

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
   * @param top_level_decls Where the defined structure will be registered.
   */
  void DefineHelperStructs(TopLevelDeclarations *top_level_decls) override;

  /**
   * If the build-pipeline is parallel, we'll need to define the partition-merging function.
   * @param top_level_decls Where the defined functions will be registered.
   */
  void DefineHelperFunctions(TopLevelDeclarations *top_level_decls) override;

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
   * @param consumer_context The context.
   */
  void DoPipelineWork(ConsumerContext *consumer_context) const override;

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
  ast::Expr *GetChildOutput(ConsumerContext *consumer_context, uint32_t child_idx,
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
  void DefinePayloadStruct(TopLevelDeclarations *top_level_decls);
  void DefineInputValuesStruct(TopLevelDeclarations *top_level_decls);

  // Generate the overflow partition merging process.
  void GenerateKeyCheckFunction(TopLevelDeclarations *top_level_decls);
  void GeneratePartialKeyCheckFunction(TopLevelDeclarations *top_level_decls);
  void GenerateMergeOverflowPartitionsFunction(TopLevelDeclarations *top_level_decls);
  void MergeOverflowPartitions(FunctionBuilder *function);

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
  ast::VariableDecl *FillInputValues(FunctionBuilder *function, ConsumerContext *ctx) const;
  ast::VariableDecl *HashInputKeys(FunctionBuilder *function, ast::Identifier agg_values) const;
  ast::VariableDecl *PerformLookup(FunctionBuilder *function, ast::Expr *agg_ht,
                                   ast::Identifier hash_val, ast::Identifier agg_values) const;
  void ConstructNewAggregate(FunctionBuilder *function, ast::Expr *agg_ht,
                             ast::Identifier agg_payload, ast::Identifier agg_values,
                             ast::Identifier hash_val) const;
  void AdvanceAggregate(FunctionBuilder *function, ast::Identifier agg_payload,
                        ast::Identifier agg_values) const;

  // Merge the input row into the aggregation hash table.
  void UpdateAggregates(ConsumerContext *consumer_context, ast::Expr *agg_ht) const;

  // Scan the final aggregation hash table.
  void ScanAggregationHashTable(ConsumerContext *consumer_context, ast::Expr *agg_ht) const;

 private:
  // The name of the variable used to:
  // 1. Materialize an input row and insert into the aggregation hash table.
  // 2. Read from an iterator when iterating over all aggregates.
  ast::Identifier agg_row_var_name_;

  // The build pipeline.
  Pipeline build_pipeline_;

  // The declaration of the struct used as the payload in the aggregation table.
  ast::StructDecl *agg_payload_;
  // The declaration of the struct used to materialize input before merging into
  // the aggregation hash table.
  ast::StructDecl *agg_values_;

  // The function that checks the equality of keys.
  ast::FunctionDecl *key_check_fn_;

  // The function that performs a partial key check between two partial
  // aggregate payload entries. This is only generated and used in parallel
  // aggregations.
  ast::FunctionDecl *key_check_partial_fn_;

  // The function that merges overflow partitions into a primary aggregation
  // hash table. This is only generated and used in parallel aggregations.
  ast::FunctionDecl *merge_partitions_fn_;

  // The global and thread-local aggregation hash tables.
  QueryState::Slot agg_ht_slot_;
  PipelineContext::Slot tl_agg_ht_slot_;
};

}  // namespace tpl::sql::codegen
