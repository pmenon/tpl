#pragma once

#include <vector>

#include "sql/codegen/ast_fwd.h"
#include "sql/codegen/operators/operator_translator.h"
#include "sql/codegen/pipeline.h"
#include "sql/codegen/state_descriptor.h"

namespace tpl::sql::planner {
class AbstractExpression;
class HashJoinPlanNode;
}  // namespace tpl::sql::planner

namespace tpl::sql::codegen {

class FunctionBuilder;

/**
 * A translator for hash joins.
 */
class HashJoinTranslator : public OperatorTranslator {
 public:
  /**
   * Create a new translator for the given hash join plan. The compilation occurs within the
   * provided compilation context and the operator is participating in the provided pipeline.
   * @param plan The plan.
   * @param compilation_context The context of compilation this translation is occurring in.
   * @param pipeline The pipeline this operator is participating in.
   */
  HashJoinTranslator(const planner::HashJoinPlanNode &plan, CompilationContext *compilation_context,
                     Pipeline *pipeline);

  /**
   * Declare the build-row struct used to materialized tuples from the build side of the join.
   * @param decls The top-level declarations for the query. The build-row struct will be
   *                        registered here after it's been constructed.
   */
  void DefineHelperStructs(util::RegionVector<ast::StructDecl *> *decls) override;

  /**
   * Initialize the global hash table.
   */
  void InitializeQueryState(FunctionBuilder *function) const override;

  /**
   * Tear-down the global hash table.
   */
  void TearDownQueryState(FunctionBuilder *function) const override;

  /**
   * If the pipeline context represents the left pipeline and the left pipeline is parallel, we'll
   * need to initialize the thread-local join hash table we've declared.
   * @param pipeline_context The pipeline context.
   */
  void InitializePipelineState(const Pipeline &pipeline, FunctionBuilder *function) const override;

  /**
   * If the pipeline context represents the left pipeline and the left pipeline is parallel, we'll
   * need to clean up and destroy the thread-local join hash table we've declared.
   * @param pipeline_context The pipeline context.
   */
  void TearDownPipelineState(const Pipeline &pipeline, FunctionBuilder *function) const override;

  /**
   * Hash-joins do not have any pre-pipeline work to do. Thus, this is a no-op.
   */
  void BeginPipelineWork(const Pipeline &pipeline, FunctionBuilder *function) const override {}

  /**
   * Implement main join logic. If the context is coming from the left pipeline, the input tuples
   * are materialized into the join hash table. If the context is coming from the right pipeline,
   * the input tuples are probed in the join hash table.
   * @param ctx The context of the work.
   */
  void PerformPipelineWork(WorkContext *ctx, FunctionBuilder *function) const override;

  /**
   * If the pipeline context represents the left pipeline and the left pipeline is parallel, we'll
   * issue a parallel join hash table construction at this point.
   * @param pipeline_context The pipeline context.
   */
  void FinishPipelineWork(const Pipeline &pipeline, FunctionBuilder *function) const override;

  /**
   * Hash-joins are never the root of a pipeline. Thus, this should never be called.
   */
  util::RegionVector<ast::FieldDecl *> GetWorkerParams() const override {
    UNREACHABLE("Impossible");
  }

  /**
   * Hash-joins are never the root of a pipeline. Thus, this should never be called.
   */
  void LaunchWork(FunctionBuilder *, ast::Identifier) const override { UNREACHABLE("Impossible"); }

  /**
   *
   * @param work_context
   * @param child_idx
   * @param attr_idx
   * @return
   */
  ast::Expr *GetChildOutput(WorkContext *work_context, uint32_t child_idx,
                            uint32_t attr_idx) const override;

  /**
   * Hash-joins do not produce columns from base tables.
   */
  ast::Expr *GetTableColumn(uint16_t col_oid) const override {
    UNREACHABLE("Hash-joins do not produce columns from base tables.");
  }

 private:
  // Return the left pipeline.
  Pipeline *LeftPipeline() { return &left_pipeline_; }
  const Pipeline &LeftPipeline() const { return left_pipeline_; }

  // Return the right pipeline.
  Pipeline *RightPipeline() { return GetPipeline(); }
  const Pipeline &RightPipeline() const { return *GetPipeline(); }

  // Is the given pipeline this join's left pipeline?
  bool IsLeftPipeline(const Pipeline &pipeline) const { return &LeftPipeline() == &pipeline; }

  // Is the given pipeline this join's right pipeline?
  bool IsRightPipeline(const Pipeline &pipeline) const { return &RightPipeline() == &pipeline; }

  // Initialize the given join hash table instance, provided as a *JHT.
  void InitializeJoinHashTable(FunctionBuilder *function, ast::Expr *jht_ptr) const;

  // Clean up and destroy the given join hash table instance, provided as a *JHT.
  void TearDownJoinHashTable(FunctionBuilder *function, ast::Expr *jht_ptr) const;

  // Access an attribute at the given index in the provided build row.
  ast::Expr *GetBuildRowAttribute(ast::Expr *build_row, uint32_t attr_idx) const;

  // Evaluate the provided hash keys in the provided context and return the
  // results in the provided results output vector.
  ast::Expr *HashKeys(WorkContext *ctx, FunctionBuilder *function,
                      const std::vector<const planner::AbstractExpression *> &hash_keys) const;

  // Fill the build row with the columns from the given context.
  void FillBuildRow(WorkContext *ctx, FunctionBuilder *function, ast::Expr *build_row) const;

  // Input the tuple(s) in the provided context into the join hash table.
  void InsertIntoJoinHashTable(WorkContext *ctx, FunctionBuilder *function) const;

  // Probe the join hash table with the input tuple(s).
  void ProbeJoinHashTable(WorkContext *ctx, FunctionBuilder *function) const;

 private:
  // The name of the materialized row when inserting or probing into join hash
  // table.
  ast::Identifier build_row_var_;
  ast::Identifier build_row_type_;

  // The left build-side pipeline.
  Pipeline left_pipeline_;

  // The slots in the global and thread-local state where this join's join hash
  // table is stored.
  StateDescriptor::Entry global_join_ht_;
  StateDescriptor::Entry local_join_ht_;
};

}  // namespace tpl::sql::codegen
