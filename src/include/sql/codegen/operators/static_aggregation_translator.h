#pragma once

#include <vector>

#include "sql/codegen/operators/operator_translator.h"
#include "sql/codegen/pipeline.h"

namespace tpl::sql::planner {
class AggregatePlanNode;
}  // namespace tpl::sql::planner

namespace tpl::sql::codegen {

class FunctionBuilder;

class StaticAggregationTranslator : public OperatorTranslator {
 public:
  StaticAggregationTranslator(const planner::AggregatePlanNode &plan,
                              CompilationContext *compilation_context, Pipeline *pipeline);

  void DefineHelperStructs(util::RegionVector<ast::StructDecl *> *decls) override;

  void DefineHelperFunctions(util::RegionVector<ast::FunctionDecl *> *top_level_funcs) override;

  void InitializeQueryState(FunctionBuilder *function) const override {}

  void TearDownQueryState(FunctionBuilder *function) const override {}

  void InitializePipelineState(const Pipeline &pipeline, FunctionBuilder *function) const override;

  void TearDownPipelineState(const Pipeline &pipeline, FunctionBuilder *function) const override {}

  void BeginPipelineWork(const Pipeline &pipeline, FunctionBuilder *function) const override;

  void PerformPipelineWork(WorkContext *work_context, FunctionBuilder *function) const override;

  void FinishPipelineWork(const Pipeline &pipeline, FunctionBuilder *function) const override;

  util::RegionVector<ast::FieldDecl *> GetWorkerParams() const override {
    UNREACHABLE("Static aggregations are never launched in parallel");
  }

  void LaunchWork(FunctionBuilder *function, ast::Identifier work_func_name) const override {
    UNREACHABLE("Static aggregations are never launched in parallel");
  }

  /**
   * @return The value (vector) of the attribute at the given index (@em attr_idx) produced by the
   *         child at the given index (@em child_idx).
   */
  ast::Expr *GetChildOutput(WorkContext *work_context, uint32_t child_idx,
                            uint32_t attr_idx) const override;

  ast::Expr *GetTableColumn(uint16_t col_oid) const override {
    UNREACHABLE("Static aggregations do not produce columns from base tables.");
  }

 private:
  // Access the plan.
  const planner::AggregatePlanNode &GetAggPlan() const {
    return GetPlanAs<planner::AggregatePlanNode>();
  }

  // Check if the input pipeline is either the build-side or producer-side.
  bool IsBuildPipeline(const Pipeline &pipeline) const { return &build_pipeline_ == &pipeline; }
  bool IsProducePipeline(const Pipeline &pipeline) const { return GetPipeline() == &pipeline; }

  ast::Expr *GetAggregateTerm(ast::Expr *agg_row, uint32_t attr_idx) const;
  ast::Expr *GetAggregateTermPtr(ast::Expr *agg_row, uint32_t attr_idx) const;

  void GeneratePayloadStruct(util::RegionVector<ast::StructDecl *> *decls);
  void GenerateValuesStruct(util::RegionVector<ast::StructDecl *> *decls);

  void InitializeAggregates(FunctionBuilder *function, bool local) const;

  void UpdateGlobalAggregate(WorkContext *ctx, FunctionBuilder *function) const;

 private:
  ast::Identifier agg_row_var_;
  ast::Identifier agg_payload_type_;
  ast::Identifier agg_values_type_;

  // The name of the merging function.
  ast::Identifier merge_func_;

  // The build pipeline.
  Pipeline build_pipeline_;

  // States.
  StateDescriptor::Entry global_aggs_;
  StateDescriptor::Entry local_aggs_;
};

}  // namespace tpl::sql::codegen
