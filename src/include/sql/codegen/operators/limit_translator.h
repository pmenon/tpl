#pragma once

#include <vector>

#include "sql/codegen/ast_fwd.h"
#include "sql/codegen/operators/operator_translator.h"
#include "sql/codegen/pipeline.h"
#include "sql/codegen/state_descriptor.h"

namespace tpl::sql::planner {
class LimitPlanNode;
}  // namespace tpl::sql::planner

namespace tpl::sql::codegen {

class FunctionBuilder;

/**
 * A translator for limits and offsets.
 */
class LimitTranslator : public OperatorTranslator {
 public:
  /**
   * Create a new translator for the given limit plan. The compilation occurs within the
   * provided compilation context and the operator is participating in the provided pipeline.
   * @param plan The plan.
   * @param compilation_context The context of compilation this translation is occurring in.
   * @param pipeline The pipeline this operator is participating in.
   */
  LimitTranslator(const planner::LimitPlanNode &plan, CompilationContext *compilation_context,
                  Pipeline *pipeline);

  /**
   * Limits only have pipeline-local state, so don't require any query-state initialization.
   * @param function The query-state initialization function.
   */
  void InitializeQueryState(FunctionBuilder *function) const override {}

  /**
   * Limits only have pipeline-local state, so don't require any query-state destruction.
   * @param function The query-state destruction function.
   */
  void TearDownQueryState(FunctionBuilder *function) const override {}

  /**
   * Initialize the tuple counter in the pipeline local state.
   * @param pipeline The pipeline that's being generated.
   * @param function The pipeline function generator.
   */
  void InitializePipelineState(const Pipeline &pipeline, FunctionBuilder *function) const override;

  /**
   * Limits don't require any pre-pipeline work.
   * @param pipeline The pipeline that's being generated.
   * @param function The pipeline function generator.
   */
  void BeginPipelineWork(const Pipeline &pipeline, FunctionBuilder *function) const override {}

  /**
   * Implement the limit's logic.
   * @param work_context The context of work.
   * @param function The pipeline function generator.
   */
  void PerformPipelineWork(WorkContext *work_context, FunctionBuilder *function) const override;

  /**
   * Limits don't require any post-pipeline work.
   * @param pipeline The pipeline that's being generated.
   * @param function The pipeline function generator.
   */
  void FinishPipelineWork(const Pipeline &pipeline, FunctionBuilder *function) const override {}

  /**
   * Limits don't require any pipeline-state tear-down logic.
   * @param pipeline The pipeline that's being generated.
   * @param function The pipeline-state destruction function.
   */
  void TearDownPipelineState(const Pipeline &pipeline, FunctionBuilder *function) const override {}

  /**
   * Limits are never the root of a pipeline.
   */
  util::RegionVector<ast::FieldDecl *> GetWorkerParams() const override {
    UNREACHABLE("LIMITs are never the root of a pipeline.");
  }

  /**
   * Limits are never the root of a pipeline.
   */
  void LaunchWork(FunctionBuilder *function, ast::Identifier work_func_name) const override {
    UNREACHABLE("LIMITs are never the root of a plan and, thus, cannot be launched in parallel.");
  }

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
   * Limits never touch raw table data.
   */
  ast::Expr *GetTableColumn(uint16_t col_oid) const override {
    UNREACHABLE("NLJ are never the root of a plan and, thus, cannot be launched in parallel.");
  }

 private:
  // The tuple counter.
  StateDescriptor::Entry tuple_count_;
};

}  // namespace tpl::sql::codegen
