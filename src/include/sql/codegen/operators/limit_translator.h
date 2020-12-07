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

  void DeclarePipelineState(PipelineContext *pipeline_ctx) override;
  /**
   * Initialize the tuple counter in the pipeline local state.
   * @param pipeline The pipeline that's being generated.
   * @param function The pipeline function generator.
   */
  void InitializePipelineState(const PipelineContext &pipeline_ctx,
                               FunctionBuilder *function) const override;

  /**
   * Implement the limit's logic.
   * @param context The context of work.
   * @param function The pipeline function generator.
   */
  void Consume(ConsumerContext *context, FunctionBuilder *function) const override;

  /**
   * Limits never touch raw table data.
   */
  ast::Expression *GetTableColumn(uint16_t col_oid) const override {
    UNREACHABLE("LIMITs do not touch base table columns.");
  }

 private:
  // The tuple counter.
  StateDescriptor::Slot tuple_count_;
};

}  // namespace tpl::sql::codegen
