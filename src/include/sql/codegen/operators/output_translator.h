#pragma once

#include <memory>

#include "sql/codegen/edsl/struct.h"
#include "sql/codegen/edsl/value_vt.h"
#include "sql/codegen/operators/operator_translator.h"
#include "sql/codegen/pipeline.h"

namespace tpl::sql::planner {
class AbstractExpression;
}  // namespace tpl::sql::planner

namespace tpl::sql::codegen {

class FunctionBuilder;

/**
 * A translator for outputting tuples.
 */
class OutputTranslator : public OperatorTranslator {
 public:
  /**
   * Create a translator for the given plan.
   * @param plan The plan.
   * @param compilation_context The context this translator belongs to.
   * @param pipeline The pipeline this translator is participating in.
   */
  OutputTranslator(const planner::AbstractPlanNode &plan, CompilationContext *compilation_context,
                   Pipeline *pipeline);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(OutputTranslator);

  /**
   * Define the output struct.
   * @param container The container for query-level types and functions.
   */
  void DefineStructsAndFunctions() override;

  /**
   * Perform the main work of the translator.
   */
  void Consume(ConsumerContext *context, FunctionBuilder *function) const override;

  /**
   * Output translator needs to finalize the output.
   */
  void FinishPipelineWork(const PipelineContext &pipeline_ctx,
                          FunctionBuilder *function) const override;

  /**
   * Does not interact with tables.
   */
  edsl::ValueVT GetTableColumn(uint16_t col_oid) const override {
    UNREACHABLE("Output does not interact with tables.");
  }

 private:
  // The output struct.
  edsl::Struct out_struct_;
  // The row whose type is that of the output structure above.
  std::unique_ptr<edsl::VariableVT> out_row_;
};

}  // namespace tpl::sql::codegen
