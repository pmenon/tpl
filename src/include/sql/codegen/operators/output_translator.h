#pragma once

#include <string_view>
#include <vector>

#include "sql/codegen/ast_fwd.h"
#include "sql/codegen/operators/operator_translator.h"
#include "sql/codegen/pipeline.h"
#include "sql/schema.h"

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
  ast::Expression *GetTableColumn(uint16_t col_oid) const override {
    UNREACHABLE("Output does not interact with tables.");
  }

 private:
  ast::Identifier output_var_;
  ast::Identifier output_struct_;
};

}  // namespace tpl::sql::codegen
