#pragma once

#include <vector>

#include "ast/identifier.h"
#include "sql/codegen/operators/operator_translator.h"
#include "sql/codegen/pipeline.h"

namespace tpl::sql::planner {
class SetOpPlanNode;
}  // namespace tpl::sql::planner

namespace tpl::sql::codegen {

/**
 * Translator for union all.
 */
class UnionAllTranslator : public OperatorTranslator {
 public:
  /**
   * Create a translator for UNION ALL.
   * @param plan The plan node.
   * @param compilation_context The compilation context translation occurs in.
   * @param pipeline The pipeline the node belongs to.
   */
  UnionAllTranslator(const planner::SetOpPlanNode &plan, CompilationContext *compilation_context,
                     Pipeline *pipeline);

  /**
   * Declare dependencies between each child pipeline.
   */
  void DeclarePipelineDependencies() const override;

  /**
   * Define any auxiliary functions and structs.
   */
  void DefineStructsAndFunctions() override;

  /**
   * Define the pipeline logic for the parent. This is invoked from each child.
   * @param pipeline The pipeline being generated.
   */
  void DefinePipelineFunctions(const PipelineContext &pipeline_ctx) override;

  /**
   * Consume data from a child.
   * @param context The context of consumption.
   * @param function The function being built.
   */
  void Consume(ConsumerContext *context, FunctionBuilder *function) const override;

  /**
   * @return The value (vector) of the attribute at the given index (@em attr_idx) produced by the
   *         child at the given index (@em child_idx).
   */
  ast::Expression *GetChildOutput(ConsumerContext *context, uint32_t child_idx,
                                  uint32_t attr_idx) const override;

  /**
   * UNION ALL doesn't produce table columns.
   */
  ast::Expression *GetTableColumn(uint16_t col_oid) const override {
    UNREACHABLE("UnionTranslator doesn't read table columns.");
  }

 private:
  void DefineRowStruct();
  // Get the value of an attribute in the given row.
  ast::Expression *GetRowAttribute(ast::Identifier sort_row, uint32_t attr_idx) const;
  // Fill the input row's attributes with values from the provided context
  void FillRow(ConsumerContext *context, FunctionBuilder *function) const;

 private:
  // All child pipelines.
  std::vector<Pipeline> child_pipelines_;
  // The name of the function encapsulating parent plan logic.
  ast::Identifier parent_fn_name_;
  // The name of the struct we materialize.
  ast::Identifier row_type_name_, row_var_;
};

}  // namespace tpl::sql::codegen
