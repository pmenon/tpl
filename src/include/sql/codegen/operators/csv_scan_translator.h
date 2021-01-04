#pragma once

#include "sql/codegen/operators/operator_translator.h"
#include "sql/codegen/pipeline_driver.h"

namespace tpl::sql::planner {
class CSVScanPlanNode;
}  // namespace tpl::sql::planner

namespace tpl::sql::codegen {

class FunctionBuilder;

class CSVScanTranslator : public OperatorTranslator, public PipelineDriver {
 public:
  /**
   * Create a new translator for the given scan plan.
   * @param plan The plan.
   * @param compilation_context The context of compilation this translation is occurring in.
   * @param pipeline The pipeline this operator is participating in.
   */
  CSVScanTranslator(const planner::CSVScanPlanNode &plan, CompilationContext *compilation_context,
                    Pipeline *pipeline);

  /**
   * Define the structure representing the rows produced by this CSV scan.
   * @param container The container for query-level types and functions.
   */
  void DefineStructsAndFunctions() override;

  /**
   * Declare the reader.
   * @param pipeline_ctx The pipeline context.
   */
  void DeclarePipelineState(PipelineContext *pipeline_ctx) override;

  /**
   * Initialize the reader.
   * @param pipeline_ctx The pipeline context.
   * @param function The function being built.
   */
  void InitializePipelineState(const PipelineContext &pipeline_ctx,
                               FunctionBuilder *function) const override;

  /**
   * Destroy the reader.
   * @param pipeline_ctx The pipeline context.
   * @param function The function being built.
   */
  void TearDownPipelineState(const PipelineContext &pipeline_ctx,
                             FunctionBuilder *function) const override;

  /**
   * Generate the CSV scan logic.
   * @param context The context of work.
   * @param function The function being built.
   */
  void Consume(ConsumerContext *context, FunctionBuilder *function) const override;

  /**
   * Launch the pipeline to scan the CSV.
   * @param pipeline_ctx The pipeline context.
   */
  void DrivePipeline(const PipelineContext &pipeline_ctx) const override;

  /**
   * Access a column from the base CSV.
   * @param col_oid The ID of the column to read.
   * @return The value of the column.
   */
  edsl::ValueVT GetTableColumn(uint16_t col_oid) const override;

 private:
  // Return the plan.
  const planner::CSVScanPlanNode &GetCSVPlan() const {
    return GetPlanAs<planner::CSVScanPlanNode>();
  }

  // Access the given field in the CSV row.
  edsl::Reference<ast::x::StringVal> GetField(uint32_t field_index) const;

  // Access a pointer to the field in the CSV row.
  edsl::Value<ast::x::StringVal *> GetFieldPtr(uint32_t field_index) const;

 private:
  // The struct capturing information about one CSV row.
  edsl::Struct row_struct_;
  // The CSV row; the type of this row is the type of the struct above.
  std::unique_ptr<edsl::VariableVT> row_var_;
  // The slot in pipeline state where the CSV reader is.
  ExecutionState::Slot<ast::x::CSVReader> csv_reader_;
  // The boolean flag indicating if the reader is valid.
  ExecutionState::Slot<bool> is_valid_reader_;
};

}  // namespace tpl::sql::codegen
