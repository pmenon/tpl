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
   * Generate the CSV scan logic.
   * @param context The context of work.
   * @param function The function being built.
   */
  void Consume(ConsumerContext *context, FunctionBuilder *function) const override;

  /**
   * CSV Scans are always serial, so should never launch work.
   */
  std::vector<ast::FieldDecl *> GetWorkerParams() const override {
    UNREACHABLE("CSV scans are always serial ... for now.");
  }

  /**
   * CSV Scans are always serial, so should never launch work.
   */
  void LaunchWork(FunctionBuilder *function, ast::Identifier work_func_name) const override {
    UNREACHABLE("CSV scans are always serial ... for now.");
  }

  /**
   * Access a column from the base CSV.
   * @param col_oid The ID of the column to read.
   * @return The value of the column.
   */
  ast::Expr *GetTableColumn(uint16_t col_oid) const override;

 private:
  // Return the plan.
  const planner::CSVScanPlanNode &GetCSVPlan() const {
    return GetPlanAs<planner::CSVScanPlanNode>();
  }

  // Access the given field in the CSV row.
  ast::Expr *GetField(uint32_t field_index) const;
  // Access a pointer to the field in the CSV row.
  ast::Expr *GetFieldPtr(uint32_t field_index) const;

 private:
  // The name of the base row variable.
  ast::Identifier base_row_type_;
  StateDescriptor::Slot base_row_;
};

}  // namespace tpl::sql::codegen
