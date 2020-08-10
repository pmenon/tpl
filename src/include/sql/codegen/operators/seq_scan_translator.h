#pragma once

#include <string_view>
#include <vector>

#include "sql/codegen/ast_fwd.h"
#include "sql/codegen/operators/operator_translator.h"
#include "sql/codegen/pipeline.h"
#include "sql/codegen/pipeline_driver.h"
#include "sql/schema.h"

namespace tpl::sql::planner {
class AbstractExpression;
class SeqScanPlanNode;
}  // namespace tpl::sql::planner

namespace tpl::sql::codegen {

class FunctionBuilder;

/**
 * A translator for sequential table scans.
 */
class SeqScanTranslator : public OperatorTranslator, public PipelineDriver {
 public:
  /**
   * Create a translator for the given plan.
   * @param plan The plan.
   * @param compilation_context The context this translator belongs to.
   * @param pipeline The pipeline this translator is participating in.
   */
  SeqScanTranslator(const planner::SeqScanPlanNode &plan, CompilationContext *compilation_context,
                    Pipeline *pipeline);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(SeqScanTranslator);

  /**
   * Declare the filter manager.
   * @param pipeline_ctx The pipeline context.
   */
  void DeclarePipelineState(PipelineContext *pipeline_ctx) override;

  /**
   * Define all predicate functions if the scan has a predicate.
   * @param pipeline The pipeline the functions are being generated for.
   */
  void DefinePipelineFunctions(const PipelineContext &pipeline_ctx) override;

  /**
   * Initialize the FilterManager if required.
   */
  void InitializePipelineState(const PipelineContext &pipeline_ctx,
                               FunctionBuilder *function) const override;

  /**
   * Generate the scan.
   * @param context The context of the work.
   */
  void Consume(ConsumerContext *context, FunctionBuilder *function) const override;

  /**
   * Tear-down the FilterManager if required.
   */
  void TearDownPipelineState(const PipelineContext &pipeline_ctx,
                             FunctionBuilder *func) const override;

  /**
   * @return The pipeline work function parameters. Just the *TVI.
   */
  std::vector<ast::FieldDecl *> GetWorkerParams() const override;

  /**
   * Launch a parallel table scan.
   * @param work_func The worker function that'll be called during the parallel scan.
   */
  void LaunchWork(FunctionBuilder *function, ast::Identifier work_func) const override;

  /**
   * @return The value (or value vector) of the column with the provided column OID in the table
   *         this sequential scan is operating over.
   */
  ast::Expr *GetTableColumn(uint16_t col_oid) const override;

 private:
  // Does the scan have a predicate?
  bool HasPredicate() const;

  // Get the name of the table being scanned.
  std::string_view GetTableName() const;

  // Generate a generic filter term.
  void GenerateGenericTerm(FunctionBuilder *function, const planner::AbstractExpression *term,
                           ast::Expr *vector_proj, ast::Expr *tid_list);

  // Generate all filter clauses.
  void GenerateFilterClauseFunctions(const planner::AbstractExpression *predicate,
                                     std::vector<ast::Identifier> *curr_clause,
                                     bool seen_conjunction);

  // Perform a table scan using the provided table vector iterator pointer.
  void ScanTable(ConsumerContext *context, FunctionBuilder *function) const;

  // Generate a scan over the VPI.
  void ScanVPI(ConsumerContext *ctx, FunctionBuilder *function, ast::Expr *vpi) const;

 private:
  // The name of the declared TVI and VPI.
  ast::Identifier tvi_var_;
  ast::Identifier vpi_var_;

  // Where the filter manager exists.
  StateDescriptor::Slot local_filter_;

  // The list of filter manager clauses. Populated during helper function
  // definition, but only if there's a predicate.
  std::vector<std::vector<ast::Identifier>> filters_;
};

}  // namespace tpl::sql::codegen
