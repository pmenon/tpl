#pragma once

#include <string_view>
#include <vector>

#include "sql/codegen/ast_fwd.h"
#include "sql/codegen/operators/operator_translator.h"
#include "sql/codegen/pipeline.h"

namespace tpl::sql::planner {
class AbstractExpression;
class SeqScanPlanNode;
}  // namespace tpl::sql::planner

namespace tpl::sql::codegen {

class FunctionBuilder;

/**
 * A translator for sequential table scans.
 */
class SeqScanTranslator : public OperatorTranslator {
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
   * Sequential scans don't have any state.
   */
  void InitializeQueryState() const override {}

  /**
   * Sequential scans don't have any state.
   */
  void TearDownQueryState() const override {}

  /**
   * If the scan has a predicate, this function will define all clause functions.
   * @param top_level_decls The top-level declarations.
   */
  void DefineHelperFunctions(TopLevelDeclarations *top_level_decls) override;

  /**
   * Declare a FilterManager if there's a scan predicate.
   */
  void DeclarePipelineState(PipelineContext *pipeline_context) override;

  /**
   * Initialize the FilterManager if required.
   */
  void InitializePipelineState(const PipelineContext &pipeline_context) const override;

  /**
   * Sequential scans don't require any pre-pipeline logic.
   */
  void BeginPipelineWork(const PipelineContext &pipeline_context) const override {}

  /**
   * Generate the scan.
   * @param consumer_context The consumer context.
   */
  void DoPipelineWork(ConsumerContext *consumer_context) const override;

  /**
   * @return The pipeline work function parameters. Just the *TVI.
   */
  util::RegionVector<ast::FieldDecl *> GetWorkerParams() const override;

  /**
   * Launch a parallel table scan.
   * @param work_func_name The worker function that'll be called during the parallel scan.
   */
  void LaunchWork(ast::Identifier work_func_name) const override;

  /**
   * Sequential scans don't rely on any post-pipeline logic.
   */
  void FinishPipelineWork(const PipelineContext &pipeline_context) const override {}

  /**
   * Tear-down the FilterManager if required.
   */
  void TearDownPipelineState(const PipelineContext &pipeline_context) const override;

 private:
  // Does the scan have a predicate?
  bool HasPredicate() const;

  // Get the name of the table being scanned.
  std::string_view GetTableName() const;

  // Generate a generic filter term.
  void GenerateGenericTerm(FunctionBuilder *func, const planner::AbstractExpression *term);

  // Generate all filter clauses.
  void GenerateFilterClauseFunctions(TopLevelDeclarations *top_level_declarations,
                                     const planner::AbstractExpression *predicate,
                                     std::vector<ast::Identifier> *curr_clause,
                                     bool seen_conjunction);

  // Perform a table scan using the provided table vector iterator pointer.
  void DoScanTable(ConsumerContext *ctx, ast::Expr *tvi, bool close_iter) const;

  // Generate a scan over the VPI.
  void GenerateVPIScan(ConsumerContext *consumer_context, ast::Expr *vpi) const;

 private:
  // Where the filter manager exists.
  PipelineContext::Slot fm_slot_;
  // The list of filter manager clauses. Populated during helper function
  // definition, but only if there's a predicate.
  std::vector<std::vector<ast::Identifier>> filters_;
};

}  // namespace tpl::sql::codegen
