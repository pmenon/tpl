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
   * Output translator does not hold state.
   */
  void InitializeQueryState(FunctionBuilder *func) const override {}

  /**
   * Output tranlator does not hold state.
   */
  void TearDownQueryState(FunctionBuilder *func) const override {}

  /**
   * Output translator does not need helper functions.
   */
  void DefineHelperFunctions(util::RegionVector<ast::FunctionDecl *> *decls) override {}

  /**
   * Define the output struct.
   */
  void DefineHelperStructs(util::RegionVector<ast::StructDecl *> *decls) override;

  /**
   * Output translator does not have pipeline specific work.
   */
  void InitializePipelineState(const Pipeline &pipeline, FunctionBuilder *function) const override {
  }

  /**
   * Output translator does not require any pre-pipeline logic.
   */
  void BeginPipelineWork(const Pipeline &pipeline, FunctionBuilder *func) const override {}

  /**
   * Perform the main work of the translator.
   */
  void PerformPipelineWork(WorkContext *work_context, FunctionBuilder *function) const override;

  /**
   * Output translator needs to finalize the output.
   */
  void FinishPipelineWork(const Pipeline &pipeline, FunctionBuilder *func) const override;

  /**
   * Nothing to tear down.
   */
  void TearDownPipelineState(const Pipeline &pipeline, FunctionBuilder *func) const override {}

  /**
   * Cannot be start of pipeline
   */
  util::RegionVector<ast::FieldDecl *> GetWorkerParams() const override {
    UNREACHABLE("Output cannot be start of pipeline");
  }

  /**
   * Cannot be start of pipeline
   */
  void LaunchWork(FunctionBuilder *function, ast::Identifier work_func) const override {
    UNREACHABLE("Output cannot be start of pipeline");
  }

  /**
   * Should not be called here.
   */
  ast::Expr *GetChildOutput(WorkContext *work_context, uint32_t child_idx,
                            uint32_t attr_idx) const override {
    UNREACHABLE("Not value is being derived.");
  }

  /**
   * Does not interact with tables.
   */
  ast::Expr *GetTableColumn(uint16_t col_oid) const override {
    UNREACHABLE("Output does not interact with tables.");
  }

 private:
  ast::Identifier output_var_;
  ast::Identifier output_struct_;

};  // namespace tpl::sql::codegen
}  // namespace tpl::sql::codegen