#pragma once

#include <functional>
#include <unordered_map>
#include <utility>

#include "common/common.h"
#include "sql/codegen/ast_fwd.h"
#include "sql/codegen/expression/expression_translator.h"
#include "sql/codegen/pipeline.h"

namespace tpl::sql::planner {
class AbstractExpression;
}  // namespace tpl::sql::planner

namespace tpl::sql::codegen {

class CodeGen;
class CompilationContext;
class Pipeline;
class PipelineContext;

/**
 * A consumer context is a wrapper class containing the code generation context and a pipelin
 */
class ConsumerContext {
 public:
  /**
   * Create a new consumer context whose data flows along the provided pipeline.
   * @param compilation_context The compilation context.
   * @param pipeline The pipeline.
   */
  ConsumerContext(CompilationContext *compilation_context, const Pipeline &pipeline);

  /**
   * Create a consumer context wrapping the provided compilation context (containing all operator
   * and expression translators) and the pipeline the consumption occurs along.
   * @param compilation_context The compilation context.
   * @param pipeline_context The pipeline context the consumption occurs along.
   */
  ConsumerContext(CompilationContext *compilation_context, const PipelineContext *pipeline_context);

  /**
   * Derive the value of the given expression.
   * @param expr The expression.
   * @return The TPL value of the expression.
   */
  ast::Expr *DeriveValue(const planner::AbstractExpression &expr,
                         const ColumnValueProvider *provider);

  /**
   * Push this context through to the next step in the pipeline.
   */
  void Push();

  /**
   * Clear any cached expression result values.
   */
  void ClearExpressionCache();

  /**
   * @return The operator the context is currently positioned at in the pipeline.
   */
  OperatorTranslator *CurrentOp() const { return *pipeline_iter_; }

  /**
   * @return The pipeline the consumption occurs in.
   */
  const Pipeline &GetPipeline() const { return pipeline_; }

  /**
   * @return The pipeline context, if available.
   */
  const PipelineContext *GetPipelineContext() const { return pipeline_context_; }

 private:
  // The compilation context.
  CompilationContext *compilation_context_;
  // The pipeline that this context flows through.
  const Pipeline &pipeline_;
  // The context of the consumption.
  const PipelineContext *pipeline_context_;
  // Cache of expression results.
  std::unordered_map<const planner::AbstractExpression *, ast::Expr *> cache_;
  // The current pipeline step and last pipeline step.
  Pipeline::StepIterator pipeline_iter_, pipeline_end_;
};

}  // namespace tpl::sql::codegen
