#pragma once

#include <functional>
#include <unordered_map>
#include <utility>

#include "common/common.h"
#include "sql/codegen/ast_fwd.h"
#include "sql/codegen/pipeline.h"

namespace tpl::sql::planner {
class AbstractExpression;
}  // namespace tpl::sql::planner

namespace tpl::sql::codegen {

class CodeGen;
class CompilationContext;
class Pipeline;
class PipelineContext;

class ConsumerContext {
 public:
  /**
   * Generic interface class that can provide a value given a context.
   */
  class ValueProvider {
   public:
    /**
     * Destructor.
     */
    virtual ~ValueProvider() = default;

    /**
     * Given a context, provide a value.
     * @param ctx The context.
     * @return The value.
     */
    virtual ast::Expr *GetValue(ConsumerContext *ctx) const = 0;
  };

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
   * Register a value provider for the given column OID. If a provider already exists for the given
   * column, its provider will be replaced by the one given.
   * @param col_id The column OID.
   * @param provider The provider of the column's value.
   */
  void RegisterColumnValueProvider(uint16_t col_id, ValueProvider *provider);

  /**
   * @return Given a column OID, return the value provider for the column in this context. If no
   *         value provider exists for the column OID in this context, returns null.
   */
  ValueProvider *LookupColumnValueProvider(uint16_t col_id) const;

  /**
   * Derive the value of the given expression.
   * @param expr The expression.
   * @return The TPL value of the expression.
   */
  ast::Expr *DeriveValue(const planner::AbstractExpression &expr);

  /**
   * Push this context through to the next step in the pipeline.
   */
  void Push();

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
  // Column value providers.
  std::unordered_map<uint16_t, ValueProvider *> col_value_providers_;
  // The current pipeline step and last pipeline step.
  Pipeline::StepIterator pipeline_iter_, pipeline_end_;
};

}  // namespace tpl::sql::codegen
