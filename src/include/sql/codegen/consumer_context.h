#pragma once

#include <functional>
#include <utility>

#include "common/common.h"
#include "sql/codegen/ast_fwd.h"

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
   * Generic interface class that can provide a value given an AST context.
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
   * Simple value provider that uses an injected function.
   */
  class FunctorValueProvider : public ValueProvider {
   public:
    using EvalFn = std::function<ast::Expr *(ConsumerContext *)>;

    /**
     * Create a value provider using the provided function callback.
     * @param f The callback function.
     */
    explicit FunctorValueProvider(EvalFn f) : f_(std::move(f)) {}

    /**
     * @return The computed value in the given context.
     */
    ast::Expr *GetValue(ConsumerContext *ctx) const override { return f_(ctx); }

   private:
    EvalFn f_;
  };

  /**
   * Create a consumer context wrapping the provided compilation context (containing all operator
   * and expression translators) and the pipeline the consumption occurs along.
   * @param compilation_context The compilation context.
   * @param pipeline_context The pipeline context the consumption occurs along.
   */
  ConsumerContext(CompilationContext *compilation_context, PipelineContext *pipeline_context);

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
   * @return The pipeline the consumption occurs in.
   */
  const Pipeline &GetPipeline() const;

  /**
   * @return The pipeline context the consumption occurs in.
   */
  PipelineContext *GetPipelineContext() const { return pipeline_context_; }

 private:
  // The compilation context.
  CompilationContext *compilation_context_;
  // The context of the consumption
  PipelineContext *pipeline_context_;
  // The current step position in the pipeline.
  uint32_t pipeline_idx_;
  // Column value providers.
  std::unordered_map<uint16_t, ValueProvider *> col_value_providers_;
};

}  // namespace tpl::sql::codegen
