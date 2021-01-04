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

class CompilationContext;
class FunctionBuilder;
class Pipeline;

/**
 * This class carries information during the "consumption" phase of code-generation. It is passed
 * along all operators that constitute the pipeline, from leaves to roots. It provides access to
 * thread-local state and a mechanism to evaluation expressions in the pipeline.
 */
class ConsumerContext {
 public:
  /**
   * Create a new context whose data flows along the provided pipeline.
   * @param compilation_context The compilation context.
   * @param pipeline The pipeline.
   */
  ConsumerContext(CompilationContext *compilation_context, const PipelineContext &pipeline_ctx);

  /**
   * Derive the value of the given expression.
   * @param expr The expression.
   * @return The TPL value of the expression.
   */
  edsl::ValueVT DeriveValue(const planner::AbstractExpression &expr,
                            const ColumnValueProvider *provider);

  /**
   * Push this context to the next operator in the pipeline.
   * @param function The function that's being built.
   */
  void Consume(FunctionBuilder *function);

  /**
   * @return The operator the context is currently positioned at in the pipeline.
   */
  OperatorTranslator *CurrentOp() const { return *pipeline_iter_; }

  /**
   * @return An untyped reference to the element at the given slot within this pipeline state.
   */
  edsl::ReferenceVT GetStateEntryGeneric(ExecutionState::RTSlot slot) const;

  /**
   * @return A typed reference to the state element at the given slot in this pipeline's state.
   */
  template <typename T>
  edsl::Reference<T> GetStateEntry(ExecutionState::Slot<T> slot) const {
    return pipeline_ctx_.GetStateEntry<T>(slot);
  }

  /**
   * @return An untyped pointer to the element at the given slot within this pipeline's state.
   */
  edsl::ValueVT GetStateEntryPtrGeneric(ExecutionState::RTSlot slot) const;

  /**
   * @return A typed pointer to the element at the given slot within this pipeline's state.
   */
  template <typename T>
  edsl::Value<T *> GetStateEntryPtr(ExecutionState::Slot<T> slot) const {
    return pipeline_ctx_.GetStateEntryPtr<T>(slot);
  }

  /**
   * @return The byte offset of the element at the given slot in the pipeline state.
   */
  edsl::Value<uint32_t> GetByteOffsetOfStateEntry(ExecutionState::RTSlot slot) const;

  /**
   * @return True if the pipeline this work is flowing on is parallel; false otherwise.
   */
  bool IsParallel() const;

  /**
   * @return True if the pipeline for this context is vectorized; false otherwise.
   */
  bool IsVectorized() const;

  /**
   * @return True if this context is for the given pipeline; false otherwise.
   */
  bool IsForPipeline(const Pipeline &pipeline) const;

 private:
  // The compilation context.
  CompilationContext *compilation_context_;
  // The pipeline that this context flows through.
  const PipelineContext &pipeline_ctx_;
  // The current pipeline step and last pipeline step.
  Pipeline::Iterator pipeline_iter_, pipeline_end_;
};

}  // namespace tpl::sql::codegen
