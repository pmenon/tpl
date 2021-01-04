#include "sql/codegen/consumer_context.h"

#include "common/exception.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/operators/operator_translator.h"
#include "sql/codegen/pipeline.h"

namespace tpl::sql::codegen {

ConsumerContext::ConsumerContext(CompilationContext *compilation_context,
                                 const PipelineContext &pipeline_ctx)
    : compilation_context_(compilation_context),
      pipeline_ctx_(pipeline_ctx),
      pipeline_iter_(pipeline_ctx_.pipeline_.Begin()),
      pipeline_end_(pipeline_ctx_.pipeline_.End()) {}

edsl::ValueVT ConsumerContext::DeriveValue(const planner::AbstractExpression &expr,
                                           const ColumnValueProvider *provider) {
  auto translator = compilation_context_->LookupTranslator(expr);
  if (translator == nullptr) {
    throw CodeGenerationException("Missing translator for expression!");
  }
  return translator->DeriveValue(this, provider);
}

void ConsumerContext::Consume(FunctionBuilder *function) {
  if (++pipeline_iter_ == pipeline_end_) return;
  (*pipeline_iter_)->Consume(this, function);
}

bool ConsumerContext::IsParallel() const { return pipeline_ctx_.IsParallel(); }

bool ConsumerContext::IsVectorized() const { return pipeline_ctx_.IsVectorized(); }

bool ConsumerContext::IsForPipeline(const Pipeline &pipeline) const {
  return pipeline_ctx_.IsForPipeline(pipeline);
}

edsl::ReferenceVT ConsumerContext::GetStateEntryGeneric(ExecutionState::RTSlot slot) const {
  return pipeline_ctx_.GetStateEntryGeneric(slot);
}

edsl::ValueVT ConsumerContext::GetStateEntryPtrGeneric(ExecutionState::RTSlot slot) const {
  return pipeline_ctx_.GetStateEntryPtrGeneric(slot);
}

edsl::Value<uint32_t> ConsumerContext::GetByteOffsetOfStateEntry(
    ExecutionState::RTSlot slot) const {
  return pipeline_ctx_.GetStateEntryByteOffset(slot);
}

}  // namespace tpl::sql::codegen
