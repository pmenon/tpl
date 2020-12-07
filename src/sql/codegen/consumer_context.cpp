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

ast::Expression *ConsumerContext::DeriveValue(const planner::AbstractExpression &expr,
                                              const ColumnValueProvider *provider) {
  auto translator = compilation_context_->LookupTranslator(expr);
  if (translator == nullptr) {
    return nullptr;
  }
  return translator->DeriveValue(this, provider);
}

void ConsumerContext::Consume(FunctionBuilder *function) {
  if (++pipeline_iter_ == pipeline_end_) {
    return;
  }
  (*pipeline_iter_)->Consume(this, function);
}

bool ConsumerContext::IsParallel() const { return pipeline_ctx_.IsParallel(); }

bool ConsumerContext::IsVectorized() const { return pipeline_ctx_.IsVectorized(); }

bool ConsumerContext::IsForPipeline(const Pipeline &pipeline) const {
  return pipeline_ctx_.IsForPipeline(pipeline);
}

ast::Expression *ConsumerContext::GetStateEntry(StateDescriptor::Slot slot) const {
  return pipeline_ctx_.GetStateEntry(slot);
}

ast::Expression *ConsumerContext::GetStateEntryPtr(StateDescriptor::Slot slot) const {
  return pipeline_ctx_.GetStateEntryPtr(slot);
}

ast::Expression *ConsumerContext::GetByteOffsetOfStateEntry(StateDescriptor::Slot slot) const {
  return pipeline_ctx_.GetStateEntryByteOffset(slot);
}

}  // namespace tpl::sql::codegen
