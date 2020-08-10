#include "sql/codegen/consumer_context.h"

#include "sql/codegen/compilation_context.h"
#include "sql/codegen/pipeline.h"

namespace tpl::sql::codegen {

ConsumerContext::ConsumerContext(CompilationContext *compilation_context,
                                 const PipelineContext &pipeline_ctx)
    : compilation_context_(compilation_context),
      pipeline_ctx_(pipeline_ctx),
      pipeline_iter_(pipeline_ctx_.pipeline_.Begin()),
      pipeline_end_(pipeline_ctx_.pipeline_.End()),
      cache_enabled_(true) {}

ast::Expr *ConsumerContext::DeriveValue(const planner::AbstractExpression &expr,
                                        const ColumnValueProvider *provider) {
  if (cache_enabled_) {
    if (auto iter = cache_.find(&expr); iter != cache_.end()) {
      return iter->second;
    }
  }
  auto *translator = compilation_context_->LookupTranslator(expr);
  if (translator == nullptr) {
    return nullptr;
  }
  auto result = translator->DeriveValue(this, provider);
  if (cache_enabled_) cache_[&expr] = result;
  return result;
}

void ConsumerContext::Consume(FunctionBuilder *function) {
  if (++pipeline_iter_ == pipeline_end_) {
    return;
  }
  (*pipeline_iter_)->Consume(this, function);
}

void ConsumerContext::ClearExpressionCache() { cache_.clear(); }

bool ConsumerContext::IsParallel() const { return pipeline_ctx_.IsParallel(); }

bool ConsumerContext::IsVectorized() const { return pipeline_ctx_.IsVectorized(); }

bool ConsumerContext::IsForPipeline(const Pipeline &pipeline) const {
  return pipeline_ctx_.IsForPipeline(pipeline);
}

ast::Expr *ConsumerContext::GetStateEntry(StateDescriptor::Slot slot) const {
  return pipeline_ctx_.GetStateEntry(slot);
}

ast::Expr *ConsumerContext::GetStateEntryPtr(StateDescriptor::Slot slot) const {
  return pipeline_ctx_.GetStateEntryPtr(slot);
}

ast::Expr *ConsumerContext::GetByteOffsetOfStateEntry(StateDescriptor::Slot slot) const {
  return pipeline_ctx_.GetStateEntryByteOffset(slot);
}

}  // namespace tpl::sql::codegen
