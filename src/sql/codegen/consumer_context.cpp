#include "sql/codegen/consumer_context.h"

#include "logging/logger.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/pipeline.h"

namespace tpl::sql::codegen {

ConsumerContext::ConsumerContext(CompilationContext *compilation_context, const Pipeline &pipeline)
    : compilation_context_(compilation_context),
      pipeline_(pipeline),
      pipeline_context_(nullptr),
      pipeline_iter_(pipeline_.Begin()),
      pipeline_end_(pipeline_.End()) {}

ConsumerContext::ConsumerContext(CompilationContext *compilation_context,
                                 const PipelineContext *pipeline_context)
    : ConsumerContext(compilation_context, pipeline_context->GetPipeline()) {
  pipeline_context_ = pipeline_context;
}

ast::Expr *ConsumerContext::DeriveValue(const planner::AbstractExpression &expr,
                                        const ColumnValueProvider *provider) {
  if (auto iter = cache_.find(&expr); iter != cache_.end()) {
    return iter->second;
  }
  auto *translator = compilation_context_->LookupTranslator(expr);
  if (translator == nullptr) {
    return nullptr;
  }
  auto result = translator->DeriveValue(this, provider);
  cache_[&expr] = result;
  return result;
}

void ConsumerContext::Push() {
  if (++pipeline_iter_ == pipeline_end_) {
    return;
  }
  (*pipeline_iter_)->DoPipelineWork(this);
}

void ConsumerContext::ClearExpressionCache() { cache_.clear(); }

}  // namespace tpl::sql::codegen
