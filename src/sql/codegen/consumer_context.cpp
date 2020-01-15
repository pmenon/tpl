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

void ConsumerContext::RegisterColumnValueProvider(const uint16_t col_id,
                                                  ConsumerContext::ValueProvider *provider) {
#ifndef NDEBUG
  if (const auto iter = col_value_providers_.find(col_id); iter != col_value_providers_.end()) {
    LOG_WARN("Column OID {} already has provider {:p} in context. Overriding with {:p}.", col_id,
             static_cast<void *>(iter->second), static_cast<void *>(provider));
  }
#endif
  col_value_providers_[col_id] = provider;
}

ConsumerContext::ValueProvider *ConsumerContext::LookupColumnValueProvider(uint16_t col_id) const {
  auto iter = col_value_providers_.find(col_id);
  return iter == col_value_providers_.end() ? nullptr : iter->second;
}

ast::Expr *ConsumerContext::DeriveValue(const planner::AbstractExpression &expr) {
  auto *translator = compilation_context_->LookupTranslator(expr);
  if (translator == nullptr) {
    return nullptr;
  }
  return translator->DeriveValue(this);
}

void ConsumerContext::Push() {
  if (++pipeline_iter_ == pipeline_end_) {
    return;
  }
  (*pipeline_iter_)->DoPipelineWork(this);
}

}  // namespace tpl::sql::codegen
