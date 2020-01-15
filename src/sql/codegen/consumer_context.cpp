#include "sql/codegen/consumer_context.h"

#include "logging/logger.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/pipeline.h"

namespace tpl::sql::codegen {

ConsumerContext::ConsumerContext(CompilationContext *compilation_context,
                                 PipelineContext *pipeline_context)
    : compilation_context_(compilation_context),
      pipeline_context_(pipeline_context),
      pipeline_idx_(0) {}

ast::Expr *ConsumerContext::DeriveValue(const planner::AbstractExpression &expr) {
  auto *translator = compilation_context_->LookupTranslator(expr);
  if (translator == nullptr) {
    return nullptr;
  }
  return translator->DeriveValue(this);
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

const Pipeline &ConsumerContext::GetPipeline() const { return pipeline_context_->GetPipeline(); }

}  // namespace tpl::sql::codegen
