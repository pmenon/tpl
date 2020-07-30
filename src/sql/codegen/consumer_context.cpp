#include "sql/codegen/consumer_context.h"

#include "sql/codegen/compilation_context.h"
#include "sql/codegen/pipeline.h"

namespace tpl::sql::codegen {

ConsumerContext::ConsumerContext(CompilationContext *compilation_context, const Pipeline &pipeline)
    : compilation_context_(compilation_context),
      pipeline_(pipeline),
      pipeline_iter_(pipeline_.Begin()),
      pipeline_end_(pipeline_.End()),
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

bool ConsumerContext::IsParallel() const { return pipeline_.IsParallel(); }

}  // namespace tpl::sql::codegen
