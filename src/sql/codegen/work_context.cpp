#include "sql/codegen/work_context.h"

#include "logging/logger.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/pipeline.h"

namespace tpl::sql::codegen {

WorkContext::WorkContext(CompilationContext *compilation_context, const Pipeline &pipeline)
    : compilation_context_(compilation_context),
      pipeline_(pipeline),
      pipeline_iter_(pipeline_.Begin()),
      pipeline_end_(pipeline_.End()) {}

ast::Expr *WorkContext::DeriveValue(const planner::AbstractExpression &expr,
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

void WorkContext::Push(FunctionBuilder *function) {
  if (++pipeline_iter_ == pipeline_end_) {
    return;
  }
  (*pipeline_iter_)->PerformPipelineWork(this, function);
}

void WorkContext::ClearExpressionCache() { cache_.clear(); }

}  // namespace tpl::sql::codegen
