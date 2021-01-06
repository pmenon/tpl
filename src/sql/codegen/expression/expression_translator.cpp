#include "sql/codegen/expression/expression_translator.h"

#include "sql/codegen/compilation_context.h"
#include "sql/planner/expressions/abstract_expression.h"

namespace tpl::sql::codegen {

ExpressionTranslator::ExpressionTranslator(const planner::AbstractExpression &expr,
                                           CompilationContext *compilation_context)
    : expr_(expr),
      compilation_context_(compilation_context),
      codegen_(compilation_context->GetCodeGen()) {
  (void)compilation_context_;
}

const planner::AbstractExpression *ExpressionTranslator::GetChild(uint32_t idx) const {
  TPL_ASSERT(idx < expr_.NumChildren(), "Out-of-bounds child access");
  return expr_.GetChild(idx);
}

}  // namespace tpl::sql::codegen
