#include "sql/codegen/expression/expression_translator.h"

#include "sql/codegen/compilation_context.h"
#include "sql/planner/expressions/abstract_expression.h"

namespace tpl::sql::codegen {

ExpressionTranslator::ExpressionTranslator(const planner::AbstractExpression &expr,
                                           CompilationContext *compilation_context)
    : expr_(expr),
      compilation_context_(compilation_context),
      codegen_(compilation_context->GetCodeGen()) {}

}  // namespace tpl::sql::codegen
