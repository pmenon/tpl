#include "sql/codegen/operators/limit_translator.h"

#include "sql/codegen/codegen.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/function_builder.h"
#include "sql/codegen/if.h"
#include "sql/codegen/work_context.h"
#include "sql/planner/plannodes/limit_plan_node.h"

namespace tpl::sql::codegen {

LimitTranslator::LimitTranslator(const planner::LimitPlanNode &plan,
                                 CompilationContext *compilation_context, Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline) {
  TPL_ASSERT(plan.GetOffset() != 0 || plan.GetLimit() != 0, "Both offset and limit cannot be 0");
  // Limits are serial ... for now.
  pipeline->RegisterStep(this, Pipeline::Parallelism::Serial);
  // Prepare child.
  compilation_context->Prepare(*plan.GetChild(0), pipeline);
  // Register state.
  CodeGen *codegen = GetCodeGen();
  tuple_count_ =
      pipeline->GetPipelineState()->DeclareStateEntry(codegen, "numTuples", codegen->Int32Type());
}

void LimitTranslator::InitializePipelineState(const Pipeline &pipeline,
                                              FunctionBuilder *function) const {
  CodeGen *codegen = GetCodeGen();
  function->Append(codegen->Assign(tuple_count_.Get(codegen), codegen->Const64(0)));
}

void LimitTranslator::PerformPipelineWork(WorkContext *work_context,
                                          FunctionBuilder *function) const {
  const auto &plan = GetPlanAs<planner::LimitPlanNode>();
  CodeGen *codegen = GetCodeGen();

  // Build the limit/offset condition check:
  // if (numTuples >= plan.offset and numTuples < plan.limit)
  ast::Expr *cond = nullptr;
  if (plan.GetOffset() != 0) {
    cond = codegen->Compare(parsing::Token::Type::GREATER_EQUAL, tuple_count_.Get(codegen),
                            codegen->Const32(plan.GetOffset()));
  }
  if (plan.GetLimit() != 0) {
    auto limit_check = codegen->Compare(parsing::Token::Type::LESS, tuple_count_.Get(codegen),
                                        codegen->Const32(plan.GetOffset() + plan.GetLimit()));
    cond = cond == nullptr ? limit_check
                           : codegen->BinaryOp(parsing::Token::Type::AND, cond, limit_check);
  }

  If check_limit(function, cond);
  work_context->Push(function);
  check_limit.EndIf();

  // Update running count: numTuples += 1
  auto increment =
      codegen->BinaryOp(parsing::Token::Type::PLUS, tuple_count_.Get(codegen), codegen->Const32(1));
  function->Append(codegen->Assign(tuple_count_.Get(codegen), increment));
}

ast::Expr *LimitTranslator::GetChildOutput(WorkContext *work_context, uint32_t child_idx,
                                           uint32_t attr_idx) const {
  const auto child_translator =
      GetCompilationContext()->LookupTranslator(*GetPlan().GetChild(child_idx));
  return child_translator->GetOutput(work_context, attr_idx);
}

}  // namespace tpl::sql::codegen
