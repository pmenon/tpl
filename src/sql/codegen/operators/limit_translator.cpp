#include "sql/codegen/operators/limit_translator.h"

#include "sql/codegen/codegen.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/consumer_context.h"
#include "sql/codegen/function_builder.h"
#include "sql/codegen/if.h"
#include "sql/planner/plannodes/limit_plan_node.h"

namespace tpl::sql::codegen {

LimitTranslator::LimitTranslator(const planner::LimitPlanNode &plan,
                                 CompilationContext *compilation_context, Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline) {
  TPL_ASSERT(plan.GetOffset() != 0 || plan.GetLimit() != 0, "Both offset and limit cannot be 0");
  // Limits are serial ... for now.
  pipeline->UpdateParallelism(Pipeline::Parallelism::Serial);
  // Prepare child.
  compilation_context->Prepare(*plan.GetChild(0), pipeline);
  // Register state.
  tuple_count_ = pipeline->DeclarePipelineStateEntry("num_tuples", codegen_->Int32Type());
}

void LimitTranslator::InitializePipelineState(const Pipeline &pipeline,
                                              FunctionBuilder *function) const {
  function->Append(codegen_->Assign(tuple_count_.Get(codegen_), codegen_->Const64(0)));
}

void LimitTranslator::Consume(ConsumerContext *context, FunctionBuilder *function) const {
  const auto &plan = GetPlanAs<planner::LimitPlanNode>();

  // Build the limit/offset condition check:
  // if (numTuples >= plan.offset and numTuples < plan.limit)
  ast::Expr *cond = nullptr;
  if (plan.GetOffset() != 0) {
    cond = codegen_->Compare(parsing::Token::Type::GREATER_EQUAL, tuple_count_.Get(codegen_),
                             codegen_->Const32(plan.GetOffset()));
  }
  if (plan.GetLimit() != 0) {
    auto limit_check = codegen_->Compare(parsing::Token::Type::LESS, tuple_count_.Get(codegen_),
                                         codegen_->Const32(plan.GetOffset() + plan.GetLimit()));
    cond = cond == nullptr ? limit_check
                           : codegen_->BinaryOp(parsing::Token::Type::AND, cond, limit_check);
  }

  If check_limit(function, cond);
  context->Consume(function);
  check_limit.EndIf();

  // Update running count: numTuples += 1
  auto increment = codegen_->BinaryOp(parsing::Token::Type::PLUS, tuple_count_.Get(codegen_),
                                      codegen_->Const32(1));
  function->Append(codegen_->Assign(tuple_count_.Get(codegen_), increment));
}

}  // namespace tpl::sql::codegen
