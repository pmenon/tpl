#include "sql/codegen/operators/static_aggregation_translator.h"

#include "sql/codegen/compilation_context.h"
#include "sql/codegen/function_builder.h"
#include "sql/codegen/work_context.h"
#include "sql/planner/plannodes/aggregate_plan_node.h"

namespace tpl::sql::codegen {

namespace {
constexpr char kAggAttrPrefix[] = "agg_term_attr";
}  // namespace

StaticAggregationTranslator::StaticAggregationTranslator(const planner::AggregatePlanNode &plan,
                                                         CompilationContext *compilation_context,
                                                         Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline),
      agg_row_var_(GetCodeGen()->MakeFreshIdentifier("aggRow")),
      agg_payload_type_(GetCodeGen()->MakeFreshIdentifier("AggPayload")),
      agg_values_type_(GetCodeGen()->MakeFreshIdentifier("AggValues")),
      merge_func_(GetCodeGen()->MakeFreshIdentifier("MergeAggregates")),
      build_pipeline_(this, Pipeline::Parallelism::Parallel) {
  TPL_ASSERT(plan.GetGroupByTerms().empty(), "Global aggregations shouldn't have grouping keys");
  TPL_ASSERT(plan.GetChildrenSize() == 1, "Global aggregations should only have one child");

  pipeline->RegisterStep(this, Pipeline::Parallelism::Serial);
  pipeline->LinkSourcePipeline(&build_pipeline_);

  compilation_context->Prepare(*plan.GetChild(0), &build_pipeline_);

  for (const auto agg_term : plan.GetAggregateTerms()) {
    compilation_context->Prepare(*agg_term->GetChild(0));
  }

  if (const auto having_clause = plan.GetHavingClausePredicate(); having_clause != nullptr) {
    compilation_context->Prepare(*having_clause);
  }

  CodeGen *codegen = GetCodeGen();
  ast::Expr *payload_type = codegen->MakeExpr(agg_payload_type_);
  global_aggs_ =
      compilation_context->GetQueryState()->DeclareStateEntry(codegen, "aggs", payload_type);

  if (build_pipeline_.IsParallel()) {
    local_aggs_ =
        build_pipeline_.GetPipelineState()->DeclareStateEntry(codegen, "aggs", payload_type);
  }
}

void StaticAggregationTranslator::GeneratePayloadStruct(
    util::RegionVector<ast::StructDecl *> *decls) {
  auto codegen = GetCodeGen();
  auto fields = codegen->MakeEmptyFieldList();
  fields.reserve(GetAggPlan().GetAggregateTerms().size());

  uint32_t term_idx = 0;
  for (const auto &term : GetAggPlan().GetAggregateTerms()) {
    auto name = codegen->MakeIdentifier(kAggAttrPrefix + std::to_string(term_idx++));
    auto type = codegen->AggregateType(term->GetExpressionType(), term->GetReturnValueType());
    fields.push_back(codegen->MakeField(name, type));
  }
  decls->push_back(codegen->DeclareStruct(agg_payload_type_, std::move(fields)));
}

void StaticAggregationTranslator::GenerateValuesStruct(
    util::RegionVector<ast::StructDecl *> *decls) {
  auto codegen = GetCodeGen();
  auto fields = codegen->MakeEmptyFieldList();
  fields.reserve(GetAggPlan().GetAggregateTerms().size());

  uint32_t term_idx = 0;
  for (const auto &term : GetAggPlan().GetAggregateTerms()) {
    auto field_name = codegen->MakeIdentifier(kAggAttrPrefix + std::to_string(term_idx));
    auto type = codegen->TplType(term->GetReturnValueType());
    fields.push_back(codegen->MakeField(field_name, type));
    term_idx++;
  }
  decls->push_back(codegen->DeclareStruct(agg_values_type_, std::move(fields)));
}

void StaticAggregationTranslator::DefineHelperStructs(
    util::RegionVector<ast::StructDecl *> *decls) {
  GeneratePayloadStruct(decls);
  GenerateValuesStruct(decls);
}

void StaticAggregationTranslator::DefineHelperFunctions(
    util::RegionVector<ast::FunctionDecl *> *decls) {
  if (build_pipeline_.IsParallel()) {
    CodeGen *codegen = GetCodeGen();
    util::RegionVector<ast::FieldDecl *> params = build_pipeline_.PipelineParams();
    FunctionBuilder function(codegen, merge_func_, std::move(params), codegen->Nil());
    {
      for (uint32_t term_idx = 0; term_idx < GetAggPlan().GetAggregateTerms().size(); term_idx++) {
        auto lhs = GetAggregateTermPtr(global_aggs_.Get(codegen), term_idx);
        auto rhs = GetAggregateTermPtr(local_aggs_.Get(codegen), term_idx);
        function.Append(codegen->AggregatorMerge(lhs, rhs));
      }
    }
    decls->push_back(function.Finish());
  }
}

ast::Expr *StaticAggregationTranslator::GetAggregateTerm(ast::Expr *agg_row,
                                                         uint32_t attr_idx) const {
  auto codegen = GetCodeGen();
  auto member = codegen->MakeIdentifier(kAggAttrPrefix + std::to_string(attr_idx));
  return codegen->AccessStructMember(agg_row, member);
}

ast::Expr *StaticAggregationTranslator::GetAggregateTermPtr(ast::Expr *agg_row,
                                                            uint32_t attr_idx) const {
  return GetCodeGen()->AddressOf(GetAggregateTerm(agg_row, attr_idx));
}

void StaticAggregationTranslator::InitializeAggregates(FunctionBuilder *function,
                                                       bool local) const {
  CodeGen *codegen = GetCodeGen();
  const auto aggs = local ? local_aggs_ : global_aggs_;
  for (uint32_t term_idx = 0; term_idx < GetAggPlan().GetAggregateTerms().size(); term_idx++) {
    ast::Expr *agg_term = GetAggregateTermPtr(aggs.Get(codegen), term_idx);
    function->Append(codegen->AggregatorInit(agg_term));
  }
}

void StaticAggregationTranslator::InitializePipelineState(const Pipeline &pipeline,
                                                          FunctionBuilder *function) const {
  if (IsBuildPipeline(pipeline) && build_pipeline_.IsParallel()) {
    InitializeAggregates(function, true);
  }
}

void StaticAggregationTranslator::BeginPipelineWork(const Pipeline &pipeline,
                                                    FunctionBuilder *function) const {
  if (IsBuildPipeline(pipeline)) {
    InitializeAggregates(function, false);
  }
}

void StaticAggregationTranslator::UpdateGlobalAggregate(WorkContext *ctx,
                                                        FunctionBuilder *function) const {
  auto codegen = GetCodeGen();

  const auto agg_payload = build_pipeline_.IsParallel() ? local_aggs_ : global_aggs_;

  // var aggValues: AggValues
  auto agg_values = codegen->MakeFreshIdentifier("aggValues");
  function->Append(codegen->DeclareVarNoInit(agg_values, codegen->MakeExpr(agg_values_type_)));

  // Fill values.
  uint32_t term_idx = 0;
  for (const auto &term : GetAggPlan().GetAggregateTerms()) {
    auto lhs = GetAggregateTerm(codegen->MakeExpr(agg_values), term_idx++);
    auto rhs = ctx->DeriveValue(*term->GetChild(0), this);
    function->Append(codegen->Assign(lhs, rhs));
  }

  // Update aggregate.
  for (term_idx = 0; term_idx < GetAggPlan().GetAggregateTerms().size(); term_idx++) {
    auto agg = GetAggregateTermPtr(agg_payload.Get(codegen), term_idx);
    auto val = GetAggregateTermPtr(codegen->MakeExpr(agg_values), term_idx);
    function->Append(codegen->AggregatorAdvance(agg, val));
  }
}

void StaticAggregationTranslator::PerformPipelineWork(WorkContext *work_context,
                                                      FunctionBuilder *function) const {
  if (IsProducePipeline(work_context->GetPipeline())) {
    // var agg_row = &state.aggs
    CodeGen *codegen = GetCodeGen();
    function->Append(codegen->DeclareVarWithInit(agg_row_var_, global_aggs_.GetPtr(codegen)));
    // Push along.
    work_context->Consume(function);
  } else {
    UpdateGlobalAggregate(work_context, function);
  }
}

void StaticAggregationTranslator::FinishPipelineWork(const Pipeline &pipeline,
                                                     FunctionBuilder *function) const {
  if (IsBuildPipeline(pipeline) && build_pipeline_.IsParallel()) {
    // Merge thread-local aggregates into one.
    CodeGen *codegen = GetCodeGen();
    ast::Expr *thread_state_container = GetThreadStateContainer();
    ast::Expr *query_state = GetQueryStatePtr();
    function->Append(codegen->TLSIterate(thread_state_container, query_state, merge_func_));
  }
}

ast::Expr *StaticAggregationTranslator::GetChildOutput(WorkContext *work_context,
                                                       UNUSED uint32_t child_idx,
                                                       uint32_t attr_idx) const {
  TPL_ASSERT(child_idx == 0, "Aggregations can only have a single child.");
  if (IsProducePipeline(work_context->GetPipeline())) {
    auto codegen = GetCodeGen();
    return codegen->AggregatorResult(
        GetAggregateTermPtr(codegen->MakeExpr(agg_row_var_), attr_idx));
  }

  // The request is in the build pipeline. Forward to child translator.
  const auto child_translator = GetCompilationContext()->LookupTranslator(*GetPlan().GetChild(0));
  return child_translator->GetOutput(work_context, attr_idx);
}

}  // namespace tpl::sql::codegen
