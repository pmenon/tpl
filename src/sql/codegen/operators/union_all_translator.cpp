#include "sql/codegen/operators/union_all_translator.h"

#include "sql/codegen/compilation_context.h"
#include "sql/codegen/consumer_context.h"
#include "sql/codegen/function_builder.h"
#include "sql/planner/plannodes/set_op_plan_node.h"

namespace tpl::sql::codegen {

namespace {
constexpr const char kRowAttrPrefix[] = "attr";
}  // namespace

UnionAllTranslator::UnionAllTranslator(const planner::SetOpPlanNode &plan,
                                       CompilationContext *compilation_context, Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline),
      parent_fn_name_(codegen_->MakeIdentifier(pipeline->CreatePipelineFunctionName("Consume"))),
      row_type_name_(codegen_->MakeFreshIdentifier("UnionAllRow")),
      row_var_(codegen_->MakeFreshIdentifier("row")) {
  // Reserve in vector now so we have stable pointers.
  child_pipelines_.reserve(plan.GetChildrenSize());
  for (uint32_t i = 0; i < plan.GetChildrenSize(); i++) {
    auto child = &child_pipelines_.emplace_back(compilation_context, pipeline->GetPipelineGraph());
    compilation_context->Prepare(*plan.GetChild(i), child);
    pipeline->MarkNestedPipeline(child);
  }
}

void UnionAllTranslator::DeclarePipelineDependencies() const {
  for (uint32_t i = 1; i < child_pipelines_.size(); i++) {
    child_pipelines_[i].AddDependency(child_pipelines_[i - 1]);
  }
}

void UnionAllTranslator::DefinePipelineFunctions(const PipelineContext &pipeline_ctx) {
  if (pipeline_ctx.IsForPipeline(*GetPipeline())) {
    auto args = pipeline_ctx.PipelineParams();
    args.push_back(
        codegen_->MakeField(row_var_, codegen_->PointerType(codegen_->MakeExpr(row_type_name_))));
    FunctionBuilder function(codegen_, parent_fn_name_, std::move(args), nullptr);
    {
      ConsumerContext ctx(GetCompilationContext(), pipeline_ctx);
      ctx.Consume(&function);
    }
  }
}

ast::Expression *UnionAllTranslator::GetRowAttribute(ast::Identifier sort_row,
                                                     uint32_t attr_idx) const {
  ast::Identifier attr_name = codegen_->MakeIdentifier(kRowAttrPrefix + std::to_string(attr_idx));
  return codegen_->AccessStructMember(codegen_->MakeExpr(sort_row), attr_name);
}

void UnionAllTranslator::FillRow(ConsumerContext *context, FunctionBuilder *function) const {
  const auto child_schema = GetPlan().GetChild(0)->GetOutputSchema();
  for (uint32_t attr_idx = 0; attr_idx < child_schema->GetColumns().size(); attr_idx++) {
    ast::Expression *lhs = GetRowAttribute(row_var_, attr_idx);
    ast::Expression *rhs = GetChildOutput(context, 0, attr_idx);
    function->Append(codegen_->Assign(lhs, rhs));
  }
}

void UnionAllTranslator::Consume(ConsumerContext *context, FunctionBuilder *function) const {
  // Materialize the row.
  // var row: RowStruct
  // for (attr in child) row.attr = child.attr
  function->Append(codegen_->DeclareVarNoInit(row_var_, codegen_->MakeExpr(row_type_name_)));
  FillRow(context, function);
  // Invoke the consumption function.
  std::vector<ast::Expression *> args = {GetQueryStatePtr(), codegen_->MakeExpr(row_var_)};
  function->Append(codegen_->Call(parent_fn_name_, args));
}

void UnionAllTranslator::DefineRowStruct() {
  auto fields = codegen_->MakeEmptyFieldList();
  GetAllChildOutputFields(0, kRowAttrPrefix, &fields);
  codegen_->DeclareStruct(row_type_name_, std::move(fields));
}

void UnionAllTranslator::DefineStructsAndFunctions() { DefineRowStruct(); }

ast::Expression *UnionAllTranslator::GetChildOutput(ConsumerContext *context, uint32_t child_idx,
                                                    uint32_t attr_idx) const {
  if (context->IsForPipeline(*GetPipeline())) {
    return GetRowAttribute(row_var_, attr_idx);
  }
  return OperatorTranslator::GetChildOutput(context, child_idx, attr_idx);
}

}  // namespace tpl::sql::codegen
