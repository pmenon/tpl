#include "sql/codegen/operators/sort_translator.h"

#include <utility>

#include "sql/codegen/compilation_context.h"
#include "sql/codegen/consumer_context.h"
#include "sql/codegen/function_builder.h"
#include "sql/codegen/top_level_declarations.h"
#include "sql/planner/plannodes/order_by_plan_node.h"

namespace tpl::sql::codegen {

SortTranslator::SortTranslator(const planner::OrderByPlanNode &plan,
                               CompilationContext *compilation_context, Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline),
      child_pipeline_(this, Pipeline::Parallelism::Flexible),
      sort_row_(nullptr),
      cmp_func_(nullptr) {
  // Indicate that this sorter is the source of the input pipeline. The output
  // of a sort must be serial to ensure delivery of tuples in sorted order.
  pipeline->RegisterSource(this, Pipeline::Parallelism::Serial);

  // Add the child pipeline as a dependency to the upper scan pipeline.
  pipeline->LinkSourcePipeline(&child_pipeline_);

  // Prepare child.
  compilation_context->Prepare(*plan.GetChild(0), &child_pipeline_);

  // Prepare expressions.
  for (const auto &[expr, _] : plan.GetSortKeys()) {
    compilation_context->Prepare(*expr);
  }

  // Register state.
  auto codegen = compilation_context->GetCodeGen();
  ast::Expr *sorter_type = codegen->BuiltinType(ast::BuiltinType::Kind::Sorter);
  sorter_ = compilation_context->GetQueryState()->DeclareStateEntry(codegen, "sorter", sorter_type);
}

void SortTranslator::DefineHelperStructs(TopLevelDeclarations *top_level_decls) {
  TPL_ASSERT(sort_row_ == nullptr, "Helper structure already generated!");
  auto codegen = GetCodeGen();
  auto fields = codegen->MakeEmptyFieldList();

  sort_row_attr_prefix_ = codegen->MakeFreshIdentifier("sorter_attr");
  std::string prefix(sort_row_attr_prefix_.GetString());

  const auto output_schema = GetPlan().GetChild(0)->GetOutputSchema();
  for (uint32_t idx = 0; idx < output_schema->GetColumns().size(); idx++) {
    auto field_name = codegen->MakeIdentifier(prefix + std::to_string(idx));
    auto type = codegen->TplType(output_schema->GetColumn(idx).GetExpr()->GetReturnValueType());
    fields.push_back(codegen->MakeField(field_name, type));
  }

  sort_row_ = codegen->DeclareStruct(codegen->MakeFreshIdentifier("SortRow"), std::move(fields));
  top_level_decls->RegisterStruct(sort_row_);
}

namespace {

class SortRowAccess : public ConsumerContext::ValueProvider {
 public:
  using EvalFn = std::function<ast::Expr *()>;
  explicit SortRowAccess(EvalFn fn) : fn_(std::move(fn)) {}
  ast::Expr *GetValue(UNUSED ConsumerContext *consumer_context) const override { return fn_(); }

 private:
  EvalFn fn_;
};

}  // namespace

void SortTranslator::DefineHelperFunctions(TopLevelDeclarations *top_level_decls) {
  TPL_ASSERT(cmp_func_ == nullptr, "Comparison function already generated!");

  auto codegen = GetCodeGen();
  auto fn_name = codegen->MakeFreshIdentifier("compare");

  // Make params
  auto sort_row_type = codegen->PointerType(sort_row_->Name());
  auto params =
      codegen->MakeFieldList({codegen->MakeField(codegen->MakeIdentifier("lhs"), sort_row_type),
                              codegen->MakeField(codegen->MakeIdentifier("rhs"), sort_row_type)});
  FunctionBuilder builder(codegen, fn_name, std::move(params), codegen->Int32Type());
  {
    // Setup the contexts.
    ConsumerContext left_ctx(GetCompilationContext(), nullptr);
    ConsumerContext right_ctx(GetCompilationContext(), nullptr);

    UNUSED int32_t ret_value;
    for (const auto &[expr, sort_order] : GetTypedPlan().GetSortKeys()) {
      if (sort_order == planner::OrderByOrderingType::ASC) {
        ret_value = -1;
      } else {
        ret_value = 1;
      }
      for (UNUSED const auto tok : {parsing::Token::Type::LESS, parsing::Token::Type::GREATER}) {
        UNUSED ast::Expr *lhs = left_ctx.DeriveValue(*expr);
        UNUSED ast::Expr *rhs = right_ctx.DeriveValue(*expr);
      }
    }
  }
  cmp_func_ = builder.Finish(codegen->Const32(1));
  top_level_decls->RegisterFunction(cmp_func_);
}

void SortTranslator::InitializeQueryState() const {
  auto codegen = GetCodeGen();
  auto func = codegen->CurrentFunction();
  auto sorter_ptr = GetQueryState().GetStateEntryPtr(codegen, sorter_);
  auto mem_pool = codegen->ExecCtxGetMemoryPool(GetExecutionContext());
  func->Append(codegen->SorterInit(sorter_ptr, mem_pool, cmp_func_->Name(), sort_row_->Name()));
}

void SortTranslator::TearDownQueryState() const {
  auto codegen = GetCodeGen();
  auto func = codegen->CurrentFunction();
  func->Append(codegen->SorterFree(GetQueryState().GetStateEntryPtr(codegen, sorter_)));
}

void SortTranslator::DeclarePipelineState(PipelineContext *pipeline_context) {
  if (IsBottomPipeline(pipeline_context->GetPipeline()) && pipeline_context->IsParallel()) {
    ast::Expr *sorter_type = GetCodeGen()->BuiltinType(ast::BuiltinType::Kind::Sorter);
    tl_sorter_ = pipeline_context->DeclareStateEntry(GetCodeGen(), "sorter", sorter_type);
  }
}

void SortTranslator::ScanSorter(ConsumerContext *ctx) const {}

void SortTranslator::InsertIntoSorter(ConsumerContext *ctx) const {}

void SortTranslator::DoPipelineWork(ConsumerContext *ctx) const {
  if (IsTopPipeline(ctx->GetPipeline())) {
    ScanSorter(ctx);
  } else {
    InsertIntoSorter(ctx);
  }
}

void SortTranslator::FinishPipelineWork(const PipelineContext &pipeline_context) const {
  if (IsBottomPipeline(pipeline_context.GetPipeline()) && pipeline_context.IsParallel()) {
    auto codegen = GetCodeGen();
    auto function = codegen->CurrentFunction();

    auto sorter = GetQueryState().GetStateEntryPtr(codegen, sorter_);
    auto offset = pipeline_context.GetThreadStateEntryOffset(codegen, tl_sorter_);

    if (GetTypedPlan().HasLimit()) {
      const auto limit = GetTypedPlan().GetLimit();
      function->Append(codegen->SortTopKParallel(sorter, GetThreadStateContainer(), offset, limit));
    } else {
      function->Append(codegen->SortParallel(sorter, GetThreadStateContainer(), offset));
    }
  }
}

util::RegionVector<ast::FieldDecl *> SortTranslator::GetWorkerParams() const {
  return GetCodeGen()->MakeEmptyFieldList();
}

}  // namespace tpl::sql::codegen
