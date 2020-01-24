#include "sql/codegen/operators/sort_translator.h"

#include <utility>

#include "sql/codegen/compilation_context.h"
#include "sql/codegen/function_builder.h"
#include "sql/codegen/if.h"
#include "sql/codegen/loop.h"
#include "sql/codegen/top_level_declarations.h"
#include "sql/codegen/work_context.h"
#include "sql/planner/plannodes/order_by_plan_node.h"

namespace tpl::sql::codegen {

namespace {
constexpr const char kSortRowAttrPrefix[] = "attr";
}  // namespace

SortTranslator::SortTranslator(const planner::OrderByPlanNode &plan,
                               CompilationContext *compilation_context, Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline),
      sort_row_var_name_(GetCodeGen()->MakeFreshIdentifier("sortRow")),
      child_pipeline_(this, Pipeline::Parallelism::Flexible),
      sort_row_(nullptr),
      cmp_func_(nullptr),
      current_row_(CurrentRow::Child) {
  pipeline->RegisterSource(this, Pipeline::Parallelism::Serial);
  pipeline->LinkSourcePipeline(GetBuildPipeline());

  compilation_context->Prepare(*plan.GetChild(0), GetBuildPipeline());

  for (const auto &[expr, _] : plan.GetSortKeys()) {
    compilation_context->Prepare(*expr);
  }

  // Register a Sorter instance.
  auto codegen = compilation_context->GetCodeGen();
  sorter_slot_ = compilation_context->GetQueryState()->DeclareStateEntry(
      codegen, "sorter", codegen->BuiltinType(ast::BuiltinType::Sorter));
}

void SortTranslator::DefineHelperStructs(TopLevelDeclarations *top_level_decls) {
  auto codegen = GetCodeGen();
  auto fields = codegen->MakeEmptyFieldList();
  GetAllChildOutputFields(0, kSortRowAttrPrefix, &fields);
  sort_row_ = codegen->DeclareStruct(codegen->MakeFreshIdentifier("SortRow"), std::move(fields));
  top_level_decls->RegisterStruct(sort_row_);
}

void SortTranslator::GenerateComparisonFunction(FunctionBuilder *builder) {
  auto codegen = GetCodeGen();
  auto context = WorkContext(GetCompilationContext(), *GetBuildPipeline());

  int32_t ret_value;
  for (const auto &[expr, sort_order] : GetPlanAs<planner::OrderByPlanNode>().GetSortKeys()) {
    if (sort_order == planner::OrderByOrderingType::ASC) {
      ret_value = -1;
    } else {
      ret_value = 1;
    }
    for (const auto tok : {parsing::Token::Type::LESS, parsing::Token::Type::GREATER}) {
      current_row_ = CurrentRow::Lhs;
      ast::Expr *lhs = context.DeriveValue(*expr, this);
      current_row_ = CurrentRow::Rhs;
      ast::Expr *rhs = context.DeriveValue(*expr, this);
      If check_comparison(codegen, codegen->Compare(tok, lhs, rhs));
      builder->Append(codegen->Return(codegen->Const32(ret_value)));
      check_comparison.EndIf();
      ret_value = -ret_value;
    }
  }
  current_row_ = CurrentRow::Child;
}

void SortTranslator::DefineHelperFunctions(TopLevelDeclarations *top_level_decls) {
  auto codegen = GetCodeGen();
  auto fn_name = codegen->MakeFreshIdentifier("compare");
  auto params = codegen->MakeFieldList({
      codegen->MakeField(codegen->MakeIdentifier("lhs"), codegen->PointerType(sort_row_->Name())),
      codegen->MakeField(codegen->MakeIdentifier("rhs"), codegen->PointerType(sort_row_->Name())),
  });
  FunctionBuilder builder(codegen, fn_name, std::move(params), codegen->Int32Type());
  {
    // Generate body.
    GenerateComparisonFunction(&builder);
  }
  cmp_func_ = builder.Finish(codegen->Const32(0));
  top_level_decls->RegisterFunction(cmp_func_);
}

void SortTranslator::InitializeSorter(ast::Expr *sorter_ptr) const {
  auto codegen = GetCodeGen();
  auto func = codegen->CurrentFunction();
  auto mem_pool = GetMemoryPool();
  func->Append(codegen->SorterInit(sorter_ptr, mem_pool, cmp_func_->Name(), sort_row_->Name()));
}

void SortTranslator::TearDownSorter(ast::Expr *sorter_ptr) const {
  auto codegen = GetCodeGen();
  codegen->CurrentFunction()->Append(codegen->SorterFree(sorter_ptr));
}

void SortTranslator::InitializeQueryState() const {
  InitializeSorter(GetQueryStateEntryPtr(sorter_slot_));
}

void SortTranslator::TearDownQueryState() const {
  TearDownSorter(GetQueryStateEntryPtr(sorter_slot_));
}

void SortTranslator::DeclarePipelineState(PipelineContext *pipeline_context) {
  if (IsBuildPipeline(pipeline_context->GetPipeline()) && pipeline_context->IsParallel()) {
    ast::Expr *sorter_type = GetCodeGen()->BuiltinType(ast::BuiltinType::Sorter);
    tl_sorter_slot_ = pipeline_context->DeclareStateEntry(GetCodeGen(), "sorter", sorter_type);
  }
}

void SortTranslator::InitializePipelineState(const PipelineContext &pipeline_context) const {
  if (IsBuildPipeline(pipeline_context.GetPipeline()) && GetBuildPipeline().IsParallel()) {
    InitializeSorter(pipeline_context.GetThreadStateEntryPtr(GetCodeGen(), tl_sorter_slot_));
  }
}

void SortTranslator::TearDownPipelineState(const PipelineContext &pipeline_context) const {
  if (IsBuildPipeline(pipeline_context.GetPipeline()) && GetBuildPipeline().IsParallel()) {
    TearDownSorter(pipeline_context.GetThreadStateEntryPtr(GetCodeGen(), tl_sorter_slot_));
  }
}

ast::Expr *SortTranslator::GetSortRowAttribute(ast::Expr *sort_row, uint32_t attr_idx) const {
  auto codegen = GetCodeGen();
  auto attr_name = codegen->MakeIdentifier(kSortRowAttrPrefix + std::to_string(attr_idx));
  return codegen->AccessStructMember(sort_row, attr_name);
}

void SortTranslator::FillSortRow(WorkContext *ctx, ast::Expr *sort_row) const {
  auto codegen = GetCodeGen();
  const auto child_schema = GetPlan().GetChild(0)->GetOutputSchema();
  for (uint32_t attr_idx = 0; attr_idx < child_schema->GetColumns().size(); attr_idx++) {
    auto lhs = GetSortRowAttribute(sort_row, attr_idx);
    auto rhs = GetChildOutput(ctx, 0, attr_idx);
    codegen->CurrentFunction()->Append(codegen->Assign(lhs, rhs));
  }
}

void SortTranslator::InsertIntoSorter(WorkContext *ctx) const {
  auto codegen = GetCodeGen();
  auto func = codegen->CurrentFunction();

  // Collect correct sorter instance.
  ast::Expr *sorter = nullptr;
  if (ctx->GetPipeline().IsParallel()) {
    const auto pipeline_context = ctx->GetPipelineContext();
    sorter = pipeline_context->GetThreadStateEntryPtr(codegen, tl_sorter_slot_);
  } else {
    sorter = GetQueryStateEntryPtr(sorter_slot_);
  }

  auto sort_row = codegen->MakeExpr(sort_row_var_name_);
  if (const auto &plan = GetPlanAs<planner::OrderByPlanNode>(); plan.HasLimit()) {
    // @sorterInsertTopK()
    const std::size_t top_k = plan.GetOffset() + plan.GetLimit();
    func->Append(codegen->DeclareVarWithInit(
        sort_row_var_name_, codegen->SorterInsertTopK(sorter, sort_row_->Name(), top_k)));
    // Fill row.
    FillSortRow(ctx, sort_row);
    // @sorterInsertTopKFinish();
    func->Append(codegen->SorterInsertTopKFinish(sorter, top_k));
  } else {
    // @sorterInsert()
    func->Append(codegen->DeclareVarWithInit(sort_row_var_name_,
                                             codegen->SorterInsert(sorter, sort_row_->Name())));
    // Fill row.
    FillSortRow(ctx, sort_row);
  }
}

void SortTranslator::ScanSorter(WorkContext *ctx) const {
  auto codegen = GetCodeGen();
  auto func = codegen->CurrentFunction();

  // var sorter_base: Sorter
  auto base_iter_name = codegen->MakeFreshIdentifier("iter_base");
  func->Append(codegen->DeclareVarNoInit(base_iter_name, ast::BuiltinType::SorterIterator));

  // var sorter = &sorter_base
  auto iter_name = codegen->MakeFreshIdentifier("iter");
  auto iter = codegen->MakeExpr(iter_name);
  func->Append(codegen->DeclareVarWithInit(iter_name,
                                           codegen->AddressOf(codegen->MakeExpr(base_iter_name))));

  auto sorter = GetQueryStateEntryPtr(sorter_slot_);
  Loop loop(codegen,
            codegen->MakeStmt(codegen->SorterIterInit(iter, sorter)),  // @sorterIterInit();
            codegen->SorterIterHasNext(iter),                          // @sorterIterHasNext();
            codegen->MakeStmt(codegen->SorterIterNext(iter)));         // @sorterIterNext();
  {
    // var sortRow = @ptrCast(SortRow*, @sorterIterGetRow(sorter))
    auto row = codegen->SorterIterGetRow(iter, sort_row_->Name());
    func->Append(codegen->DeclareVarWithInit(sort_row_var_name_, row));
    // Move along
    ctx->Push();
  }
  loop.EndLoop();

  // @sorterIterClose()
  func->Append(codegen->SorterIterClose(iter));
}

void SortTranslator::PerformPipelineWork(WorkContext *ctx) const {
  if (IsScanPipeline(ctx->GetPipeline())) {
    ScanSorter(ctx);
  } else {
    TPL_ASSERT(IsBuildPipeline(ctx->GetPipeline()), "Pipeline is unknown to sort translator");
    InsertIntoSorter(ctx);
  }
}

void SortTranslator::FinishPipelineWork(const PipelineContext &pipeline_context) const {
  if (IsBuildPipeline(pipeline_context.GetPipeline())) {
    auto codegen = GetCodeGen();
    auto func = codegen->CurrentFunction();

    auto sorter = GetQueryStateEntryPtr(sorter_slot_);
    auto offset = pipeline_context.GetThreadStateEntryOffset(codegen, tl_sorter_slot_);

    if (GetBuildPipeline().IsParallel()) {
      if (const auto &plan = GetPlanAs<planner::OrderByPlanNode>(); plan.HasLimit()) {
        const std::size_t top_k = plan.GetOffset() + plan.GetLimit();
        func->Append(codegen->SortTopKParallel(sorter, GetThreadStateContainer(), offset, top_k));
      } else {
        func->Append(codegen->SortParallel(sorter, GetThreadStateContainer(), offset));
      }
    } else {
      func->Append(codegen->SorterSort(sorter));
    }
  }
}

ast::Expr *SortTranslator::GetChildOutput(WorkContext *work_context, UNUSED uint32_t child_idx,
                                          uint32_t attr_idx) const {
  if (IsScanPipeline(work_context->GetPipeline())) {
    return GetSortRowAttribute(GetCodeGen()->MakeExpr(sort_row_var_name_), attr_idx);
  } else {
    TPL_ASSERT(IsBuildPipeline(work_context->GetPipeline()), "Pipeline not known to sorter");
    switch (current_row_) {
      case CurrentRow::Lhs: {
        auto func = GetCodeGen()->CurrentFunction();
        return GetSortRowAttribute(func->GetParameterByPosition(0), attr_idx);
      }
      case CurrentRow::Rhs: {
        auto func = GetCodeGen()->CurrentFunction();
        return GetSortRowAttribute(func->GetParameterByPosition(1), attr_idx);
      }
      case CurrentRow::Child: {
        auto child_translator = GetCompilationContext()->LookupTranslator(*GetPlan().GetChild(0));
        return child_translator->GetOutput(work_context, attr_idx);
      }
    }
  }
  return nullptr;
}

}  // namespace tpl::sql::codegen
