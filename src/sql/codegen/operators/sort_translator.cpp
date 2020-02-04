#include "sql/codegen/operators/sort_translator.h"

#include <utility>

#include "sql/codegen/compilation_context.h"
#include "sql/codegen/function_builder.h"
#include "sql/codegen/if.h"
#include "sql/codegen/loop.h"
#include "sql/codegen/work_context.h"
#include "sql/planner/plannodes/order_by_plan_node.h"

namespace tpl::sql::codegen {

namespace {
constexpr const char kSortRowAttrPrefix[] = "attr";
}  // namespace

SortTranslator::SortTranslator(const planner::OrderByPlanNode &plan,
                               CompilationContext *compilation_context, Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline),
      sort_row_var_(GetCodeGen()->MakeFreshIdentifier("sortRow")),
      sort_row_type_(GetCodeGen()->MakeFreshIdentifier("SortRow")),
      lhs_row_(GetCodeGen()->MakeIdentifier("lhs")),
      rhs_row_(GetCodeGen()->MakeIdentifier("rhs")),
      compare_func_(GetCodeGen()->MakeFreshIdentifier("Compare")),
      child_pipeline_(this, Pipeline::Parallelism::Parallel),
      current_row_(CurrentRow::Child) {
  pipeline->RegisterStep(this, Pipeline::Parallelism::Serial);
  pipeline->LinkSourcePipeline(GetBuildPipeline());

  compilation_context->Prepare(*plan.GetChild(0), GetBuildPipeline());

  for (const auto &[expr, _] : plan.GetSortKeys()) {
    compilation_context->Prepare(*expr);
  }

  // Register a Sorter instance in the global query state.
  CodeGen *codegen = compilation_context->GetCodeGen();
  ast::Expr *sorter_type = codegen->BuiltinType(ast::BuiltinType::Sorter);
  global_sorter_ =
      compilation_context->GetQueryState()->DeclareStateEntry(codegen, "sorter", sorter_type);

  // Register another Sorter instance in the pipeline-local state if the
  // build pipeline is parallel.
  if (child_pipeline_.IsParallel()) {
    local_sorter_ =
        child_pipeline_.GetPipelineState()->DeclareStateEntry(GetCodeGen(), "sorter", sorter_type);
  }
}

void SortTranslator::DefineHelperStructs(util::RegionVector<ast::StructDecl *> *decls) {
  auto codegen = GetCodeGen();
  auto fields = codegen->MakeEmptyFieldList();
  GetAllChildOutputFields(0, kSortRowAttrPrefix, &fields);
  decls->push_back(codegen->DeclareStruct(sort_row_type_, std::move(fields)));
}

void SortTranslator::GenerateComparisonFunction(FunctionBuilder *function) {
  CodeGen *codegen = GetCodeGen();
  WorkContext context(GetCompilationContext(), *GetBuildPipeline());

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
      If check_comparison(function, codegen->Compare(tok, lhs, rhs));
      {
        // Return the appropriate value based on ordering.
        function->Append(codegen->Return(codegen->Const32(ret_value)));
      }
      check_comparison.EndIf();
      ret_value = -ret_value;
    }
  }
  current_row_ = CurrentRow::Child;
}

void SortTranslator::DefineHelperFunctions(util::RegionVector<ast::FunctionDecl *> *decls) {
  auto codegen = GetCodeGen();
  auto params = codegen->MakeFieldList({
      codegen->MakeField(lhs_row_, codegen->PointerType(sort_row_type_)),
      codegen->MakeField(rhs_row_, codegen->PointerType(sort_row_type_)),
  });
  FunctionBuilder builder(codegen, compare_func_, std::move(params), codegen->Int32Type());
  {
    // Generate body.
    GenerateComparisonFunction(&builder);
  }
  decls->push_back(builder.Finish(codegen->Const32(0)));
}

void SortTranslator::InitializeSorter(FunctionBuilder *function, ast::Expr *sorter_ptr) const {
  ast::Expr *mem_pool = GetMemoryPool();
  function->Append(GetCodeGen()->SorterInit(sorter_ptr, mem_pool, compare_func_, sort_row_type_));
}

void SortTranslator::TearDownSorter(FunctionBuilder *function, ast::Expr *sorter_ptr) const {
  function->Append(GetCodeGen()->SorterFree(sorter_ptr));
}

void SortTranslator::InitializeQueryState(FunctionBuilder *function) const {
  InitializeSorter(function, global_sorter_.GetPtr(GetCodeGen()));
}

void SortTranslator::TearDownQueryState(FunctionBuilder *function) const {
  TearDownSorter(function, global_sorter_.GetPtr(GetCodeGen()));
}

void SortTranslator::InitializePipelineState(const Pipeline &pipeline,
                                             FunctionBuilder *function) const {
  if (IsBuildPipeline(pipeline) && GetBuildPipeline().IsParallel()) {
    InitializeSorter(function, local_sorter_.GetPtr(GetCodeGen()));
  }
}

void SortTranslator::TearDownPipelineState(const Pipeline &pipeline,
                                           FunctionBuilder *function) const {
  if (IsBuildPipeline(pipeline) && GetBuildPipeline().IsParallel()) {
    TearDownSorter(function, local_sorter_.GetPtr(GetCodeGen()));
  }
}

ast::Expr *SortTranslator::GetSortRowAttribute(ast::Expr *sort_row, uint32_t attr_idx) const {
  auto codegen = GetCodeGen();
  auto attr_name = codegen->MakeIdentifier(kSortRowAttrPrefix + std::to_string(attr_idx));
  return codegen->AccessStructMember(sort_row, attr_name);
}

void SortTranslator::FillSortRow(WorkContext *ctx, FunctionBuilder *function,
                                 ast::Expr *sort_row) const {
  CodeGen *codegen = GetCodeGen();
  const auto child_schema = GetPlan().GetChild(0)->GetOutputSchema();
  for (uint32_t attr_idx = 0; attr_idx < child_schema->GetColumns().size(); attr_idx++) {
    ast::Expr *lhs = GetSortRowAttribute(sort_row, attr_idx);
    ast::Expr *rhs = GetChildOutput(ctx, 0, attr_idx);
    function->Append(codegen->Assign(lhs, rhs));
  }
}

void SortTranslator::InsertIntoSorter(WorkContext *ctx, FunctionBuilder *function) const {
  auto codegen = GetCodeGen();

  // Collect correct sorter instance.
  const auto sorter = ctx->GetPipeline().IsParallel() ? local_sorter_ : global_sorter_;

  auto sort_row = codegen->MakeExpr(sort_row_var_);
  if (const auto &plan = GetPlanAs<planner::OrderByPlanNode>(); plan.HasLimit()) {
    const auto top_k_val = plan.GetOffset() + plan.GetLimit();
    function->Append(codegen->DeclareVarWithInit(
        sort_row_var_,
        codegen->SorterInsertTopK(sorter.GetPtr(codegen), sort_row_type_, top_k_val)));
    FillSortRow(ctx, function, sort_row);
    function->Append(codegen->SorterInsertTopKFinish(sorter.GetPtr(codegen), top_k_val));
  } else {
    function->Append(codegen->DeclareVarWithInit(
        sort_row_var_, codegen->SorterInsert(sorter.GetPtr(codegen), sort_row_type_)));
    FillSortRow(ctx, function, sort_row);
  }
}

void SortTranslator::ScanSorter(WorkContext *ctx, FunctionBuilder *function) const {
  auto codegen = GetCodeGen();

  // var sorter_base: Sorter
  auto base_iter_name = codegen->MakeFreshIdentifier("iterBase");
  function->Append(codegen->DeclareVarNoInit(base_iter_name, ast::BuiltinType::SorterIterator));

  // var sorter = &sorter_base
  auto iter_name = codegen->MakeFreshIdentifier("iter");
  auto iter = codegen->MakeExpr(iter_name);
  function->Append(codegen->DeclareVarWithInit(
      iter_name, codegen->AddressOf(codegen->MakeExpr(base_iter_name))));

  auto sorter = global_sorter_.GetPtr(codegen);
  Loop loop(function,
            codegen->MakeStmt(codegen->SorterIterInit(iter, sorter)),  // @sorterIterInit();
            codegen->SorterIterHasNext(iter),                          // @sorterIterHasNext();
            codegen->MakeStmt(codegen->SorterIterNext(iter)));         // @sorterIterNext();
  {
    // var sortRow = @ptrCast(SortRow*, @sorterIterGetRow(sorter))
    auto row = codegen->SorterIterGetRow(iter, sort_row_type_);
    function->Append(codegen->DeclareVarWithInit(sort_row_var_, row));
    // Move along
    ctx->Push(function);
  }
  loop.EndLoop();

  // @sorterIterClose()
  function->Append(codegen->SorterIterClose(iter));
}

void SortTranslator::PerformPipelineWork(WorkContext *ctx, FunctionBuilder *function) const {
  if (IsScanPipeline(ctx->GetPipeline())) {
    ScanSorter(ctx, function);
  } else {
    TPL_ASSERT(IsBuildPipeline(ctx->GetPipeline()), "Pipeline is unknown to sort translator");
    InsertIntoSorter(ctx, function);
  }
}

void SortTranslator::FinishPipelineWork(const Pipeline &pipeline, FunctionBuilder *function) const {
  if (IsBuildPipeline(pipeline)) {
    CodeGen *codegen = GetCodeGen();
    ast::Expr *sorter_ptr = global_sorter_.GetPtr(codegen);
    if (GetBuildPipeline().IsParallel()) {
      // Build pipeline is parallel, so we need to issue a parallel sort. Issue
      // a SortParallel() or a SortParallelTopK() depending on whether a limit
      // was provided in the plan.
      ast::Expr *offset = local_sorter_.OffsetFromState(codegen);
      if (const auto &plan = GetPlanAs<planner::OrderByPlanNode>(); plan.HasLimit()) {
        const std::size_t top_k = plan.GetOffset() + plan.GetLimit();
        function->Append(
            codegen->SortTopKParallel(sorter_ptr, GetThreadStateContainer(), offset, top_k));
      } else {
        function->Append(codegen->SortParallel(sorter_ptr, GetThreadStateContainer(), offset));
      }
    } else {
      function->Append(codegen->SorterSort(sorter_ptr));
    }
  }
}

ast::Expr *SortTranslator::GetChildOutput(WorkContext *work_context, UNUSED uint32_t child_idx,
                                          uint32_t attr_idx) const {
  if (IsScanPipeline(work_context->GetPipeline())) {
    return GetSortRowAttribute(GetCodeGen()->MakeExpr(sort_row_var_), attr_idx);
  }

  TPL_ASSERT(IsBuildPipeline(work_context->GetPipeline()), "Pipeline not known to sorter");
  switch (current_row_) {
    case CurrentRow::Lhs:
      return GetSortRowAttribute(GetCodeGen()->MakeExpr(lhs_row_), attr_idx);
    case CurrentRow::Rhs:
      return GetSortRowAttribute(GetCodeGen()->MakeExpr(rhs_row_), attr_idx);
    case CurrentRow::Child: {
      auto child_translator = GetCompilationContext()->LookupTranslator(*GetPlan().GetChild(0));
      return child_translator->GetOutput(work_context, attr_idx);
    }
  }
  UNREACHABLE("Impossible output row option");
}

}  // namespace tpl::sql::codegen
