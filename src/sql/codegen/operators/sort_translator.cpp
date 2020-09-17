#include "sql/codegen/operators/sort_translator.h"

#include <utility>

#include "sql/codegen/compilation_context.h"
#include "sql/codegen/consumer_context.h"
#include "sql/codegen/function_builder.h"
#include "sql/codegen/if.h"
#include "sql/codegen/loop.h"
#include "sql/planner/plannodes/order_by_plan_node.h"

namespace tpl::sql::codegen {

namespace {
constexpr const char kSortRowAttrPrefix[] = "attr";
}  // namespace

SortTranslator::SortTranslator(const planner::OrderByPlanNode &plan,
                               CompilationContext *compilation_context, Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline),
      sort_row_var_(codegen_->MakeFreshIdentifier("sort_row")),
      sort_row_type_(codegen_->MakeFreshIdentifier("SortRow")),
      lhs_row_(codegen_->MakeIdentifier("lhs")),
      rhs_row_(codegen_->MakeIdentifier("rhs")),
      compare_func_(codegen_->MakeFreshIdentifier(pipeline->CreatePipelineFunctionName("Compare"))),
      build_pipeline_(this, pipeline->GetPipelineGraph(), Pipeline::Parallelism::Parallel),
      current_row_(CurrentRow::Child) {
  TPL_ASSERT(plan.GetChildrenSize() == 1, "Sorts expected to have a single child.");
  // Register this as the source for the pipeline. It must be serial to maintain
  // sorted output order.
  pipeline->RegisterSource(this, Pipeline::Parallelism::Serial);

  // Prepare the child.
  compilation_context->Prepare(*plan.GetChild(0), &build_pipeline_);

  // Prepare the sort-key expressions.
  for (const auto &[expr, _] : plan.GetSortKeys()) {
    (void)_;
    compilation_context->Prepare(*expr);
  }

  // Register a Sorter instance in the global query state.
  ast::Expr *sorter_type = codegen_->BuiltinType(ast::BuiltinType::Sorter);
  global_sorter_ =
      compilation_context->GetQueryState()->DeclareStateEntry(codegen_, "sorter", sorter_type);
}

void SortTranslator::DeclarePipelineDependencies() const {
  GetPipeline()->AddDependency(build_pipeline_);
}

void SortTranslator::GenerateComparisonLogic(FunctionBuilder *function) {
  PipelineContext pipeline_context(build_pipeline_);
  ConsumerContext context(GetCompilationContext(), pipeline_context);
  context.SetExpressionCacheEnable(false);

  // For each sorting key, generate:
  //
  // return left.key < right.key;
  //
  // For multi-key sorting, we chain together checks through the else-clause:
  //
  // if (left.key1 != right.key1) return left.key1 < right.key1
  // if (left.key2 != right.key2) return left.key2 < right.key2
  // ...
  // return left.keyN < right.keyN
  //
  // The return value is controlled through the sort order for the key.

  const auto &sort_keys = GetPlanAs<planner::OrderByPlanNode>().GetSortKeys();
  for (std::size_t idx = 0; idx < sort_keys.size(); idx++) {
    const auto &[expr, sort_order] = sort_keys[idx];
    current_row_ = CurrentRow::Lhs;
    ast::Expr *lhs = context.DeriveValue(*expr, this);
    current_row_ = CurrentRow::Rhs;
    ast::Expr *rhs = context.DeriveValue(*expr, this);
    const auto comparison_type = sort_order == planner::OrderByOrderingType::ASC
                                     ? parsing::Token::Type::LESS
                                     : parsing::Token::Type::GREATER;
    if (idx != sort_keys.size() - 1) {
      If check_comparison(function, codegen_->Compare(parsing::Token::Type::BANG_EQUAL, lhs, rhs));
      ast::Expr *result = codegen_->Compare(comparison_type, lhs, rhs);
      function->Append(codegen_->Return(result));
    } else {
      ast::Expr *result = codegen_->Compare(comparison_type, lhs, rhs);
      function->Append(codegen_->Return(result));
    }
  }

  current_row_ = CurrentRow::Child;
}

ast::StructDecl *SortTranslator::GenerateSortRowStructType() const {
  auto fields = codegen_->MakeEmptyFieldList();
  GetAllChildOutputFields(0, kSortRowAttrPrefix, &fields);
  return codegen_->DeclareStruct(sort_row_type_, std::move(fields));
}

ast::FunctionDecl *SortTranslator::GenerateComparisonFunction() {
  auto params = codegen_->MakeFieldList({
      codegen_->MakeField(lhs_row_, codegen_->PointerType(sort_row_type_)),
      codegen_->MakeField(rhs_row_, codegen_->PointerType(sort_row_type_)),
  });
  FunctionBuilder builder(codegen_, compare_func_, std::move(params), codegen_->BoolType());
  {  //
    GenerateComparisonLogic(&builder);
  }
  return builder.Finish(codegen_->Const32(0));
}

void SortTranslator::DefineStructsAndFunctions() {
  GenerateSortRowStructType();
  GenerateComparisonFunction();
}

void SortTranslator::InitializeSorter(FunctionBuilder *function, ast::Expr *sorter_ptr) const {
  ast::Expr *mem_pool = GetMemoryPool();
  function->Append(codegen_->SorterInit(sorter_ptr, mem_pool, compare_func_, sort_row_type_));
}

void SortTranslator::TearDownSorter(FunctionBuilder *function, ast::Expr *sorter_ptr) const {
  function->Append(codegen_->SorterFree(sorter_ptr));
}

void SortTranslator::InitializeQueryState(FunctionBuilder *function) const {
  ast::Expr *sorter = GetQueryStateEntryPtr(global_sorter_);
  InitializeSorter(function, sorter);
}

void SortTranslator::TearDownQueryState(FunctionBuilder *function) const {
  ast::Expr *sorter = GetQueryStateEntryPtr(global_sorter_);
  TearDownSorter(function, sorter);
}

void SortTranslator::DeclarePipelineState(PipelineContext *pipeline_ctx) {
  if (pipeline_ctx->IsForPipeline(build_pipeline_) && pipeline_ctx->IsParallel()) {
    ast::Expr *sorter_type = codegen_->BuiltinType(ast::BuiltinType::Sorter);
    local_sorter_ = pipeline_ctx->DeclarePipelineStateEntry("sorter", sorter_type);
  }
}

void SortTranslator::InitializePipelineState(const PipelineContext &pipeline_ctx,
                                             FunctionBuilder *function) const {
  if (pipeline_ctx.IsForPipeline(build_pipeline_) && pipeline_ctx.IsParallel()) {
    ast::Expr *local_sorter = pipeline_ctx.GetStateEntryPtr(local_sorter_);
    InitializeSorter(function, local_sorter);
  }
}

void SortTranslator::TearDownPipelineState(const PipelineContext &pipeline_ctx,
                                           FunctionBuilder *function) const {
  if (pipeline_ctx.IsForPipeline(build_pipeline_) && pipeline_ctx.IsParallel()) {
    ast::Expr *local_sorter = pipeline_ctx.GetStateEntryPtr(local_sorter_);
    TearDownSorter(function, local_sorter);
  }
}

ast::Expr *SortTranslator::GetSortRowAttribute(ast::Identifier sort_row, uint32_t attr_idx) const {
  ast::Identifier attr_name =
      codegen_->MakeIdentifier(kSortRowAttrPrefix + std::to_string(attr_idx));
  return codegen_->AccessStructMember(codegen_->MakeExpr(sort_row), attr_name);
}

void SortTranslator::FillSortRow(ConsumerContext *ctx, FunctionBuilder *function) const {
  const auto child_schema = GetPlan().GetChild(0)->GetOutputSchema();
  for (uint32_t attr_idx = 0; attr_idx < child_schema->GetColumns().size(); attr_idx++) {
    ast::Expr *lhs = GetSortRowAttribute(sort_row_var_, attr_idx);
    ast::Expr *rhs = GetChildOutput(ctx, 0, attr_idx);
    function->Append(codegen_->Assign(lhs, rhs));
  }
}

template <typename F>
void SortTranslator::InsertIntoSorter(ConsumerContext *context, FunctionBuilder *function,
                                      F sorter_provider) const {
  if (const auto &plan = GetPlanAs<planner::OrderByPlanNode>(); plan.HasLimit()) {
    const std::size_t top_k = plan.GetOffset() + plan.GetLimit();
    ast::Expr *insert_call = codegen_->SorterInsertTopK(sorter_provider(), sort_row_type_, top_k);
    function->Append(codegen_->DeclareVarWithInit(sort_row_var_, insert_call));
    FillSortRow(context, function);
    function->Append(codegen_->SorterInsertTopKFinish(sorter_provider(), top_k));
  } else {
    ast::Expr *insert_call = codegen_->SorterInsert(sorter_provider(), sort_row_type_);
    function->Append(codegen_->DeclareVarWithInit(sort_row_var_, insert_call));
    FillSortRow(context, function);
  }
}

void SortTranslator::ScanSorter(ConsumerContext *context, FunctionBuilder *function) const {
  // var sorter_base: Sorter
  auto base_iter_name = codegen_->MakeFreshIdentifier("iter_base");
  function->Append(codegen_->DeclareVarNoInit(base_iter_name, ast::BuiltinType::SorterIterator));

  // var sorter = &sorter_base
  auto iter_name = codegen_->MakeFreshIdentifier("iter");
  const auto iter = [&]() { return codegen_->MakeExpr(iter_name); };
  function->Append(codegen_->DeclareVarWithInit(
      iter_name, codegen_->AddressOf(codegen_->MakeExpr(base_iter_name))));

  // Call @sorterIterInit().
  function->Append(codegen_->SorterIterInit(iter(), GetQueryStateEntryPtr(global_sorter_)));

  ast::Expr *init = nullptr;
  if (const auto offset = GetPlanAs<planner::OrderByPlanNode>().GetOffset(); offset != 0) {
    init = codegen_->SorterIterSkipRows(iter(), offset);
  }
  Loop loop(function,
            init == nullptr ? nullptr : codegen_->MakeStmt(init),   // @sorterIterSkipRows();
            codegen_->SorterIterHasNext(iter()),                    // @sorterIterHasNext();
            codegen_->MakeStmt(codegen_->SorterIterNext(iter())));  // @sorterIterNext()
  {
    // var sortRow = @ptrCast(SortRow*, @sorterIterGetRow(sorter))
    auto row = codegen_->SorterIterGetRow(iter(), sort_row_type_);
    function->Append(codegen_->DeclareVarWithInit(sort_row_var_, row));
    // Move along
    context->Consume(function);
  }
  loop.EndLoop();

  // @sorterIterClose()
  function->Append(codegen_->SorterIterClose(iter()));
}

void SortTranslator::Consume(ConsumerContext *context, FunctionBuilder *function) const {
  if (context->IsForPipeline(*GetPipeline())) {
    TPL_ASSERT(!context->IsParallel(), "Producing pipeline cannot be parallel!");
    ScanSorter(context, function);
  } else {
    TPL_ASSERT(IsBuildPipeline(context->GetPipeline()), "Pipeline is unknown to sort translator");
    if (context->IsParallel()) {
      const auto sorter_provider = [&]() { return context->GetStateEntryPtr(local_sorter_); };
      InsertIntoSorter(context, function, sorter_provider);
    } else {
      const auto sorter_provider = [&]() { return GetQueryStateEntryPtr(global_sorter_); };
      InsertIntoSorter(context, function, sorter_provider);
    }
  }
}

void SortTranslator::FinishPipelineWork(const PipelineContext &pipeline_ctx,
                                        FunctionBuilder *function) const {
  if (pipeline_ctx.IsForPipeline(build_pipeline_)) {
    ast::Expr *sorter_ptr = GetQueryStateEntryPtr(global_sorter_);
    if (pipeline_ctx.IsParallel()) {
      // Build pipeline is parallel, so we need to issue a parallel sort. Issue
      // a SortParallel() or a SortParallelTopK() depending on whether a limit
      // was provided in the plan.
      ast::Expr *tls = GetThreadStateContainer();
      ast::Expr *offset = pipeline_ctx.GetStateEntryByteOffset(local_sorter_);
      if (const auto &plan = GetPlanAs<planner::OrderByPlanNode>(); plan.HasLimit()) {
        const std::size_t top_k = plan.GetOffset() + plan.GetLimit();
        function->Append(codegen_->SortTopKParallel(sorter_ptr, tls, offset, top_k));
      } else {
        function->Append(codegen_->SortParallel(sorter_ptr, tls, offset));
      }
    } else {
      function->Append(codegen_->SorterSort(sorter_ptr));
    }
  }
}

ast::Expr *SortTranslator::GetChildOutput(ConsumerContext *context, UNUSED uint32_t child_idx,
                                          uint32_t attr_idx) const {
  if (context->IsForPipeline(*GetPipeline())) {
    return GetSortRowAttribute(sort_row_var_, attr_idx);
  }

  TPL_ASSERT(IsBuildPipeline(context->GetPipeline()), "Pipeline not known to sorter");
  switch (current_row_) {
    case CurrentRow::Lhs:
      return GetSortRowAttribute(lhs_row_, attr_idx);
    case CurrentRow::Rhs:
      return GetSortRowAttribute(rhs_row_, attr_idx);
    case CurrentRow::Child: {
      return OperatorTranslator::GetChildOutput(context, child_idx, attr_idx);
    }
  }
  UNREACHABLE("Impossible output row option");
}

void SortTranslator::DrivePipeline(const PipelineContext &pipeline_ctx) const {
  TPL_ASSERT(pipeline_ctx.IsForPipeline(*GetPipeline()), "Driving unknown pipeline!");
  GetPipeline()->LaunchSerial(pipeline_ctx);
}

}  // namespace tpl::sql::codegen
