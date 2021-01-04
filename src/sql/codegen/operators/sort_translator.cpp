#include "sql/codegen/operators/sort_translator.h"

#include <utility>

#include "sql/codegen/compilation_context.h"
#include "sql/codegen/consumer_context.h"
#include "sql/codegen/edsl/value_vt.h"
#include "sql/codegen/function_builder.h"
#include "sql/codegen/if.h"
#include "sql/codegen/loop.h"
#include "sql/planner/plannodes/order_by_plan_node.h"

namespace tpl::sql::codegen {

namespace {
constexpr std::string_view kSortRowAttrPrefix = "attr";
}  // namespace

SortTranslator::SortTranslator(const planner::OrderByPlanNode &plan,
                               CompilationContext *compilation_context, Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline),
      row_struct_(codegen_, "SortRow", true),
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
  global_sorter_ = GetQueryState()->DeclareStateEntry<ast::x::Sorter>("sorter");
}

void SortTranslator::DeclarePipelineDependencies() const {
  GetPipeline()->AddDependency(build_pipeline_);
}

void SortTranslator::GenerateComparisonLogic(FunctionBuilder *function) {
  PipelineContext pipeline_context(build_pipeline_);
  ConsumerContext context(GetCompilationContext(), pipeline_context);

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
    auto lhs = context.DeriveValue(*expr, this);
    current_row_ = CurrentRow::Rhs;
    auto rhs = context.DeriveValue(*expr, this);
    const auto comparison_type = sort_order == planner::OrderByOrderingType::ASC
                                     ? parsing::Token::Type::LESS
                                     : parsing::Token::Type::GREATER;
    if (idx != sort_keys.size() - 1) {
      If check_comparison(function, edsl::ComparisonOp(parsing::Token::Type::BANG_EQUAL, lhs, rhs));
      function->Append(edsl::Return(edsl::ComparisonOp(comparison_type, lhs, rhs)));
    } else {
      function->Append(edsl::Return(edsl::ComparisonOp(comparison_type, lhs, rhs)));
    }
  }

  current_row_ = CurrentRow::Child;
}

void SortTranslator::GenerateSortRowStructType() {
  GetAllChildOutputFields(0, kSortRowAttrPrefix, &row_struct_);
  row_struct_.Seal();
  row_ = std::make_unique<edsl::VariableVT>(codegen_, "sort_row", row_struct_.GetPtrToType());
  lhs_row_ = std::make_unique<edsl::VariableVT>(codegen_, "lhs", row_struct_.GetPtrToType());
  rhs_row_ = std::make_unique<edsl::VariableVT>(codegen_, "rhs", row_struct_.GetPtrToType());
}

void SortTranslator::GenerateComparisonFunction() {
  std::vector<FunctionBuilder::Param> params = {
      {lhs_row_->GetName(), row_struct_.GetType()->PointerTo()},
      {rhs_row_->GetName(), row_struct_.GetType()->PointerTo()},
  };
  FunctionBuilder builder(codegen_, compare_func_, std::move(params), codegen_->GetType<bool>());
  {  // Comparison logic.
    GenerateComparisonLogic(&builder);
  }
  builder.Finish();
}

void SortTranslator::DefineStructsAndFunctions() {
  GenerateSortRowStructType();
  GenerateComparisonFunction();
}

void SortTranslator::InitializeQueryState(FunctionBuilder *function) const {
  auto sorter = GetQueryStateEntryPtr(global_sorter_);
  function->Append(sorter->Init(GetMemoryPool(), compare_func_, row_struct_.GetSize()));
}

void SortTranslator::TearDownQueryState(FunctionBuilder *function) const {
  auto sorter = GetQueryStateEntryPtr(global_sorter_);
  function->Append(sorter->Free());
}

void SortTranslator::DeclarePipelineState(PipelineContext *pipeline_ctx) {
  if (pipeline_ctx->IsForPipeline(build_pipeline_) && pipeline_ctx->IsParallel()) {
    local_sorter_ = pipeline_ctx->DeclarePipelineStateEntry<ast::x::Sorter>("sorter");
  }
}

void SortTranslator::InitializePipelineState(const PipelineContext &pipeline_ctx,
                                             FunctionBuilder *function) const {
  if (pipeline_ctx.IsForPipeline(build_pipeline_) && pipeline_ctx.IsParallel()) {
    auto sorter = pipeline_ctx.GetStateEntryPtr(local_sorter_);
    function->Append(sorter->Init(GetMemoryPool(), compare_func_, row_struct_.GetSize()));
  }
}

void SortTranslator::TearDownPipelineState(const PipelineContext &pipeline_ctx,
                                           FunctionBuilder *function) const {
  if (pipeline_ctx.IsForPipeline(build_pipeline_) && pipeline_ctx.IsParallel()) {
    auto sorter = pipeline_ctx.GetStateEntryPtr(local_sorter_);
    function->Append(sorter->Free());
  }
}

edsl::ReferenceVT SortTranslator::GetSortRowAttribute(const edsl::ReferenceVT &row_ptr,
                                                      uint32_t attr_idx) const {
  return row_struct_.MemberGeneric(row_ptr, attr_idx);
}

void SortTranslator::FillSortRow(ConsumerContext *ctx, FunctionBuilder *function) const {
  const auto child_schema = GetPlan().GetChild(0)->GetOutputSchema();
  for (uint32_t attr_idx = 0; attr_idx < child_schema->GetColumns().size(); attr_idx++) {
    auto lhs = GetSortRowAttribute(*row_, attr_idx);
    auto rhs = GetChildOutput(ctx, 0, attr_idx);
    function->Append(edsl::Assign(lhs, rhs));
  }
}

void SortTranslator::InsertIntoSorter(ConsumerContext *context, FunctionBuilder *function,
                                      const edsl::Value<ast::x::Sorter *> &sorter) const {
  if (const auto &plan = GetPlanAs<planner::OrderByPlanNode>(); plan.HasLimit()) {
    // var sort_var = @sorterInsert()
    const std::size_t top_k = plan.GetOffset() + plan.GetLimit();
    auto bytes = sorter->InsertTopK(edsl::Literal<uint64_t>(codegen_, top_k));
    function->Append(edsl::Declare(*row_, edsl::PtrCast(row_struct_.GetPtrToType(), bytes)));
    FillSortRow(context, function);
    function->Append(sorter->InsertTopKFinish(edsl::Literal<uint64_t>(codegen_, top_k)));
  } else {
    auto bytes = sorter->Insert();
    function->Append(edsl::Declare(*row_, edsl::PtrCast(row_struct_.GetPtrToType(), bytes)));
    FillSortRow(context, function);
  }
}

void SortTranslator::ScanSorter(ConsumerContext *context, FunctionBuilder *function,
                                const edsl::Value<ast::x::Sorter *> &sorter) const {
  // var sorter_base: Sorter
  edsl::Variable<ast::x::SorterIterator> iter_base(codegen_, "iter_base");
  edsl::Variable<ast::x::SorterIterator *> iter(codegen_, "iter");

  function->Append(edsl::Declare(iter_base));
  function->Append(edsl::Declare(iter, iter_base.Addr()));

  // Call @sorterIterInit().
  function->Append(iter->Init(sorter));

  if (const auto offset = GetPlanAs<planner::OrderByPlanNode>().GetOffset(); offset != 0) {
    function->Append(iter->SkipRows(edsl::Literal<uint32_t>(codegen_, offset)));
  }

  Loop loop(function, edsl::Value<void>(nullptr), iter->HasNext(), iter->Next());
  {
    // var sortRow = @ptrCast(SortRow*, @sorterIterGetRow(sorter))
    function->Append(
        edsl::Declare(*row_, edsl::PtrCast(row_struct_.GetPtrToType(), iter->GetRow())));
    // Move along.
    context->Consume(function);
  }
  loop.EndLoop();

  // @sorterIterClose()
  function->Append(iter->Close());
}

void SortTranslator::Consume(ConsumerContext *context, FunctionBuilder *function) const {
  edsl::Variable<ast::x::Sorter *> sorter(codegen_, "sorter");
  if (context->IsForPipeline(*GetPipeline())) {
    TPL_ASSERT(!context->IsParallel(), "Producing pipeline cannot be parallel!");
    function->Append(edsl::Declare(sorter, GetQueryStateEntryPtr(global_sorter_)));
    ScanSorter(context, function, sorter);
  } else {
    TPL_ASSERT(IsBuildPipeline(context->GetPipeline()), "Pipeline is unknown to sort translator");
    function->Append(edsl::Declare(sorter, context->IsParallel()
                                               ? context->GetStateEntryPtr(local_sorter_)
                                               : GetQueryStateEntryPtr(global_sorter_)));
    InsertIntoSorter(context, function, sorter);
  }
}

void SortTranslator::FinishPipelineWork(const PipelineContext &pipeline_ctx,
                                        FunctionBuilder *function) const {
  if (pipeline_ctx.IsForPipeline(build_pipeline_)) {
    auto sorter = GetQueryStateEntryPtr(global_sorter_);
    if (pipeline_ctx.IsParallel()) {
      auto tls = GetThreadStateContainer();
      auto offset = pipeline_ctx.GetStateEntryByteOffset(local_sorter_);
      if (const auto &plan = GetPlanAs<planner::OrderByPlanNode>(); plan.HasLimit()) {
        auto top_k = edsl::Literal<uint32_t>(codegen_, plan.GetOffset() + plan.GetLimit());
        function->Append(sorter->SortTopKParallel(tls, offset, top_k));
      } else {
        function->Append(sorter->SortParallel(tls, offset));
      }
    } else {
      function->Append(sorter->Sort());
    }
  }
}

edsl::ValueVT SortTranslator::GetChildOutput(ConsumerContext *context, UNUSED uint32_t child_idx,
                                             uint32_t attr_idx) const {
  if (context->IsForPipeline(*GetPipeline())) {
    return GetSortRowAttribute(*row_, attr_idx);
  }

  TPL_ASSERT(IsBuildPipeline(context->GetPipeline()), "Pipeline not known to sorter");
  switch (current_row_) {
    case CurrentRow::Lhs:
      return GetSortRowAttribute(*lhs_row_, attr_idx);
    case CurrentRow::Rhs:
      return GetSortRowAttribute(*rhs_row_, attr_idx);
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
