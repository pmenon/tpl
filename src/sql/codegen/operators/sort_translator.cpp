#include "sql/codegen/operators/sort_translator.h"

#include <utility>

#include "sql/codegen/compilation_context.h"
#include "sql/codegen/consumer_context.h"
#include "sql/codegen/function_builder.h"
#include "sql/codegen/if.h"
#include "sql/codegen/loop.h"
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

  // Prepare children.
  compilation_context->Prepare(*plan.GetChild(0), &child_pipeline_);

  // Prepare expressions.
  for (const auto &[expr, _] : plan.GetSortKeys()) {
    compilation_context->Prepare(*expr);
  }

  // Register state.
  CodeGen *codegen = compilation_context->GetCodeGen();
  ast::Expr *sorter_type = codegen->BuiltinType(ast::BuiltinType::Kind::Sorter);
  sorter_slot_ =
      compilation_context->GetQueryState()->DeclareStateEntry(codegen, "sorter", sorter_type);
}

void SortTranslator::DefineHelperStructs(TopLevelDeclarations *top_level_decls) {
  // Build struct.
  CodeGen *codegen = GetCodeGen();
  util::RegionVector<ast::FieldDecl *> fields = codegen->MakeEmptyFieldList();
  GetChildOutputFields(0, "attr", &fields);
  sort_row_ = codegen->DeclareStruct(codegen->MakeFreshIdentifier("SortRow"), std::move(fields));

  // Declare struct.
  top_level_decls->RegisterStruct(sort_row_);
}

class SortTranslator::SortRowAccess : public ConsumerContext::ValueProvider {
 public:
  SortRowAccess() : codegen_(nullptr), row_ptr_(nullptr) {}

  SortRowAccess(CodeGen *codegen, ast::Expr *row_ptr, const ast::Identifier &attribute)
      : codegen_(codegen), row_ptr_(row_ptr), attribute_(attribute) {}

  ast::Expr *GetValue(ConsumerContext *ctx) const override {
    return codegen_->AccessStructMember(row_ptr_, attribute_);
  }

 private:
  CodeGen *codegen_;
  ast::Expr *row_ptr_;
  ast::Identifier attribute_;
};

void SortTranslator::PopulateContextWithSortAttributes(ConsumerContext *consumer_context,
                                                       ast::Expr *sort_row,
                                                       std::vector<SortRowAccess> *attrs) const {
  const std::size_t num_cols = GetPlan().GetChild(0)->GetOutputSchema()->GetColumns().size();
  attrs->resize(num_cols);
  for (uint32_t i = 0; i < num_cols; i++) {
    ast::Identifier attr = GetCodeGen()->MakeIdentifier("attr" + std::to_string(i));
    (*attrs)[i] = SortRowAccess(GetCodeGen(), sort_row, attr);
    consumer_context->RegisterColumnValueProvider(i, &(*attrs)[i]);
  }
}

void SortTranslator::GenerateComparisonFunction(FunctionBuilder *builder, ast::Expr *lhs_row,
                                                ast::Expr *rhs_row) const {
  // Setup the contexts.
  std::vector<SortRowAccess> left_attrs, right_attrs;
  ConsumerContext left_ctx(GetCompilationContext(), *GetPipeline()),
      right_ctx(GetCompilationContext(), *GetPipeline());
  PopulateContextWithSortAttributes(&left_ctx, lhs_row, &left_attrs);
  PopulateContextWithSortAttributes(&right_ctx, rhs_row, &right_attrs);

  // Generate all column comparisons.
  CodeGen *codegen = GetCodeGen();
  int32_t ret_value;
  for (const auto &[expr, sort_order] : GetTypedPlan().GetSortKeys()) {
    if (sort_order == planner::OrderByOrderingType::ASC) {
      ret_value = -1;
    } else {
      ret_value = 1;
    }
    for (const auto tok : {parsing::Token::Type::LESS, parsing::Token::Type::GREATER}) {
      ast::Expr *lhs = left_ctx.DeriveValue(*expr);
      ast::Expr *rhs = right_ctx.DeriveValue(*expr);
      If cond(codegen, codegen->Compare(tok, lhs, rhs));
      {
        // Return the value depending on ordering.
        builder->Append(codegen->Return(codegen->Const32(ret_value)));
      }
      cond.EndIf();
      ret_value = -ret_value;
    }
  }
}

void SortTranslator::DefineHelperFunctions(TopLevelDeclarations *top_level_decls) {
  CodeGen *codegen = GetCodeGen();
  ast::Identifier fn_name = codegen->MakeFreshIdentifier("compare");

  // Params
  ast::Expr *sort_row_type = codegen->PointerType(sort_row_->Name());
  ast::FieldDecl *lhs = codegen->MakeField(codegen->MakeIdentifier("lhs"), sort_row_type);
  ast::FieldDecl *rhs = codegen->MakeField(codegen->MakeIdentifier("rhs"), sort_row_type);
  util::RegionVector<ast::FieldDecl *> params = codegen->MakeFieldList({lhs, rhs});
  FunctionBuilder builder(codegen, fn_name, std::move(params), codegen->Int32Type());
  {
    ast::Expr *lhs_row = builder.GetParameterByPosition(0);
    ast::Expr *rhs_row = builder.GetParameterByPosition(1);
    GenerateComparisonFunction(&builder, lhs_row, rhs_row);
  }
  cmp_func_ = builder.Finish(codegen->Const32(0));
  top_level_decls->RegisterFunction(cmp_func_);
}

void SortTranslator::InitializeSorter(ast::Expr *sorter_ptr) const {
  CodeGen *codegen = GetCodeGen();
  FunctionBuilder *func = codegen->CurrentFunction();
  ast::Expr *mem_pool = codegen->ExecCtxGetMemoryPool(GetExecutionContext());
  func->Append(codegen->SorterInit(sorter_ptr, mem_pool, cmp_func_->Name(), sort_row_->Name()));
}

void SortTranslator::TearDownSorter(ast::Expr *sorter_ptr) const {
  CodeGen *codegen = GetCodeGen();
  codegen->CurrentFunction()->Append(codegen->SorterFree(sorter_ptr));
}

void SortTranslator::InitializeQueryState() const {
  InitializeSorter(GetQueryState().GetStateEntryPtr(GetCodeGen(), sorter_slot_));
}

void SortTranslator::TearDownQueryState() const {
  TearDownSorter(GetQueryState().GetStateEntryPtr(GetCodeGen(), sorter_slot_));
}

void SortTranslator::DeclarePipelineState(PipelineContext *pipeline_context) {
  if (IsBottomPipeline(pipeline_context->GetPipeline()) && pipeline_context->IsParallel()) {
    ast::Expr *sorter_type = GetCodeGen()->BuiltinType(ast::BuiltinType::Kind::Sorter);
    tl_sorter_slot_ = pipeline_context->DeclareStateEntry(GetCodeGen(), "sorter", sorter_type);
  }
}

void SortTranslator::InitializePipelineState(const PipelineContext &pipeline_context) const {
  if (IsBottomPipeline(pipeline_context.GetPipeline()) && pipeline_context.IsParallel()) {
    InitializeSorter(pipeline_context.GetThreadStateEntryPtr(GetCodeGen(), tl_sorter_slot_));
  }
}

void SortTranslator::TearDownPipelineState(const PipelineContext &pipeline_context) const {
  if (IsBottomPipeline(pipeline_context.GetPipeline()) && pipeline_context.IsParallel()) {
    TearDownSorter(pipeline_context.GetThreadStateEntryPtr(GetCodeGen(), tl_sorter_slot_));
  }
}

void SortTranslator::FillSortRow(ConsumerContext *consumer_context, ast::Expr *sort_row) const {
  CodeGen *codegen = GetCodeGen();
  FunctionBuilder *builder = codegen->CurrentFunction();

  // For each child output, set the sorter attribute
  const auto child_schema = GetPlan().GetChild(0)->GetOutputSchema();
  for (uint32_t attr_idx = 0; attr_idx < child_schema->GetColumns().size(); attr_idx++) {
    ast::Identifier attr = GetCodeGen()->MakeIdentifier("attr" + std::to_string(attr_idx));
    ast::Expr *lhs = codegen->AccessStructMember(sort_row, attr);
    ast::Expr *rhs = consumer_context->DeriveValue(*child_schema->GetColumn(attr_idx).GetExpr());
    builder->Append(codegen->Assign(lhs, rhs));
  }
}

void SortTranslator::InsertIntoSorter(ConsumerContext *consumer_context) const {
  CodeGen *codegen = GetCodeGen();
  FunctionBuilder *func = codegen->CurrentFunction();

  // Collect correct sorter instance.
  ast::Expr *sorter = nullptr;
  if (consumer_context->GetPipeline().IsParallel()) {
    const auto pipeline_context = consumer_context->GetPipelineContext();
    sorter = pipeline_context->GetThreadStateEntryPtr(codegen, tl_sorter_slot_);
  } else {
    sorter = GetQueryState().GetStateEntryPtr(codegen, sorter_slot_);
  }

  ast::Identifier sort_row_name = codegen->MakeFreshIdentifier("sort_row");
  ast::Expr *sort_row = codegen->MakeExpr(sort_row_name);
  if (const auto &plan = GetTypedPlan(); plan.HasLimit()) {
    // @sorterInsertTopK()
    const std::size_t topk = plan.GetOffset() + plan.GetLimit();
    func->Append(codegen->DeclareVarWithInit(
        sort_row_name, codegen->SorterInsertTopK(sorter, sort_row_->Name(), topk)));
    // Fill row.
    FillSortRow(consumer_context, sort_row);
    // @sorterInsertTopKFinish();
    func->Append(codegen->SorterInsertTopKFinish(sorter));
  } else {
    // @sorterInsert()
    func->Append(codegen->DeclareVarWithInit(sort_row_name,
                                             codegen->SorterInsert(sorter, sort_row_->Name())));
    // Fill row.
    FillSortRow(consumer_context, sort_row);
  }
}

void SortTranslator::ScanSorter(ConsumerContext *consumer_context) const {
  CodeGen *codegen = GetCodeGen();
  FunctionBuilder *func = codegen->CurrentFunction();

  ast::Identifier sorter_iter_var = codegen->MakeFreshIdentifier("iter");
  ast::Expr *iter = codegen->AddressOf(codegen->MakeExpr(sorter_iter_var));
  ast::Expr *sorter = GetQueryState().GetStateEntryPtr(codegen, sorter_slot_);

  func->Append(codegen->DeclareVarNoInit(sorter_iter_var, ast::BuiltinType::Kind::SorterIterator));
  Loop loop(codegen, codegen->MakeStmt(codegen->SorterIterInit(iter, sorter)),
            codegen->SorterIterHasNext(iter), codegen->MakeStmt(codegen->SorterIterNext(iter)));
  {
    // Pull out current row from sorter iterator.
    ast::Identifier row_var = codegen->MakeFreshIdentifier("row");
    ast::Expr *row = codegen->MakeExpr(row_var);
    func->Append(codegen->DeclareVarWithInit(
        row_var, codegen->PtrCast(sort_row_->Name(), codegen->SorterIterGetRow(iter))));

    // Add attributes into context.
    std::vector<SortRowAccess> attrs;
    PopulateContextWithSortAttributes(consumer_context, row, &attrs);

    // Move along
    consumer_context->Push();
  }
  loop.EndLoop();
  func->Append(codegen->SorterIterClose(iter));
}

void SortTranslator::DoPipelineWork(ConsumerContext *consumer_context) const {
  if (IsTopPipeline(consumer_context->GetPipeline())) {
    ScanSorter(consumer_context);
  } else {
    InsertIntoSorter(consumer_context);
  }
}

void SortTranslator::FinishPipelineWork(const PipelineContext &pipeline_context) const {
  if (IsBottomPipeline(pipeline_context.GetPipeline()) && pipeline_context.IsParallel()) {
    CodeGen *codegen = GetCodeGen();
    FunctionBuilder *function = codegen->CurrentFunction();

    ast::Expr *sorter = GetQueryState().GetStateEntryPtr(codegen, sorter_slot_);
    ast::Expr *offset = pipeline_context.GetThreadStateEntryOffset(codegen, tl_sorter_slot_);

    if (const auto &plan = GetTypedPlan(); plan.HasLimit()) {
      const std::size_t topk = plan.GetOffset() + plan.GetLimit();
      function->Append(codegen->SortTopKParallel(sorter, GetThreadStateContainer(), offset, topk));
    } else {
      function->Append(codegen->SortParallel(sorter, GetThreadStateContainer(), offset));
    }
  }
}

}  // namespace tpl::sql::codegen
