#include "sql/codegen/operators/sort_translator.h"

#include <utility>

#include "sql/codegen/compilation_context.h"
#include "sql/codegen/consumer_context.h"
#include "sql/codegen/function_builder.h"
#include "sql/codegen/if.h"
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
  CodeGen *codegen = compilation_context->GetCodeGen();
  ast::Expr *sorter_type = codegen->BuiltinType(ast::BuiltinType::Kind::Sorter);
  sorter_ = compilation_context->GetQueryState()->DeclareStateEntry(codegen, "sorter", sorter_type);
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

namespace {

class SortRowAccess : public ConsumerContext::ValueProvider {
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

}  // namespace

void SortTranslator::GenerateComparisonFunction(FunctionBuilder *builder, ast::Expr *lhs_row,
                                                ast::Expr *rhs_row) const {
  CodeGen *codegen = GetCodeGen();

  // Setup the contexts.
  const auto num_cols = GetPlan().GetChild(0)->GetOutputSchema()->GetColumns().size();
  std::vector<SortRowAccess> attrs;
  attrs.resize(num_cols * 2);

  ConsumerContext left_ctx(GetCompilationContext(), nullptr);
  ConsumerContext right_ctx(GetCompilationContext(), nullptr);
  for (uint32_t i = 0; i < num_cols; i++) {
    ast::Identifier attr = codegen->MakeIdentifier("attr" + std::to_string(i));
    attrs[i] = SortRowAccess(codegen, lhs_row, attr);
    attrs[i + num_cols] = SortRowAccess(codegen, rhs_row, attr);
    left_ctx.RegisterColumnValueProvider(i, &attrs[i]);
    right_ctx.RegisterColumnValueProvider(i, &attrs[i + num_cols]);
  }

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
    GenerateComparisonFunction(&builder, builder.GetParameterByPosition(0),
                               builder.GetParameterByPosition(0));
  }
  cmp_func_ = builder.Finish(codegen->Const32(0));
  top_level_decls->RegisterFunction(cmp_func_);
}

void SortTranslator::InitializeQueryState() const {
  CodeGen *codegen = GetCodeGen();
  FunctionBuilder *func = codegen->CurrentFunction();
  ast::Expr *sorter_ptr = GetQueryState().GetStateEntryPtr(codegen, sorter_);
  ast::Expr *mem_pool = codegen->ExecCtxGetMemoryPool(GetExecutionContext());
  func->Append(codegen->SorterInit(sorter_ptr, mem_pool, cmp_func_->Name(), sort_row_->Name()));
}

void SortTranslator::TearDownQueryState() const {
  CodeGen *codegen = GetCodeGen();
  FunctionBuilder *func = codegen->CurrentFunction();
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
    CodeGen *codegen = GetCodeGen();
    FunctionBuilder *function = codegen->CurrentFunction();

    ast::Expr *sorter = GetQueryState().GetStateEntryPtr(codegen, sorter_);
    ast::Expr *offset = pipeline_context.GetThreadStateEntryOffset(codegen, tl_sorter_);

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
