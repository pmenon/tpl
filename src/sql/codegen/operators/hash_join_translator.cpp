#include "sql/codegen/operators/hash_join_translator.h"

#include "spdlog/fmt/fmt.h"

#include "ast/type.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/consumer_context.h"
#include "sql/codegen/function_builder.h"
#include "sql/codegen/if.h"
#include "sql/codegen/loop.h"
#include "sql/codegen/top_level_declarations.h"
#include "sql/planner/plannodes/hash_join_plan_node.h"

namespace tpl::sql::codegen {

namespace {
const char *kBuildRowAttrPrefix = "attr";
}  // namespace

HashJoinTranslator::HashJoinTranslator(const planner::HashJoinPlanNode &plan,
                                       CompilationContext *compilation_context, Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline),
      build_row_name_(GetCodeGen()->MakeFreshIdentifier("buildRow")),
      left_pipeline_(this, Pipeline::Parallelism::Flexible),
      build_row_(nullptr),
      key_check_(nullptr) {
  pipeline->RegisterStep(this, Pipeline::Parallelism::Flexible);
  pipeline->LinkSourcePipeline(LeftPipeline());

  compilation_context->Prepare(*plan.GetChild(0), LeftPipeline());
  compilation_context->Prepare(*plan.GetChild(1), RightPipeline());

  compilation_context->Prepare(*plan.GetJoinPredicate());
  for (const auto left_hash_key : plan.GetLeftHashKeys()) {
    compilation_context->Prepare(*left_hash_key);
  }
  for (const auto right_hash_key : plan.GetRightHashKeys()) {
    compilation_context->Prepare(*right_hash_key);
  }

  auto codegen = GetCodeGen();
  jht_slot_ = compilation_context->GetQueryState()->DeclareStateEntry(
      codegen, "joinHashTable", codegen->BuiltinType(ast::BuiltinType::JoinHashTable));
}

void HashJoinTranslator::DefineHelperStructs(TopLevelDeclarations *top_level_decls) {
  auto codegen = GetCodeGen();
  auto fields = codegen->MakeEmptyFieldList();
  GetAllChildOutputFields(0, kBuildRowAttrPrefix, &fields);
  if (GetPlanAs<planner::HashJoinPlanNode>().RequiresLeftMark()) {
    fields.push_back(codegen->MakeField(codegen->MakeFreshIdentifier("mark"), codegen->BoolType()));
  }
  build_row_ = codegen->DeclareStruct(codegen->MakeFreshIdentifier("BuildRow"), std::move(fields));
  top_level_decls->RegisterStruct(build_row_);
}

void HashJoinTranslator::DefineHelperFunctions(TopLevelDeclarations *top_level_decls) {
  // Signature: fun keyCheck(*QueryState, *VectorProjectionIterator, *BuildRow) -> bool
  auto codegen = GetCodeGen();
  auto params = GetCompilationContext()->QueryParams();
  params.push_back(
      codegen->MakeField(codegen->MakeFreshIdentifier("probeRow"),
                         codegen->PointerType(ast::BuiltinType::VectorProjectionIterator)));
  params.push_back(codegen->MakeField(codegen->MakeFreshIdentifier("tableRow"),
                                      codegen->PointerType(build_row_->Name())));
  FunctionBuilder func(codegen, codegen->MakeFreshIdentifier("keyCheck"), std::move(params),
                       codegen->BoolType());
  {
    ConsumerContext left_ctx(GetCompilationContext(), *LeftPipeline());
    //    auto match = left_ctx.DeriveValue(*Op<planner::HashJoinPlanNode>().GetJoinPredicate());
    //    func.Append(codegen->Return(match));
  }
  key_check_ = func.Finish(codegen->ConstBool(true));
  top_level_decls->RegisterFunction(key_check_);
}

void HashJoinTranslator::InitializeJoinHashTable(ast::Expr *jht_ptr) const {
  auto codegen = GetCodeGen();
  auto func = codegen->CurrentFunction();
  func->Append(codegen->JoinHashTableInit(jht_ptr, GetMemoryPool(), build_row_->Name()));
}

void HashJoinTranslator::TearDownJoinHashTable(ast::Expr *jht_ptr) const {
  auto codegen = GetCodeGen();
  auto func = codegen->CurrentFunction();
  func->Append(codegen->JoinHashTableFree(jht_ptr));
}

void HashJoinTranslator::InitializeQueryState() const {
  InitializeJoinHashTable(GetQueryStateEntryPtr(jht_slot_));
}

void HashJoinTranslator::TearDownQueryState() const {
  TearDownJoinHashTable(GetQueryStateEntryPtr(jht_slot_));
}

void HashJoinTranslator::DeclarePipelineState(PipelineContext *pipeline_context) {
  if (IsLeftPipeline(pipeline_context->GetPipeline()) && LeftPipeline()->IsParallel()) {
    auto codegen = GetCodeGen();
    tl_jht_slot_ = pipeline_context->DeclareStateEntry(
        codegen, "joinHashTable", codegen->BuiltinType(ast::BuiltinType::JoinHashTable));
  }
}

void HashJoinTranslator::InitializePipelineState(const PipelineContext &pipeline_context) const {
  if (IsLeftPipeline(pipeline_context.GetPipeline()) && LeftPipeline().IsParallel()) {
    InitializeJoinHashTable(pipeline_context.GetThreadStateEntryPtr(GetCodeGen(), tl_jht_slot_));
  }
}

void HashJoinTranslator::TearDownPipelineState(const PipelineContext &pipeline_context) const {
  if (IsLeftPipeline(pipeline_context.GetPipeline()) && LeftPipeline().IsParallel()) {
    TearDownJoinHashTable(pipeline_context.GetThreadStateEntryPtr(GetCodeGen(), tl_jht_slot_));
  }
}

ast::Expr *HashJoinTranslator::HashKeys(
    ConsumerContext *ctx, const std::vector<const planner::AbstractExpression *> &hash_keys) const {
  auto codegen = GetCodeGen();
  auto func = codegen->CurrentFunction();

  std::vector<ast::Expr *> key_values;
  key_values.reserve(hash_keys.size());
  for (const auto hash_key : hash_keys) {
    key_values.push_back(ctx->DeriveValue(*hash_key, this));
  }

  auto hash_val_name = codegen->MakeFreshIdentifier("hashVal");
  func->Append(codegen->DeclareVarWithInit(hash_val_name, codegen->Hash(key_values)));

  return codegen->MakeExpr(hash_val_name);
}

ast::Expr *HashJoinTranslator::GetBuildRowAttribute(ast::Expr *build_row, uint32_t attr_idx) const {
  auto codegen = GetCodeGen();
  auto attr_name = codegen->MakeIdentifier(fmt::format("{}{}", kBuildRowAttrPrefix, attr_idx));
  return codegen->AccessStructMember(build_row, attr_name);
}

void HashJoinTranslator::FillBuildRow(ConsumerContext *ctx, ast::Expr *build_row) const {
  auto codegen = GetCodeGen();
  auto func = codegen->CurrentFunction();

  const auto child_schema = GetPlan().GetChild(0)->GetOutputSchema();
  for (uint32_t attr_idx = 0; attr_idx < child_schema->GetColumns().size(); attr_idx++) {
    auto lhs = GetBuildRowAttribute(build_row, attr_idx);
    auto rhs = GetChildOutput(ctx, 0, attr_idx);
    func->Append(codegen->Assign(lhs, rhs));
  }
}

void HashJoinTranslator::InsertIntoJoinHashTable(ConsumerContext *ctx) const {
  auto codegen = GetCodeGen();
  auto func = codegen->CurrentFunction();

  ast::Expr *jht = nullptr;
  if (ctx->GetPipeline().IsParallel()) {
    jht = ctx->GetPipelineContext()->GetThreadStateEntryPtr(codegen, tl_jht_slot_);
  } else {
    jht = GetQueryStateEntryPtr(jht_slot_);
  }

  // Insert into hash table.
  auto hash_val = HashKeys(ctx, GetPlanAs<planner::HashJoinPlanNode>().GetLeftHashKeys());
  func->Append(codegen->DeclareVarWithInit(
      build_row_name_, codegen->JoinHashTableInsert(jht, hash_val, build_row_->Name())));

  // Fill row.
  FillBuildRow(ctx, codegen->MakeExpr(build_row_name_));
}

void HashJoinTranslator::ProbeJoinHashTable(ConsumerContext *ctx) const {
  auto codegen = GetCodeGen();
  auto func = codegen->CurrentFunction();

  auto iter_name_base = codegen->MakeFreshIdentifier("entryIterBase");
  func->Append(codegen->DeclareVarNoInit(iter_name_base, ast::BuiltinType::HashTableEntryIterator));

  auto iter_name = codegen->MakeFreshIdentifier("entryIter");
  func->Append(codegen->DeclareVarWithInit(iter_name,
                                           codegen->AddressOf(codegen->MakeExpr(iter_name_base))));

  auto query_state_ptr = GetQueryStatePtr();
  auto jht = GetQueryStateEntryPtr(jht_slot_);
  auto entry_iter = codegen->MakeExpr(iter_name);
  auto hash_val = HashKeys(ctx, GetPlanAs<planner::HashJoinPlanNode>().GetRightHashKeys());

  // Loop over matches.
  Loop entry_loop(codegen,
                  codegen->MakeStmt(codegen->JoinHashTableLookup(jht, entry_iter, hash_val)),
                  codegen->HTEntryIterHasNext(entry_iter, key_check_->Name(), query_state_ptr,
                                              codegen->Const64(0)),
                  nullptr);
  {
    // Push matches?
    ctx->Push();
  }
  entry_loop.EndLoop();
}

void HashJoinTranslator::DoPipelineWork(ConsumerContext *ctx) const {
  if (IsLeftPipeline(ctx->GetPipeline())) {
    InsertIntoJoinHashTable(ctx);
  } else {
    TPL_ASSERT(IsRightPipeline(ctx->GetPipeline()), "Pipeline is unknown to join translator");
    ProbeJoinHashTable(ctx);
  }
}

void HashJoinTranslator::FinishPipelineWork(const PipelineContext &pipeline_context) const {
  if (IsLeftPipeline(pipeline_context.GetPipeline())) {
    auto codegen = GetCodeGen();
    auto func = codegen->CurrentFunction();
    auto jht = GetQueryStateEntryPtr(jht_slot_);
    if (LeftPipeline().IsParallel()) {
      auto tls = GetThreadStateContainer();
      auto offset = pipeline_context.GetThreadStateEntryOffset(codegen, tl_jht_slot_);
      func->Append(codegen->JoinHashTableBuildParallel(jht, tls, offset));
    } else {
      func->Append(codegen->JoinHashTableBuild(jht));
    }
  }
}

ast::Expr *HashJoinTranslator::GetOutput(ConsumerContext *consumer_context,
                                         uint32_t attr_idx) const {
  const auto &output_col = GetPlan().GetOutputSchema()->GetColumn(attr_idx);
  return consumer_context->DeriveValue(*output_col.GetExpr(), this);
}

ast::Expr *HashJoinTranslator::GetChildOutput(ConsumerContext *consumer_context, uint32_t child_idx,
                                              uint32_t attr_idx) const {
  if (IsLeftPipeline(consumer_context->GetPipeline())) {
    // Propagate to left child.
    const auto child = GetPlan().GetChild(0);
    const auto child_translator = GetCompilationContext()->LookupTranslator(*child);
    return child_translator->GetOutput(consumer_context, attr_idx);
  } else {
    // If the requested output is for the left child, the parent actually wants
    // the attribute from the build-row we've pulled out of the hash table.
    auto codegen = GetCodeGen();
    if (child_idx == 0) {
      return GetBuildRowAttribute(codegen->MakeExpr(build_row_name_), attr_idx);
    }
    // The parent wants an attribute from the right child. Propagate to child.
    const auto child = GetPlan().GetChild(child_idx);
    const auto child_translator = GetCompilationContext()->LookupTranslator(*child);
    return child_translator->GetOutput(consumer_context, attr_idx);
  }
}

}  // namespace tpl::sql::codegen
