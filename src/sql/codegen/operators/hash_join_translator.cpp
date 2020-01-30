#include "sql/codegen/operators/hash_join_translator.h"

#include "ast/type.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/function_builder.h"
#include "sql/codegen/if.h"
#include "sql/codegen/loop.h"
#include "sql/codegen/work_context.h"
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
      build_row_(nullptr) {
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

void HashJoinTranslator::DefineHelperStructs(
    util::RegionVector<ast::StructDecl *> *top_level_structs) {
  auto codegen = GetCodeGen();
  auto fields = codegen->MakeEmptyFieldList();
  GetAllChildOutputFields(0, kBuildRowAttrPrefix, &fields);
  if (GetPlanAs<planner::HashJoinPlanNode>().RequiresLeftMark()) {
    fields.push_back(codegen->MakeField(codegen->MakeFreshIdentifier("mark"), codegen->BoolType()));
  }
  build_row_ = codegen->DeclareStruct(codegen->MakeFreshIdentifier("BuildRow"), std::move(fields));
  top_level_structs->push_back(build_row_);
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
    WorkContext *ctx, const std::vector<const planner::AbstractExpression *> &hash_keys) const {
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
  auto attr_name = codegen->MakeIdentifier(kBuildRowAttrPrefix + std::to_string(attr_idx));
  return codegen->AccessStructMember(build_row, attr_name);
}

void HashJoinTranslator::FillBuildRow(WorkContext *ctx, ast::Expr *build_row) const {
  auto codegen = GetCodeGen();
  auto func = codegen->CurrentFunction();

  const auto child_schema = GetPlan().GetChild(0)->GetOutputSchema();
  for (uint32_t attr_idx = 0; attr_idx < child_schema->GetColumns().size(); attr_idx++) {
    auto lhs = GetBuildRowAttribute(build_row, attr_idx);
    auto rhs = GetChildOutput(ctx, 0, attr_idx);
    func->Append(codegen->Assign(lhs, rhs));
  }
}

void HashJoinTranslator::InsertIntoJoinHashTable(WorkContext *ctx) const {
  auto codegen = GetCodeGen();
  auto func = codegen->CurrentFunction();

  ast::Expr *jht = nullptr;
  if (ctx->GetPipeline().IsParallel()) {
    jht = ctx->GetThreadStateEntryPtr(codegen, tl_jht_slot_);
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

void HashJoinTranslator::ProbeJoinHashTable(WorkContext *ctx) const {
  auto codegen = GetCodeGen();
  auto func = codegen->CurrentFunction();

  auto iter_name_base = codegen->MakeFreshIdentifier("entryIterBase");
  func->Append(codegen->DeclareVarNoInit(iter_name_base, ast::BuiltinType::HashTableEntryIterator));

  auto iter_name = codegen->MakeFreshIdentifier("entryIter");
  func->Append(codegen->DeclareVarWithInit(iter_name,
                                           codegen->AddressOf(codegen->MakeExpr(iter_name_base))));

  auto jht = GetQueryStateEntryPtr(jht_slot_);
  auto entry_iter = codegen->MakeExpr(iter_name);
  auto hash_val = HashKeys(ctx, GetPlanAs<planner::HashJoinPlanNode>().GetRightHashKeys());

  // Loop over matches.
  Loop entry_loop(codegen,
                  codegen->MakeStmt(codegen->JoinHashTableLookup(jht, entry_iter, hash_val)),
                  codegen->HTEntryIterHasNext(entry_iter), nullptr);
  {
    // var buildRow = @ptrCast(*BuildRow, @htEntryIterGetRow())
    func->Append(codegen->DeclareVarWithInit(
        build_row_name_, codegen->HTEntryIterGetRow(entry_iter, build_row_->Name())));

    // Check predicate
    if (const auto join_predicate = GetPlanAs<planner::HashJoinPlanNode>().GetJoinPredicate()) {
      auto cond = ctx->DeriveValue(*join_predicate, this);
      If check_condition(codegen, cond);
      ctx->Push();
    } else {
      ctx->Push();
    }
  }
  entry_loop.EndLoop();
}

void HashJoinTranslator::PerformPipelineWork(WorkContext *ctx) const {
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

ast::Expr *HashJoinTranslator::GetChildOutput(WorkContext *work_context, uint32_t child_idx,
                                              uint32_t attr_idx) const {
  if (IsRightPipeline(work_context->GetPipeline()) && child_idx == 0) {
    return GetBuildRowAttribute(GetCodeGen()->MakeExpr(build_row_name_), attr_idx);
  } else {
    const auto child = GetPlan().GetChild(child_idx);
    const auto child_translator = GetCompilationContext()->LookupTranslator(*child);
    return child_translator->GetOutput(work_context, attr_idx);
  }
}

}  // namespace tpl::sql::codegen
