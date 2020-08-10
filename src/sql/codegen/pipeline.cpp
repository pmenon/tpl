#include "sql/codegen/pipeline.h"

#include <algorithm>

#include "spdlog/fmt/fmt.h"

#include "ast/ast.h"
#include "common/macros.h"
#include "common/settings.h"
#include "logging/logger.h"
#include "sql/codegen/codegen.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/consumer_context.h"
#include "sql/codegen/function_builder.h"
#include "sql/codegen/operators/operator_translator.h"
#include "sql/codegen/pipeline_driver.h"
#include "sql/codegen/pipeline_graph.h"
#include "sql/planner/plannodes/abstract_plan_node.h"

namespace tpl::sql::codegen {

//===----------------------------------------------------------------------===//
//
// Pipeline Context
//
//===----------------------------------------------------------------------===//

PipelineContext::PipelineContext(const Pipeline &pipeline)
    : pipeline_(pipeline),
      codegen_(pipeline.GetCompilationContext()->GetCodeGen()),
      state_var_(codegen_->MakeIdentifier("pipelineState")),
      state_(codegen_->MakeIdentifier(fmt::format("P{}_State", pipeline_.GetId())),
             [this](CodeGen *codegen) { return codegen->MakeExpr(state_var_); }) {}

StateDescriptor::Slot PipelineContext::DeclarePipelineStateEntry(const std::string &name,
                                                                 ast::Expr *type_repr) {
  return state_.DeclareStateEntry(codegen_, name, type_repr);
}

ast::StructDecl *PipelineContext::ConstructPipelineStateType() {
  return state_.ConstructFinalType(codegen_);
}

ast::Expr *PipelineContext::GetStateEntry(StateDescriptor::Slot slot) const {
  return state_.GetStateEntry(codegen_, slot);
}

ast::Expr *PipelineContext::GetStateEntryPtr(StateDescriptor::Slot slot) const {
  return state_.GetStateEntryPtr(codegen_, slot);
}

ast::Expr *PipelineContext::GetStateEntryByteOffset(StateDescriptor::Slot slot) const {
  return state_.GetStateEntryOffset(codegen_, slot);
}

bool PipelineContext::IsForPipeline(const Pipeline &that) const { return pipeline_.IsSameAs(that); }

bool PipelineContext::IsParallel() const { return pipeline_.IsParallel(); }

bool PipelineContext::IsVectorized() const { return pipeline_.IsVectorized(); }

ast::Expr *PipelineContext::AccessCurrentThreadState() const {
  ast::Expr *exec_ctx = pipeline_.GetCompilationContext()->GetExecutionContextPtrFromQueryState();
  ast::Expr *tls = codegen_->ExecCtxGetTLS(exec_ctx);
  return codegen_->TLSAccessCurrentThreadState(tls, state_.GetTypeName());
}

util::RegionVector<ast::FieldDecl *> PipelineContext::PipelineParams() const {
  // The main query parameters.
  util::RegionVector<ast::FieldDecl *> query_params =
      pipeline_.GetCompilationContext()->QueryParams();
  // Tag on the pipeline state.
  ast::Expr *pipeline_state = codegen_->PointerType(codegen_->MakeExpr(state_.GetTypeName()));
  query_params.push_back(codegen_->MakeField(state_var_, pipeline_state));
  return query_params;
}

//===----------------------------------------------------------------------===//
//
// Pipeline
//
//===----------------------------------------------------------------------===//

Pipeline::Pipeline(CompilationContext *compilation_context, PipelineGraph *pipeline_graph)
    : id_(pipeline_graph->NextPipelineId()),
      compilation_ctx_(compilation_context),
      pipeline_graph_(pipeline_graph),
      codegen_(compilation_ctx_->GetCodeGen()),
      parallelism_(Parallelism::Parallel),
      check_parallelism_(true),
      type_(Type::Regular) {
  pipeline_graph_->RegisterPipeline(*this);
}

Pipeline::Pipeline(OperatorTranslator *op, PipelineGraph *pipeline_graph, Parallelism parallelism)
    : Pipeline(op->GetCompilationContext(), pipeline_graph) {
  RegisterStep(op);
}

void Pipeline::RegisterStep(OperatorTranslator *op) {
  TPL_ASSERT(std::count(operators_.begin(), operators_.end(), op) == 0,
             "Duplicate registration of operator in pipeline.");
  operators_.push_back(op);
}

void Pipeline::RegisterSource(PipelineDriver *driver, Pipeline::Parallelism parallelism) {
  driver_ = driver;
  UpdateParallelism(parallelism);
}

void Pipeline::UpdateParallelism(Pipeline::Parallelism parallelism) {
  if (check_parallelism_) {
    parallelism_ = std::min(parallelism, parallelism_);
  }
}

void Pipeline::SetParallelCheck(bool check) { check_parallelism_ = check; }

void Pipeline::RegisterExpression(ExpressionTranslator *expression) {
  TPL_ASSERT(std::find(expressions_.begin(), expressions_.end(), expression) == expressions_.end(),
             "Expression already registered in pipeline");
  expressions_.push_back(expression);
}

std::string Pipeline::CreatePipelineFunctionName(const std::string &func_name) const {
  auto result = fmt::format("{}_Pipeline{}", compilation_ctx_->GetFunctionPrefix(), id_);
  if (!func_name.empty()) {
    result += "_" + func_name;
  }
  return result;
}

void Pipeline::AddDependency(const Pipeline &dependency) const {
  pipeline_graph_->AddDependency(*this, dependency);
}

bool Pipeline::IsLastOperator(const OperatorTranslator &op) const { return operators_[0] == &op; }

void Pipeline::MarkNestedPipeline(Pipeline *parent) {
  type_ = Type::Nested;
  parent->child_pipelines_.push_back(this);
  parent_pipelines_.push_back(parent);
}

std::string Pipeline::BuildPipelineName() const {
  std::string result;

  bool first = true;
  for (auto iter = Begin(), end = End(); iter != end; ++iter) {
    if (!first) result += " --> ";
    first = false;
    std::string plan_type = planner::PlanNodeTypeToString((*iter)->GetPlan().GetPlanNodeType());
    std::transform(plan_type.begin(), plan_type.end(), plan_type.begin(), ::tolower);
    result.append(plan_type);
  }

  if (!child_pipelines_.empty()) {
    for (auto inner : child_pipelines_) {
      result += " --> { " + inner->BuildPipelineName() + " (nested) } ";
    }
  }

  return result;
}

void Pipeline::Prepare() {
  // Finalize the execution mode. We choose serial execution if ANY of the below
  // conditions are satisfied:
  //  1. If parallel execution is globally disabled.
  //  2. If the consumer doesn't support parallel execution.
  //  3. If ANY operator in the pipeline explicitly requested serial execution.

  const bool parallel_exec_disabled =
      !Settings::Instance()->GetBool(Settings::Name::ParallelQueryExecution);
  const bool parallel_consumer = true;
  if (parallel_exec_disabled || !parallel_consumer ||
      parallelism_ == Pipeline::Parallelism::Serial) {
    parallelism_ = Pipeline::Parallelism::Serial;
  } else {
    parallelism_ = Pipeline::Parallelism::Parallel;
  }

  LOG_INFO("Pipeline-{}: parallel={}, vectorized={}, operators=[{}]", id_, IsParallel(),
           IsVectorized(), BuildPipelineName());
}

ast::FunctionDecl *Pipeline::GenerateSetupPipelineStateFunction(
    PipelineContext *pipeline_ctx) const {
  auto name = codegen_->MakeIdentifier(CreatePipelineFunctionName("InitPipelineState"));
  FunctionBuilder builder(codegen_, name, pipeline_ctx->PipelineParams(), codegen_->Nil());
  {
    // Request new scope for the function.
    CodeGen::CodeScope code_scope(codegen_);
    for (auto op : operators_) {
      op->InitializePipelineState(*pipeline_ctx, &builder);
    }
  }
  return builder.Finish();
}

ast::FunctionDecl *Pipeline::GenerateTearDownPipelineStateFunction(
    PipelineContext *pipeline_ctx) const {
  auto name = codegen_->MakeIdentifier(CreatePipelineFunctionName("TearDownPipelineState"));
  FunctionBuilder builder(codegen_, name, pipeline_ctx->PipelineParams(), codegen_->Nil());
  {
    // Request new scope for the function.
    CodeGen::CodeScope code_scope(codegen_);
    for (auto op : operators_) {
      op->TearDownPipelineState(*pipeline_ctx, &builder);
    }
  }
  return builder.Finish();
}

ast::FunctionDecl *Pipeline::GenerateInitPipelineFunction(PipelineContext *pipeline_ctx) const {
  ast::FunctionDecl *setup_state_fn = GenerateSetupPipelineStateFunction(pipeline_ctx);
  ast::FunctionDecl *cleanup_state_fn = GenerateTearDownPipelineStateFunction(pipeline_ctx);

  auto name = codegen_->MakeIdentifier(CreatePipelineFunctionName("Init"));
  FunctionBuilder builder(codegen_, name, compilation_ctx_->QueryParams(), codegen_->Nil());
  {
    CodeGen::CodeScope code_scope(codegen_);
    // var tls = @execCtxGetTLS(exec_ctx)
    ast::Expr *exec_ctx = compilation_ctx_->GetExecutionContextPtrFromQueryState();
    ast::Identifier tls = codegen_->MakeFreshIdentifier("thread_state_container");
    builder.Append(codegen_->DeclareVarWithInit(tls, codegen_->ExecCtxGetTLS(exec_ctx)));

    // @tlsReset(tls, @sizeOf(ThreadState), init, tearDown, queryState)
    ast::Expr *state_ptr = compilation_ctx_->GetQueryState()->GetStatePointer(codegen_);
    ast::Decl *state_type = pipeline_ctx->ConstructPipelineStateType();
    builder.Append(codegen_->TLSReset(codegen_->MakeExpr(tls), state_type->Name(),
                                      setup_state_fn->Name(), cleanup_state_fn->Name(), state_ptr));
  }
  return builder.Finish();
}

ast::FunctionDecl *Pipeline::GeneratePipelineWorkFunction(PipelineContext *pipeline_ctx) const {
  util::RegionVector<ast::FieldDecl *> params = pipeline_ctx->PipelineParams();

  if (IsParallel()) {
    std::vector<ast::FieldDecl *> additional_params = driver_->GetWorkerParams();
    params.insert(params.end(), additional_params.begin(), additional_params.end());
  }

  auto name = codegen_->MakeIdentifier(
      CreatePipelineFunctionName(IsParallel() ? "ParallelWork" : "SerialWork"));
  FunctionBuilder builder(codegen_, name, std::move(params), codegen_->Nil());
  {
    // Begin a new code scope for fresh variables.
    CodeGen::CodeScope code_scope(codegen_);
    // Create the working context and push it through the pipeline.
    ConsumerContext context(compilation_ctx_, *pipeline_ctx);
    (*Begin())->Consume(&context, &builder);
  }
  return builder.Finish();
}

ast::FunctionDecl *Pipeline::GenerateRunPipelineFunction(PipelineContext *pipeline_ctx) const {
  // Generate the work function first.
  ast::FunctionDecl *work_function = GeneratePipelineWorkFunction(pipeline_ctx);

  auto name = codegen_->MakeIdentifier(CreatePipelineFunctionName("Run"));
  FunctionBuilder builder(codegen_, name, compilation_ctx_->QueryParams(), codegen_->Nil());
  {
    CodeGen::CodeScope code_scope(codegen_);

    for (auto op : operators_) {
      op->BeginPipelineWork(*pipeline_ctx, &builder);
    }

    if (IsParallel()) {
      driver_->LaunchWork(&builder, work_function->Name());
    } else {
      ast::Expr *q_state = builder.GetParameterByPosition(0);
      ast::Expr *p_state = pipeline_ctx->AccessCurrentThreadState();
      ast::Identifier p_state_name = codegen_->MakeFreshIdentifier("p_state");
      builder.Append(codegen_->DeclareVarWithInit(p_state_name, p_state));
      builder.Append(
          codegen_->Call(work_function->Name(), {q_state, codegen_->MakeExpr(p_state_name)}));
    }

    for (auto op : operators_) {
      op->FinishPipelineWork(*pipeline_ctx, &builder);
    }
  }
  return builder.Finish();
}

ast::FunctionDecl *Pipeline::GenerateTearDownPipelineFunction() const {
  auto name = codegen_->MakeIdentifier(CreatePipelineFunctionName("TearDown"));
  FunctionBuilder builder(codegen_, name, compilation_ctx_->QueryParams(), codegen_->Nil());
  {
    // Begin a new code scope for fresh variables.
    CodeGen::CodeScope code_scope(codegen_);
    // Tear down thread local state if parallel pipeline.
    ast::Expr *exec_ctx = compilation_ctx_->GetExecutionContextPtrFromQueryState();
    builder.Append(codegen_->TLSClear(codegen_->ExecCtxGetTLS(exec_ctx)));
  }
  return builder.Finish();
}

void Pipeline::DeclarePipelineState(PipelineContext *pipeline_ctx) const {
  for (auto op : operators_) {
    op->DeclarePipelineState(pipeline_ctx);
  }
  pipeline_ctx->ConstructPipelineStateType();
}

void Pipeline::DefinePipelineFunctions(PipelineContext *pipeline_ctx) const {
  for (auto op : operators_) {
    op->DefinePipelineFunctions(*pipeline_ctx);
  }
}

std::vector<ast::FunctionDecl *> Pipeline::GeneratePipelineLogic() const {
  PipelineContext pipeline_context(*this);

  DeclarePipelineState(&pipeline_context);

  DefinePipelineFunctions(&pipeline_context);

  ast::FunctionDecl *init_fn = GenerateInitPipelineFunction(&pipeline_context);

  ast::FunctionDecl *run_fn = GenerateRunPipelineFunction(&pipeline_context);

  ast::FunctionDecl *tear_down_fn = GenerateTearDownPipelineFunction();

  return {init_fn, run_fn, tear_down_fn};
}

}  // namespace tpl::sql::codegen
