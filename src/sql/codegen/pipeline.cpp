#include "sql/codegen/pipeline.h"

#include <algorithm>

#include "spdlog/fmt/fmt.h"

#include "common/macros.h"
#include "common/settings.h"
#include "logging/logger.h"
#include "sql/codegen/codegen.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/executable_query_builder.h"
#include "sql/codegen/function_builder.h"
#include "sql/codegen/operators/operator_translator.h"
#include "sql/codegen/work_context.h"
#include "sql/planner/plannodes/abstract_plan_node.h"

namespace tpl::sql::codegen {

Pipeline::Pipeline(CompilationContext *ctx)
    : id_(ctx->RegisterPipeline(this)),
      compilation_context_(ctx),
      parallelism_(Parallelism::Parallel),
      state_var_(GetCodeGen()->MakeIdentifier("pipelineState")),
      state_type_(GetCodeGen()->MakeIdentifier(fmt::format("P{}_State", id_))),
      state_(state_type_, [this](CodeGen *codegen) { return codegen->MakeExpr(state_var_); }) {}

Pipeline::Pipeline(OperatorTranslator *op, Pipeline::Parallelism parallelism)
    : Pipeline(op->GetCompilationContext()) {
  RegisterStep(op, parallelism);
}

CodeGen *Pipeline::GetCodeGen() { return compilation_context_->GetCodeGen(); }

void Pipeline::RegisterStep(OperatorTranslator *op, Parallelism parallelism) {
  steps_.push_back(op);
  parallelism_ = std::min(parallelism, parallelism_);
}

void Pipeline::RegisterExpression(ExpressionTranslator *expression) {
  TPL_ASSERT(std::find(expressions_.begin(), expressions_.end(), expression) == expressions_.end(),
             "Expression already registered in pipeline");
  expressions_.push_back(expression);
}

std::string Pipeline::ConstructPipelineFunctionName(const std::string &func_name) const {
  auto result = fmt::format("{}_Pipeline{}", compilation_context_->GetFunctionPrefix(), id_);
  if (!func_name.empty()) {
    result += "_" + func_name;
  }
  return result;
}

ast::Identifier Pipeline::GetSetupPipelineStateFunctionName() const {
  const auto &name = ConstructPipelineFunctionName("InitPipelineState");
  return compilation_context_->GetCodeGen()->MakeIdentifier(name);
}

ast::Identifier Pipeline::GetTearDownPipelineStateFunctionName() const {
  const auto &name = ConstructPipelineFunctionName("TearDownPipelineState");
  return compilation_context_->GetCodeGen()->MakeIdentifier(name);
}

ast::Identifier Pipeline::GetWorkFunctionName() const {
  const auto &name = ConstructPipelineFunctionName(IsParallel() ? "ParallelWork" : "SerialWork");
  return compilation_context_->GetCodeGen()->MakeIdentifier(name);
}

util::RegionVector<ast::FieldDecl *> Pipeline::PipelineParams() const {
  auto codegen = compilation_context_->GetCodeGen();
  // The main query parameters.
  util::RegionVector<ast::FieldDecl *> query_params = compilation_context_->QueryParams();
  // Tag on the pipeline state.
  ast::Expr *pipeline_state = codegen->PointerType(codegen->MakeExpr(state_.GetName()));
  query_params.push_back(codegen->MakeField(state_var_, pipeline_state));
  return query_params;
}

void Pipeline::LinkSourcePipeline(Pipeline *dependency) {
  TPL_ASSERT(dependency != nullptr, "Source cannot be null");
  dependencies_.push_back(dependency);
}

void Pipeline::CollectDependencies(std::vector<Pipeline *> *deps) {
  for (auto *pipeline : dependencies_) {
    pipeline->CollectDependencies(deps);
  }
  deps->push_back(this);
}

void Pipeline::Prepare() {
  // Finalize the pipeline state.
  state_.ConstructFinalType(GetCodeGen());

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

  LOG_INFO("Generated pipeline: {}", PrettyPrint());
}

ast::FunctionDecl *Pipeline::GenerateSetupPipelineStateFunction() const {
  auto codegen = compilation_context_->GetCodeGen();
  auto name = GetSetupPipelineStateFunctionName();
  FunctionBuilder builder(codegen, name, PipelineParams(), codegen->Nil());
  {
    // Request new scope for the function.
    CodeGen::CodeScope code_scope(codegen);
    for (auto *op : steps_) {
      op->InitializePipelineState(*this, &builder);
    }
  }
  return builder.Finish();
}

ast::FunctionDecl *Pipeline::GenerateTearDownPipelineStateFunction() const {
  auto codegen = compilation_context_->GetCodeGen();
  auto name = GetTearDownPipelineStateFunctionName();
  FunctionBuilder builder(codegen, name, PipelineParams(), codegen->Nil());
  {
    // Request new scope for the function.
    CodeGen::CodeScope code_scope(codegen);
    for (auto *op : steps_) {
      op->TearDownPipelineState(*this, &builder);
    }
  }
  return builder.Finish();
}

ast::FunctionDecl *Pipeline::GenerateInitPipelineFunction() const {
  auto codegen = compilation_context_->GetCodeGen();
  auto query_state = compilation_context_->GetQueryState();
  auto name = codegen->MakeIdentifier(ConstructPipelineFunctionName("Init"));
  FunctionBuilder builder(codegen, name, compilation_context_->QueryParams(), codegen->Nil());
  {
    CodeGen::CodeScope code_scope(codegen);
    // If this pipeline is not parallel there isn't any specific setup to do.
    // Otherwise, the pipeline requires some thread-local state which needs to
    // be initialized at this point. To do so, we'll call @tlsReset() and pass
    // in the thread-local state initialization and tear-down functions.
    if (IsParallel()) {
      // var tls = @execCtxGetTLS(exec_ctx)
      // @tlsReset(tls, @sizeOf(ThreadState), init, tearDown, queryState)
      ast::Expr *state_ptr = query_state->GetStatePointer(codegen);
      ast::Expr *exec_ctx = compilation_context_->GetExecutionContextPtrFromQueryState();
      ast::Identifier tls = codegen->MakeFreshIdentifier("tls");
      builder.Append(codegen->DeclareVarWithInit(tls, codegen->ExecCtxGetTLS(exec_ctx)));
      builder.Append(codegen->TLSReset(codegen->MakeExpr(tls), state_.GetName(),
                                       GetSetupPipelineStateFunctionName(),
                                       GetTearDownPipelineStateFunctionName(), state_ptr));
    }
  }
  return builder.Finish();
}

ast::FunctionDecl *Pipeline::GeneratePipelineWorkFunction() const {
  auto codegen = compilation_context_->GetCodeGen();
  auto params = PipelineParams();

  if (IsParallel()) {
    auto additional_params = (*Begin())->GetWorkerParams();
    params.insert(params.end(), additional_params.begin(), additional_params.end());
  }

  FunctionBuilder builder(codegen, GetWorkFunctionName(), std::move(params), codegen->Nil());
  {
    // Begin a new code scope for fresh variables.
    CodeGen::CodeScope code_scope(codegen);
    // Create the working context and push it through the pipeline.
    WorkContext work_context(compilation_context_, *this);
    Root()->PerformPipelineWork(&work_context, &builder);
  }
  return builder.Finish();
}

ast::FunctionDecl *Pipeline::GenerateRunPipelineFunction() const {
  auto codegen = compilation_context_->GetCodeGen();
  auto name = codegen->MakeIdentifier(ConstructPipelineFunctionName("Run"));
  FunctionBuilder builder(codegen, name, compilation_context_->QueryParams(), codegen->Nil());
  {
    // Begin a new code scope for fresh variables.
    CodeGen::CodeScope code_scope(codegen);

    // Let the operators perform some initialization work in this pipeline.
    for (auto op : steps_) {
      op->BeginPipelineWork(*this, &builder);
    }

    // Launch pipeline work.
    if (IsParallel()) {
      Root()->LaunchWork(&builder, GetWorkFunctionName());
    } else {
      const auto gen_args = [&]() -> std::vector<ast::Expr *> {
        ast::Expr *pipeline_state = codegen->MakeExpr(state_var_);
        return {builder.GetParameterByPosition(0), codegen->AddressOf(pipeline_state)};
      };
      builder.Append(codegen->DeclareVarNoInit(state_var_, codegen->MakeExpr(state_.GetName())));
      builder.Append(codegen->Call(GetSetupPipelineStateFunctionName(), gen_args()));
      builder.Append(codegen->Call(GetWorkFunctionName(), gen_args()));
      builder.Append(codegen->Call(GetTearDownPipelineStateFunctionName(), gen_args()));
    }

    // Let the operators perform some completion work in this pipeline.
    for (auto op : steps_) {
      op->FinishPipelineWork(*this, &builder);
    }
  }
  return builder.Finish();
}

ast::FunctionDecl *Pipeline::GenerateTearDownPipelineFunction() const {
  auto codegen = compilation_context_->GetCodeGen();
  auto name = codegen->MakeIdentifier(ConstructPipelineFunctionName("TearDown"));
  FunctionBuilder builder(codegen, name, compilation_context_->QueryParams(), codegen->Nil());
  {
    // Begin a new code scope for fresh variables.
    CodeGen::CodeScope code_scope(codegen);
    // Tear down thread local state if parallel pipeline.
    if (IsParallel()) {
      ast::Expr *exec_ctx = compilation_context_->GetExecutionContextPtrFromQueryState();
      builder.Append(codegen->TLSClear(codegen->ExecCtxGetTLS(exec_ctx)));
    }
  }
  return builder.Finish();
}

void Pipeline::GeneratePipeline(ExecutableQueryFragmentBuilder *builder) const {
  // Declare the pipeline state.
  builder->DeclareStruct(state_.GetType());

  // Generate pipeline state initialization and tear-down functions.
  builder->DeclareFunction(GenerateSetupPipelineStateFunction());
  builder->DeclareFunction(GenerateTearDownPipelineStateFunction());

  // Generate main pipeline logic.
  builder->DeclareFunction(GeneratePipelineWorkFunction());

  // Register the main init, run, tear-down functions as steps, in that order.
  builder->RegisterStep(GenerateInitPipelineFunction());
  builder->RegisterStep(GenerateRunPipelineFunction());
  builder->RegisterStep(GenerateTearDownPipelineFunction());
}

std::string Pipeline::PrettyPrint() const {
  std::string result;
  bool first = true;
  for (auto iter = Begin(), end = End(); iter != end; ++iter) {
    if (!first) result += " -> ";
    first = false;
    std::string plan_type = planner::PlanNodeTypeToString((*iter)->GetPlan().GetPlanNodeType());
    std::transform(plan_type.begin(), plan_type.end(), plan_type.begin(), ::tolower);
    result.append(plan_type);
  }
  return result;
}

}  // namespace tpl::sql::codegen
