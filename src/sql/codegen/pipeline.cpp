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

//===----------------------------------------------------------------------===//
//
// Pipeline Context
//
//===----------------------------------------------------------------------===//

PipelineContext::PipelineContext(const Pipeline &pipeline, ast::Identifier state_var,
                                 ast::Identifier state_type)
    : pipeline_(pipeline),
      state_var_(state_var),
      pipeline_state_access_(this),
      pipeline_state_(state_type, &pipeline_state_access_) {}

ast::Expr *PipelineContext::GetPipelineStatePtr(CodeGen *codegen) const {
  return codegen->MakeExpr(state_var_);
}

StateDescriptor::Slot PipelineContext::DeclareStateEntry(CodeGen *codegen, const std::string &name,
                                                         ast::Expr *type) {
  return pipeline_state_.DeclareStateEntry(codegen, name, type);
}

ast::StructDecl *PipelineContext::ConstructPipelineStateType(CodeGen *codegen) {
  return pipeline_state_.ConstructFinalType(codegen);
}

ast::Expr *PipelineContext::GetThreadStateEntry(CodeGen *codegen,
                                                const StateDescriptor::Slot slot) const {
  return pipeline_state_.GetStateEntry(codegen, slot);
}

ast::Expr *PipelineContext::GetThreadStateEntryPtr(CodeGen *codegen,
                                                   const StateDescriptor::Slot slot) const {
  return pipeline_state_.GetStateEntryPtr(codegen, slot);
}

ast::Expr *PipelineContext::GetThreadStateEntryOffset(CodeGen *codegen,
                                                      const StateDescriptor::Slot slot) const {
  return pipeline_state_.GetStateEntryOffset(codegen, slot);
}

bool PipelineContext::IsParallel() const { return pipeline_.IsParallel(); }

//===----------------------------------------------------------------------===//
//
// Pipeline
//
//===----------------------------------------------------------------------===//

Pipeline::Pipeline(CompilationContext *ctx)
    : id_(ctx->RegisterPipeline(this)),
      compilation_context_(ctx),
      parallelism_(Parallelism::Flexible),
      state_var_(compilation_context_->GetCodeGen()->MakeIdentifier("pipelineState")),
      state_type_(compilation_context_->GetCodeGen()->MakeIdentifier("P" + std::to_string(id_) +
                                                                     "_State")) {}

Pipeline::Pipeline(OperatorTranslator *op, Pipeline::Parallelism parallelism)
    : Pipeline(op->GetCompilationContext()) {
  RegisterStep(op, parallelism);
}

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
  ast::Expr *pipeline_state = codegen->PointerType(codegen->MakeExpr(state_type_));
  query_params.push_back(codegen->MakeField(state_var_, pipeline_state));
  return query_params;
}

void Pipeline::RegisterSource(OperatorTranslator *op, Pipeline::Parallelism parallelism) {
  // Add the source to the pipeline.
  RegisterStep(op, parallelism);

  // Check parallel execution settings.
  bool parallel_exec_disabled =
      !Settings::Instance()->GetBool(Settings::Name::ParallelQueryExecution);

  // Check if the consumer supports parallel execution.
  bool parallel_consumer = true;

  // We choose serial execution for one of four reasons:
  //  1. If parallel execution is globally disabled.
  //  2. If the consumer isn't parallel.
  //  3. If the source of the pipeline explicitly requests serial execution.
  //  4. If any other operator in the pipeline explicitly requested serial
  //     execution.
  if (parallel_exec_disabled || !parallel_consumer ||
      parallelism == Pipeline::Parallelism::Serial ||
      parallelism_ == Pipeline::Parallelism::Serial) {
    parallelism_ = Pipeline::Parallelism::Serial;
    return;
  }

  // At this point, the pipeline (including its source) is either entirely
  // parallel or entirely flexible. For us, "flexible" parallelism isn't a real
  // thing; it is neither parallel or serial. We make a conservative choice
  // preferring serial execution unless the source specifically, clearly, and
  // explicitly requested parallel execution.

  if (parallelism == Pipeline::Parallelism::Flexible) {
    LOG_WARN(
        "Pipeline '{}' source ({}) chose Flexible parallelism rather than committing to Serial or "
        "Parallel. A Serial execution mode was chosen to err on the side of caution.",
        id_, planner::PlanNodeTypeToString(op->GetPlan().GetPlanNodeType()));
    parallelism_ = Pipeline::Parallelism::Serial;
  } else {
    parallelism_ = Pipeline::Parallelism::Parallel;
  }

  LOG_INFO("Generated pipeline: {}", PrettyPrint());
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

ast::FunctionDecl *Pipeline::GenerateSetupPipelineStateFunction(
    PipelineContext *pipeline_context) const {
  auto codegen = compilation_context_->GetCodeGen();
  auto name = GetSetupPipelineStateFunctionName();
  FunctionBuilder builder(codegen, name, PipelineParams(), codegen->Nil());
  {
    // Request new scope for the function.
    CodeGen::CodeScope code_scope(codegen);
    for (auto *op : steps_) {
      op->InitializePipelineState(*pipeline_context);
    }
  }
  return builder.Finish();
}

ast::FunctionDecl *Pipeline::GenerateTearDownPipelineStateFunction(
    PipelineContext *pipeline_context) const {
  auto codegen = compilation_context_->GetCodeGen();
  auto name = GetTearDownPipelineStateFunctionName();
  FunctionBuilder builder(codegen, name, PipelineParams(), codegen->Nil());
  {
    // Request new scope for the function.
    CodeGen::CodeScope code_scope(codegen);
    for (auto *op : steps_) {
      op->TearDownPipelineState(*pipeline_context);
    }
  }
  return builder.Finish();
}

ast::FunctionDecl *Pipeline::GenerateInitPipelineFunction(PipelineContext *pipeline_context) const {
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
      TPL_ASSERT(pipeline_context->GetLocalState()->GetType() != nullptr,
                 "Pipeline state has not been finalized yet");

      // var tls = @execCtxGetTLS(exec_ctx)
      // @tlsReset(tls, @sizeOf(ThreadState), init, tearDown, queryState)
      ast::Expr *state_ptr = query_state->GetStatePointer(codegen);
      ast::Expr *exec_ctx = compilation_context_->GetExecutionContextPtrFromQueryState();
      ast::Identifier tls = codegen->MakeFreshIdentifier("tls");
      builder.Append(codegen->DeclareVarWithInit(tls, codegen->ExecCtxGetTLS(exec_ctx)));
      builder.Append(codegen->TLSReset(codegen->MakeExpr(tls), state_type_,
                                       GetSetupPipelineStateFunctionName(),
                                       GetTearDownPipelineStateFunctionName(), state_ptr));
    }
  }
  return builder.Finish();
}

ast::FunctionDecl *Pipeline::GeneratePipelineWorkFunction(PipelineContext *pipeline_context) const {
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
    WorkContext work_context(compilation_context_, pipeline_context);
    (*Begin())->PerformPipelineWork(&work_context);
  }
  return builder.Finish();
}

ast::FunctionDecl *Pipeline::GenerateRunPipelineFunction(PipelineContext *pipeline_context) const {
  auto codegen = compilation_context_->GetCodeGen();
  auto name = codegen->MakeIdentifier(ConstructPipelineFunctionName("Run"));
  FunctionBuilder builder(codegen, name, compilation_context_->QueryParams(), codegen->Nil());
  {
    // Begin a new code scope for fresh variables.
    CodeGen::CodeScope code_scope(codegen);

    // Let the operators perform some initialization work in this pipeline.
    for (auto op : steps_) {
      op->BeginPipelineWork(*pipeline_context);
    }

    // Launch pipeline work.
    if (IsParallel()) {
      (*Begin())->LaunchWork(GetWorkFunctionName());
    } else {
      auto state_ptr = builder.GetParameterByPosition(0);
      auto thread_state_ptr = codegen->Const64(0);
      builder.Append(codegen->Call(GetWorkFunctionName(), {state_ptr, thread_state_ptr}));
    }

    // Let the operators perform some completion work in this pipeline.
    for (auto op : steps_) {
      op->FinishPipelineWork(*pipeline_context);
    }
  }
  return builder.Finish();
}

ast::FunctionDecl *Pipeline::GenerateTearDownPipelineFunction(
    PipelineContext *pipeline_context) const {
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
  PipelineContext pipeline_context(*this, state_var_, state_type_);

  // Collect all pipeline state and register it in the container.
  for (auto *op : steps_) {
    op->DeclarePipelineState(&pipeline_context);
  }

  auto codegen = compilation_context_->GetCodeGen();
  builder->DeclareStruct(pipeline_context.ConstructPipelineStateType(codegen));

  // Generate pipeline state initialization and tear-down functions.
  builder->DeclareFunction(GenerateSetupPipelineStateFunction(&pipeline_context));
  builder->DeclareFunction(GenerateTearDownPipelineStateFunction(&pipeline_context));

  // Generate main pipeline logic.
  builder->DeclareFunction(GeneratePipelineWorkFunction(&pipeline_context));

  // Register the main init, run, tear-down functions as steps, in that order.
  builder->RegisterStep(GenerateInitPipelineFunction(&pipeline_context));
  builder->RegisterStep(GenerateRunPipelineFunction(&pipeline_context));
  builder->RegisterStep(GenerateTearDownPipelineFunction(&pipeline_context));
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
