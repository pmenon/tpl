#include "sql/codegen/pipeline.h"

#include <algorithm>

#include "spdlog/fmt/fmt.h"

#include "common/macros.h"
#include "common/settings.h"
#include "logging/logger.h"
#include "sql/codegen/code_container.h"
#include "sql/codegen/codegen.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/function_builder.h"
#include "sql/codegen/operators/operator_translator.h"
#include "sql/codegen/work_context.h"
#include "sql/planner/plannodes/abstract_plan_node.h"

namespace tpl::sql::codegen {

//===----------------------------------------------------------------------===//
//
// Pipeline Context - State Scope
//
//===----------------------------------------------------------------------===//

PipelineContext::StateScope::StateScope(PipelineContext *pipeline_context,
                                        ast::Expr *thread_state_ptr)
    : ctx_(pipeline_context), prev_thread_state_ptr_(pipeline_context->thread_state_ptr_) {
  ctx_->thread_state_ptr_ = thread_state_ptr;
}

PipelineContext::StateScope::~StateScope() { ctx_->thread_state_ptr_ = prev_thread_state_ptr_; }

//===----------------------------------------------------------------------===//
//
// Pipeline Context
//
//===----------------------------------------------------------------------===//

PipelineContext::PipelineContext(const Pipeline &pipeline, ast::Identifier state_name)
    : pipeline_(pipeline), state_name_(state_name), thread_state_ptr_(nullptr) {}

PipelineContext::Slot PipelineContext::DeclareStateEntry(CodeGen *codegen, const std::string &name,
                                                         ast::Expr *type) {
  const auto slot_id = slots_.size();
  slots_.emplace_back(std::make_pair(codegen->MakeFreshIdentifier(name), type));
  return slot_id;
}

ast::StructDecl *PipelineContext::ConstructPipelineStateType(CodeGen *codegen) {
  auto fields = codegen->MakeEmptyFieldList();
  for (auto &[name, type] : slots_) {
    fields.push_back(codegen->MakeField(name, type));
  }
  return codegen->DeclareStruct(state_name_, std::move(fields));
}

ast::Expr *PipelineContext::GetThreadStateEntry(CodeGen *codegen,
                                                PipelineContext::Slot slot) const {
  return codegen->AccessStructMember(thread_state_ptr_, slots_[slot].first);
}

ast::Expr *PipelineContext::GetThreadStateEntryPtr(CodeGen *codegen,
                                                   PipelineContext::Slot slot) const {
  return codegen->AddressOf(GetThreadStateEntry(codegen, slot));
}

ast::Expr *PipelineContext::GetThreadStateEntryOffset(CodeGen *codegen,
                                                      PipelineContext::Slot slot) const {
  return codegen->OffsetOf(state_name_, slots_[slot].first);
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
      pipeline_state_var_(compilation_context_->GetCodeGen()->MakeIdentifier("pipelineState")),
      pipeline_state_type_name_(
          compilation_context_->GetCodeGen()->MakeIdentifier(fmt::format("P{}_State", id_))) {}

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

util::RegionVector<ast::FieldDecl *> Pipeline::PipelineArgs() const {
  auto codegen = compilation_context_->GetCodeGen();
  // The main query parameters.
  util::RegionVector<ast::FieldDecl *> query_params = compilation_context_->QueryParams();
  // Tag on the pipeline state.
  ast::Expr *pipeline_state = codegen->PointerType(codegen->MakeExpr(pipeline_state_type_name_));
  query_params.push_back(codegen->MakeField(pipeline_state_var_, pipeline_state));
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
  FunctionBuilder builder(codegen, name, PipelineArgs(), codegen->Nil());
  {
    // Request new scope for the function.
    CodeGen::CodeScope code_scope(codegen);
    // Set the thread state.
    PipelineContext::StateScope state_scope(pipeline_context,
                                            codegen->MakeExpr(pipeline_state_var_));
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
  FunctionBuilder builder(codegen, name, PipelineArgs(), codegen->Nil());
  {
    // Request new scope for the function.
    CodeGen::CodeScope code_scope(codegen);
    // Set the thread state.
    PipelineContext::StateScope state_scope(pipeline_context,
                                            codegen->MakeExpr(pipeline_state_var_));
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
      builder.Append(codegen->TLSReset(codegen->MakeExpr(tls), pipeline_state_type_name_,
                                       GetSetupPipelineStateFunctionName(),
                                       GetTearDownPipelineStateFunctionName(), state_ptr));
    }
  }
  return builder.Finish();
}

ast::FunctionDecl *Pipeline::GeneratePipelineWorkFunction(PipelineContext *pipeline_context) const {
  auto codegen = compilation_context_->GetCodeGen();
  auto params = PipelineArgs();

  if (IsParallel()) {
    auto additional_params = Source()->GetWorkerParams();
    params.insert(params.end(), additional_params.begin(), additional_params.end());
  }

  FunctionBuilder work(codegen, GetWorkFunctionName(), std::move(params), codegen->Nil());
  {
    // Begin a new code scope for fresh variables.
    CodeGen::CodeScope code_scope(codegen);
    // Set the state in this scope.
    PipelineContext::StateScope state_scope(pipeline_context,
                                            codegen->MakeExpr(pipeline_state_var_));
    WorkContext work_context(compilation_context_, pipeline_context);
    Source()->PerformPipelineWork(&work_context);
  }
  return work.Finish();
}

ast::FunctionDecl *Pipeline::GenerateRunPipelineFunction(PipelineContext *pipeline_context) const {
  auto codegen = compilation_context_->GetCodeGen();
  auto name = codegen->MakeIdentifier(ConstructPipelineFunctionName("Run"));
  FunctionBuilder plan(codegen, name, compilation_context_->QueryParams(), codegen->Nil());
  {
    // Begin a new code scope for fresh variables.
    CodeGen::CodeScope code_scope(codegen);

    // Let the operators perform some initialization work in this pipeline.
    for (auto op : steps_) {
      op->BeginPipelineWork(*pipeline_context);
    }

    // Launch pipeline work.
    if (IsParallel()) {
      Source()->LaunchWork(GetWorkFunctionName());
    } else {
      auto state_ptr = plan.GetParameterByPosition(0);
      auto thread_state_ptr = codegen->Const64(0);
      plan.Append(codegen->Call(GetWorkFunctionName(), {state_ptr, thread_state_ptr}));
    }

    // Let the operators perform some completion work in this pipeline.
    for (auto op : steps_) {
      op->FinishPipelineWork(*pipeline_context);
    }
  }
  return plan.Finish();
}

ast::FunctionDecl *Pipeline::GenerateTearDownPipelineFunction(
    PipelineContext *pipeline_context) const {
  auto codegen = compilation_context_->GetCodeGen();
  auto name = codegen->MakeIdentifier(ConstructPipelineFunctionName("TearDown"));
  FunctionBuilder clean_up(codegen, name, compilation_context_->QueryParams(), codegen->Nil());
  {
    // Begin a new code scope for fresh variables.
    CodeGen::CodeScope code_scope(codegen);
    // Nothing, for now ...
    (void)pipeline_context;
  }
  return clean_up.Finish();
}

void Pipeline::GeneratePipeline(CodeContainer *code_container) const {
  PipelineContext pipeline_context(*this, pipeline_state_type_name_);

  // Collect all pipeline state and register it in the container.
  for (auto *op : steps_) {
    op->DeclarePipelineState(&pipeline_context);
  }

  auto codegen = compilation_context_->GetCodeGen();
  code_container->RegisterStruct(pipeline_context.ConstructPipelineStateType(codegen));

  // Generate pipeline state initialization and tear-down functions.
  code_container->RegisterFunction(GenerateSetupPipelineStateFunction(&pipeline_context));
  code_container->RegisterFunction(GenerateTearDownPipelineStateFunction(&pipeline_context));

  // Generate pipeline setup logic.
  code_container->RegisterFunction(GenerateInitPipelineFunction(&pipeline_context));

  // Generate main pipeline logic.
  code_container->RegisterFunction(GeneratePipelineWorkFunction(&pipeline_context));

  // Generate the pipeline launch code.
  code_container->RegisterFunction(GenerateRunPipelineFunction(&pipeline_context));

  // Generate pipeline tear-down logic.
  code_container->RegisterFunction(GenerateTearDownPipelineFunction(&pipeline_context));
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
