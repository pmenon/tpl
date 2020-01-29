#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ast/identifier.h"
#include "common/common.h"
#include "sql/codegen/ast_fwd.h"
#include "util/region_containers.h"

namespace tpl::sql::codegen {

class CodeGen;
class CodeContainer;
class CompilationContext;
class ExpressionTranslator;
class OperatorTranslator;

class Pipeline;

/**
 * Ties together a pipeline instance and the struct representing the pipeline's state.
 */
class PipelineContext {
 public:
  using Slot = uint32_t;

  /**
   * Create a new context in the given pipeline.
   * @param pipeline The pipeline the context represents.
   * @param state_name The name of the pipeline state.
   */
  PipelineContext(const Pipeline &pipeline, ast::Identifier state_name);

  /**
   * Declare an entry in the pipeline's state.
   * @param codegen The code generator instance.
   * @param name The name of the state.
   * @param type The type of the state.
   * @return The slot in the state that can be used to retrieve the state.
   */
  PipelineContext::Slot DeclareStateEntry(CodeGen *codegen, const std::string &name,
                                          ast::Expr *type);

  /**
   * Seal and construct the pipeline-local state. After this, the state is unmodifiable.
   * @param codegen The code generator instance.
   * @return The local state type.
   */
  ast::StructDecl *ConstructPipelineStateType(CodeGen *codegen);

  /**
   * @return The value of the state element at the given slot in this pipeline's state.
   */
  ast::Expr *GetThreadStateEntry(CodeGen *codegen, Slot slot) const;

  /**
   * @return A pointer to the state element at the given slot in this pipeline's state.
   */
  ast::Expr *GetThreadStateEntryPtr(CodeGen *codegen, Slot slot) const;

  /**
   * @return The byte offset of the stat element at the given slot in the pipeline state.
   */
  ast::Expr *GetThreadStateEntryOffset(CodeGen *codegen, Slot slot) const;

  /**
   * @return True if the pipeline in this context is parallel; false otherwise.
   */
  bool IsParallel() const;

  /**
   * @return The pipeline this context contains.
   */
  const Pipeline &GetPipeline() const { return pipeline_; }

  /**
   * RAII scope class to temporarily set the pipeline state pointer in this context.
   */
  class StateScope {
   public:
    /**
     * Construct a scope object that sets the current pipeline state pointer to the provided value.
     * The previous value is cached locally.
     * @param pipeline_context The pipeline context.
     * @param thread_state_ptr The state pointer to set.
     */
    StateScope(PipelineContext *pipeline_context, ast::Expr *thread_state_ptr);

    /**
     * Destructor. The previous state pointer is restored in the pipeline context.
     */
    ~StateScope();

   private:
    // The pipeline context.
    PipelineContext *ctx_;
    // The previous state pointer.
    ast::Expr *prev_thread_state_ptr_;
  };

 private:
  // The pipeline.
  const Pipeline &pipeline_;
  // The name of the pipeline state.
  ast::Identifier state_name_;
  // The fields of the pipeline state.
  std::vector<std::pair<ast::Identifier, ast::Expr *>> slots_;
  // The current thread state pointer.
  ast::Expr *thread_state_ptr_;
};

/**
 * A pipeline represents an ordered sequence of relational operators that operate on tuple data
 * without explicit copying or materialization. Tuples are read at the start of the pipeline, pass
 * through each operator, and are materialized in some form only at the end of the pipeline.
 *
 * Pipelines are flexible allowing the flow of batches of tuples as well as individual tuples, thus
 * supporting vector-at-a-time (VaaT) and tuple-at-a-time (TaaT) execution. Translators composing
 * the pipeline are aware of this hybrid approach and can generate code in both paradigms.
 *
 * Pipelines form the unit of parallelism. Each pipeline can either be launched serially or in
 * parallel.
 */
class Pipeline {
 public:
  /**
   * Enum class representing a degree of parallelism. The Serial and Parallel values are clear. The
   * Flexible option should be used when both serial and parallel operation is supported, but no
   * preference is taken.
   */
  enum class Parallelism : uint8_t { Serial = 0, Flexible = 1, Parallel = 2 };

  /**
   * Enum class representing whether the pipeline is vectorized.
   */
  enum class Vectorization : uint8_t { Disabled = 0, Enabled = 1 };

  /**
   * Create an empty pipeline in the given compilation context.
   * @param ctx The compilation context the pipeline is in.
   */
  explicit Pipeline(CompilationContext *ctx);

  /**
   * Create a pipeline with the given operator as the root.
   * @param op The root operator of the pipeline.
   * @param parallelism The operator's requested parallelism.
   */
  Pipeline(OperatorTranslator *op, Parallelism parallelism);

  /**
   * Register an operator in this pipeline with a customized parallelism configuration.
   * @param op The operator to add to the pipeline.
   * @param parallelism The operator's requested parallelism.
   */
  void RegisterStep(OperatorTranslator *op, Parallelism parallelism);

  /**
   * Register an expression in this pipeline. This expression may or may not create/destroy state.
   * @param expression The expression to register.
   */
  void RegisterExpression(ExpressionTranslator *expression);

  /**
   * Register the given operator as the source of this pipeline. A source operator provides the
   * pipeline with a flow of tuple batches (or tuples in TaaT mode).
   * @param op The operator.
   * @param parallelism The source operator's requested parallelism.
   */
  void RegisterSource(OperatorTranslator *op, Parallelism parallelism);

  /**
   * Register the provided pipeline as a dependency for this pipeline. In other words, this pipeline
   * cannot begin until the provided pipeline completes.
   * @param dependency Another pipeline this pipeline is dependent on.
   */
  void LinkSourcePipeline(Pipeline *dependency);

  /**
   * Store in the provided output vector the set of all dependencies for this pipeline. In other
   * words, store in the output vector all pipelines that must execute (in order) before this
   * pipeline can begin.
   * @param[out] deps The sorted list of pipelines to execute before this pipeline can begin.
   */
  void CollectDependencies(std::vector<Pipeline *> *deps);

  /**
   * Generate all functions to execute this pipeline in the provided container.
   * @param codegen The code generator instance.
   */
  void GeneratePipeline(CodeContainer *code_container) const;

  /**
   * @return True if the pipeline is parallel; false otherwise.
   */
  bool IsParallel() const { return parallelism_ == Parallelism ::Parallel; }

  /**
   * @return True if this pipeline is fully vectorized; false otherwise.
   */
  bool IsVectorized() const { return false; }

  /**
   * Typedef used to specify an iterator over the steps in a pipeline.
   */
  using StepIterator = std::vector<OperatorTranslator *>::const_reverse_iterator;

  /**
   * @return An iterator over the operators in the pipeline.
   */
  StepIterator Begin() const { return steps_.rbegin(); }

  /**
   * @return An iterator positioned at the end of the operators steps in the pipeline.
   */
  StepIterator End() const { return steps_.rend(); }

  /**
   * Pretty print this pipeline's information.
   * @return A string containing pretty-printed information about this pipeline.
   */
  std::string PrettyPrint() const;

  /**
   * @return A unique name for a function local to this pipeline.
   */
  std::string ConstructPipelineFunctionName(const std::string &func_name) const;

  /**
   * @return Arguments common to all pipeline functions.
   */
  util::RegionVector<ast::FieldDecl *> PipelineParams() const;

 private:
  // Return the thread-local state initialization and tear-down function names.
  // This is needed when we invoke @tlsReset() from the pipeline initialization
  // function to setup the thread-local state.
  ast::Identifier GetSetupPipelineStateFunctionName() const;
  ast::Identifier GetTearDownPipelineStateFunctionName() const;
  ast::Identifier GetWorkFunctionName() const;

  // Generate the pipeline state initialization logic.
  ast::FunctionDecl *GenerateSetupPipelineStateFunction(PipelineContext *pipeline_context) const;

  // Generate the pipeline state cleanup logic.
  ast::FunctionDecl *GenerateTearDownPipelineStateFunction(PipelineContext *pipeline_context) const;

  // Generate pipeline initialization logic.
  ast::FunctionDecl *GenerateInitPipelineFunction(PipelineContext *pipeline_context) const;

  // Generate the main pipeline work function.
  ast::FunctionDecl *GeneratePipelineWorkFunction(PipelineContext *pipeline_context) const;

  // Generate the main pipeline logic.
  ast::FunctionDecl *GenerateRunPipelineFunction(PipelineContext *pipeline_context) const;

  // Generate pipeline tear-down logic.
  ast::FunctionDecl *GenerateTearDownPipelineFunction(PipelineContext *pipeline_context) const;

 private:
  // A unique pipeline ID.
  uint32_t id_;
  // The compilation context this pipeline is part of.
  CompilationContext *compilation_context_;
  // Operators making up the pipeline.
  std::vector<OperatorTranslator *> steps_;
  // Expressions participating in the pipeline.
  std::vector<ExpressionTranslator *> expressions_;
  // Configured parallelism.
  Parallelism parallelism_;
  // All pipelines this one depends on completion of.
  std::vector<Pipeline *> dependencies_;

  // Cache of common identifiers.
  ast::Identifier pipeline_state_var_;
  ast::Identifier pipeline_state_type_name_;
};

}  // namespace tpl::sql::codegen
