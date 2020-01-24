#pragma once

#include <string>
#include <type_traits>

#include "ast/identifier.h"
#include "common/macros.h"
#include "sql/codegen/ast_fwd.h"
#include "sql/codegen/expression/column_value_provider.h"
#include "sql/codegen/query_state.h"
#include "util/region_containers.h"

namespace tpl::sql::planner {
class AbstractPlanNode;
}  // namespace tpl::sql::planner

namespace tpl::sql::codegen {

class CodeGen;
class CompilationContext;
class WorkContext;
class Pipeline;
class PipelineContext;
class TopLevelDeclarations;

/**
 * The base class of all operator translators.
 *
 * All operators are associated to a CompilationContext. The compilation context serves as a central
 * context object containing the query state, all operator translators, all expression translators
 * and all pipeline objects. Upon construction, it is the translators duty to prepare each of its
 * children in the provided compilation context.
 *
 * Operators must also register themselves in the pipelines. If an operator belongs to multiple
 * pipelines, it may construct new pipelines and register its children as appropriate between the
 * one or more pipelines it controls.
 *
 * State:
 * ------
 * Operators usually require some runtime state in order to implement their logic. A hash-join
 * requires a hash table, a sort requires a sorting instance, a block-nested loop join requires a
 * temporary buffer, etc. State exists at two levels: query and pipeline.
 *
 * Query-level state exists for the duration of the entire query. Operators declare and register
 * their state entries with the QueryState object in CompilationContext, initialize their state in
 * InitializeQueryState(), and destroy their state in TearDownQueryState(). All query-state based
 * operations are called exactly once per operator in the plan. Translators may not assume an
 * initialization order.
 *
 * Pipeline-level state exists only within a pipeline. Operators declare and register pipeline-local
 * state in DeclarePipelineState(), initialize it in InitializePipelineState(), and destroy in in
 * TearDownPipelineState(). Since some operators participate in multiple pipelines, a context object
 * is provided to discern <b>which pipeline</b> is being considered.
 *
 * Data-flow:
 * ----------
 * Operators participate in one or more pipelines. A pipeline represents a uni-directional data flow
 * of tuples or tuple batches. Another of way of thinking of pipelines are as a post-order walk of
 * a subtree in the query plan composing of operators that do not materialize tuple (batch) data
 * into cache or memory.
 *
 * Pipelines have a source and a sink. Tuples (batches) flow from the source to the sink,
 * potentially revisiting operators. Tuple (batch) data is contained in a ConsumerContext and
 * passed along the pipeline through DoPipelineWork(). DoPipelineWork() is where operators generate
 * their logic.
 *
 * Operators can implement some pre-work and post-work logic that may be required by implementing
 * BeginPipelineWork() and FinishPipelineWork() which are called before and after the primary/main
 * pipeline work is generated, respectively.
 *
 * Helper functions:
 * -----------------
 * Operators may require the used oa helper functions of auxiliary intermediate structures in order
 * to simplify their logic. Helper functions and structures should be defined in both
 * DefineHelperStructs() and DefineHelperFunctions(), respectively. All helper structures and
 * functions are visible across the whole query and must be declared in the provided
 * TopLevelDeclarations container. These operations are guaranteed to be called exactly once per
 * operator in the query plan.
 */
class OperatorTranslator : public ColumnValueProvider {
 public:
  /**
   * Create a translator.
   * @param plan The plan node this translator will generate code for.
   * @param compilation_context The context this compilation is occurring in.
   * @param pipeline The pipeline this translator is a part of.
   */
  OperatorTranslator(const planner::AbstractPlanNode &plan, CompilationContext *compilation_context,
                     Pipeline *pipeline);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(OperatorTranslator);

  /**
   * Destructor.
   */
  virtual ~OperatorTranslator() = default;

  /**
   * Define any helper structures required for processing. Ensure they're declared in the provided
   * declaration container.
   * @param top_level_decls Query-level declarations.
   */
  virtual void DefineHelperStructs(TopLevelDeclarations *top_level_decls) {}

  /**
   * Define any helper functions required for processing. Ensure they're declared in the provided
   * declaration container.
   * @param top_level_decls Query-level declarations.
   */
  virtual void DefineHelperFunctions(TopLevelDeclarations *top_level_decls) {}

  /**
   * Initialize all query state.
   */
  virtual void InitializeQueryState() const = 0;

  /**
   * Tear down all query state.
   */
  virtual void TearDownQueryState() const = 0;

  /**
   * Declare any pipeline-local state needed by this operator. The given context contains the
   * pipeline that's being considered.
   * @param pipeline_context The pipeline context.
   */
  virtual void DeclarePipelineState(PipelineContext *pipeline_context) = 0;

  /**
   * Initialize any declared pipeline-local state.
   * @param pipeline_context The pipeline context.
   */
  virtual void InitializePipelineState(const PipelineContext &pipeline_context) const = 0;

  /**
   * Perform any work required before beginning main pipeline work. This is executed by one thread.
   * @param pipeline_context The pipeline context.
   */
  virtual void BeginPipelineWork(const PipelineContext &pipeline_context) const = 0;

  /**
   * Perform the primary logic of a pipeline. This is where the operator's logic should be
   * implemented. The provided context object contains information necessary to help operators
   * decide what work to perform. Specifically,
   *  1. The pipeline the work is for:
   *     Some operators may be connected to multiple pipelines. For example, hash joins rely on two
   *     pipelines. The provided work context indicates specifically which pipeline the work is for
   *     so that operators generate the correct code.
   *  2. Access to thread-local state:
   *     Operators may rely on pipeline-local state. The provided context allows operators to access
   *     such state using slot references they've received when declaring pipeline state in
   *     DeclarePipelineState().
   *  3. An expression evaluation mechanism:
   *     The provided work context can be used to evaluate an expression. Expression evaluation is
   *     context sensitive. The context also provides a mechanism to cache expression results.
   * @param work_context The context of the work.
   */
  virtual void PerformPipelineWork(WorkContext *work_context) const = 0;

  /**
   * Perform any work required <b>after</b> the main pipeline work. This is executed by one thread.
   * @param pipeline_context The pipeline context.
   */
  virtual void FinishPipelineWork(const PipelineContext &pipeline_context) const = 0;

  /**
   * Tear down and destroy any pipeline-local state.
   * @param pipeline_context The pipeline context.
   */
  virtual void TearDownPipelineState(const PipelineContext &pipeline_context) const = 0;

  /**
   * @return The list of extra fields added to the main work function. This is only called on
   *         parallel pipelines and only for the source/root of the pipeline that's responsible for
   *         launching the worker function. By default, the first two argument of a parallel work
   *         function are the query state and thread-local pipeline state.
   */
  virtual util::RegionVector<ast::FieldDecl *> GetWorkerParams() const = 0;

  /**
   * This is called on the source/root of parallel pipelines to launch the provided worker function
   * in parallel across a set of threads.
   * @param work_func_name The name of the work function that implements the pipeline logic.
   */
  virtual void LaunchWork(ast::Identifier work_func_name) const = 0;

  /**
   * @return The value (vector) of the attribute at the given index in this operator's output.
   */
  ast::Expr *GetOutput(WorkContext *work_context, uint32_t attr_idx) const;

  /**
   * @return The plan the translator is generating.
   */
  const planner::AbstractPlanNode &GetPlan() const { return plan_; }

  /**
   * @return The compilation context the translator is owned by.
   */
  CompilationContext *GetCompilationContext() const { return compilation_context_; }

 protected:
  // Get the code generator instance.
  CodeGen *GetCodeGen() const;

  // Get a pointer to the query state.
  ast::Expr *GetQueryStatePtr() const;

  // Return a pointer to an entry in the query state at the given slot.
  ast::Expr *GetQueryStateEntryPtr(QueryState::Slot slot) const;

  // Get the execution context pointer in the current function.
  ast::Expr *GetExecutionContext() const;

  // Get the thread state container pointer from the execution context stored in the query state.
  ast::Expr *GetThreadStateContainer() const;

  // Get the memory pool pointer from the execution context stored in the query state.
  ast::Expr *GetMemoryPool() const;

  // The pipeline this translator is a part of.
  Pipeline *GetPipeline() const { return pipeline_; }

  // The plan node for this translator as its concrete type.
  template <typename T>
  const T &GetPlanAs() const {
    static_assert(std::is_base_of_v<planner::AbstractPlanNode, T>, "Template is not a plan node");
    return static_cast<const T &>(plan_);
  }

  // Used by operators when they need to generate a struct containing a child's
  // output. Also used by the output layer to materialize the output.
  // The child index refers to which specific child to inspect.
  // The prefix is added to each field/attribute of the child.
  // The fields vector collects the resulting field declarations.
  void GetAllChildOutputFields(uint32_t child_index, const std::string &field_name_prefix,
                               util::RegionVector<ast::FieldDecl *> *fields) const;

 private:
  // The plan node.
  const planner::AbstractPlanNode &plan_;
  // The compilation context.
  CompilationContext *compilation_context_;
  // The pipeline the operator belongs to.
  Pipeline *pipeline_;
};

}  // namespace tpl::sql::codegen
