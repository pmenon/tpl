#pragma once

#include <string>
#include <type_traits>

#include "ast/identifier.h"
#include "common/macros.h"
#include "sql/codegen/ast_fwd.h"
#include "sql/codegen/compilation_unit.h"
#include "sql/codegen/edsl/struct.h"
#include "sql/codegen/edsl/value.h"
#include "sql/codegen/edsl/value_vt.h"
#include "sql/codegen/expression/column_value_provider.h"
#include "sql/codegen/state_descriptor.h"
#include "util/region_containers.h"

namespace tpl::sql::planner {
class AbstractPlanNode;
class OutputSchema;
}  // namespace tpl::sql::planner

namespace tpl::sql::codegen {

class CodeGen;
class CompilationContext;
class FunctionBuilder;
class Pipeline;
class PipelineContext;
class ConsumerContext;

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
 * All pipelines have a source operator and a sink operator. Tuples (batches) flow from the source
 * to the sink along a pipeline in a WorkContext. When an operator receives a work context, it is
 * a request to generate the logic of the operator (i.e., perform some work). When this work is
 * complete, it should be sent to the following operator in the pipeline.
 *
 * Operators may generate pre-work and post-work pipeline logic by overriding BeginPipelineWork()
 * and FinishPipelineWork() which are called before and after the primary/main pipeline work is
 * generated, respectively.
 *
 * Helper functions:
 * -----------------
 * Operators may require the use of helper functions or auxiliary structures in order to simplify
 * their logic. Helper functions and structures should be defined in both DefineHelperStructs() and
 * DefineHelperFunctions(), respectively. All helper structures and functions are visible across the
 * whole query and must be declared in the provided input container.
 */
class OperatorTranslator : public ColumnValueProvider {
 public:
  /**
   * Create a translator.1
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
   * For any pipelines this translator has, declare all dependencies.
   */
  virtual void DeclarePipelineDependencies() const {}

  /**
   * Define any helper structures or functions required for processing. These are available for the
   * whole query.
   */
  virtual void DefineStructsAndFunctions() {}

  /**
   * Initialize all query state.
   * @param The builder for the query state initialization function.
   */
  virtual void InitializeQueryState(FunctionBuilder *function) const {}

  /**
   * Tear down all query state.
   */
  virtual void TearDownQueryState(FunctionBuilder *function) const {}

  /**
   * Declare any pipeline local state.
   * @param pipeline_ctx The pipeline context.
   */
  virtual void DeclarePipelineState(PipelineContext *pipeline_ctx) {}

  /**
   * Define any pipeline-local helper functions.
   * @param pipeline)ctx The pipeline we're generating functions in.
   */
  virtual void DefinePipelineFunctions(const PipelineContext &pipeline_ctx) {}

  /**
   * Initialize any declared pipeline-local state.
   * @param pipeline The pipeline whose state is being initialized.
   * @param function The function being built.
   */
  virtual void InitializePipelineState(const PipelineContext &pipeline_ctx,
                                       FunctionBuilder *function) const {}

  /**
   * Tear down and destroy any pipeline-local state.
   * @param pipeline The pipeline whose state is being destroyed.
   * @param function The function being built.
   */
  virtual void TearDownPipelineState(const PipelineContext &pipeline_ctx,
                                     FunctionBuilder *function) const {}

  /**
   * Perform any work required before beginning main pipeline work. This is executed by one thread.
   * @param pipeline The pipeline whose pre-work logic is being generated.
   * @param function The function being built.
   */
  virtual void BeginPipelineWork(const PipelineContext &pipeline_ctx,
                                 FunctionBuilder *function) const {}

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
   * @param context The context of the work.
   * @param function The function being built.
   */
  virtual void Consume(ConsumerContext *context, FunctionBuilder *function) const = 0;

  /**
   * Perform any work required <b>after</b> the main pipeline work. This is executed by one thread.
   * @param pipeline The pipeline whose post-work logic is being generated.
   * @param function The function being built.
   */
  virtual void FinishPipelineWork(const PipelineContext &pipeline_ctx,
                                  FunctionBuilder *function) const {}

  /**
   * @return The value (vector) of the attribute at the given index in this operator's output.
   */
  edsl::ValueVT GetOutput(ConsumerContext *context, uint32_t attr_idx) const;

  /**
   * @return The value (vector) of the attribute at the given index (@em attr_idx) produced by the
   *         child at the given index (@em child_idx).
   */
  edsl::ValueVT GetChildOutput(ConsumerContext *context, uint32_t child_idx,
                               uint32_t attr_idx) const override;

  /**
   * @return The plan the translator is generating.
   */
  const planner::AbstractPlanNode &GetPlan() const { return plan_; }

  /**
   * @return The compilation context the translator is owned by.
   */
  CompilationContext *GetCompilationContext() const { return compilation_ctx_; }

 protected:
  // Get a pointer to the query state.
  edsl::ValueVT GetQueryStatePtr() const;

  // Get an untyped reference to element in the query state at the given index.
  edsl::ReferenceVT GetQueryStateEntryGeneric(StateDescriptor::Slot slot) const;

  // Get a typed reference to element in the query state at the given index.
  template <typename T>
  edsl::Value<T> GetQueryStateEntry(StateDescriptor::Slot slot) const {
    return edsl::Value<T>(GetQueryStateEntryGeneric(slot));
  }

  // Get an untyped pointer to element in the query state at the given index.
  edsl::ValueVT GetQueryStateEntryPtrGeneric(StateDescriptor::Slot slot) const;

  // Get a typed pointer to element in the query state at the given index.
  template <typename T>
  edsl::Value<T *> GetQueryStateEntryPtr(StateDescriptor::Slot slot) const {
    return edsl::Value<T *>(GetQueryStateEntryPtrGeneric(slot));
  }

  // Get the execution context pointer in the current function.
  edsl::Value<ast::x::ExecutionContext *> GetExecutionContext() const;

  // Get the thread state container pointer from the execution context stored in the query state.
  edsl::Value<ast::x::ThreadStateContainer *> GetThreadStateContainer() const;

  // Get the memory pool pointer from the execution context stored in the query state.
  edsl::Value<ast::x::MemoryPool *> GetMemoryPool() const;

  // The pipeline this translator is a part of.
  Pipeline *GetPipeline() const { return pipeline_; }

  // The plan node for this translator as its concrete type.
  template <typename T>
  const T &GetPlanAs() const {
    static_assert(std::is_base_of_v<planner::AbstractPlanNode, T>, "Template is not a plan node");
    return static_cast<const T &>(plan_);
  }

  // Get the output schema of the given child plan node.
  const planner::OutputSchema *GetChildOutputSchema(uint32_t child_idx) const;

  // Used by operators when they need to generate a struct containing a child's
  // output. Also used by the output layer to materialize the output.
  // The child index refers to which specific child to inspect.
  // The prefix is added to each field/attribute of the child.
  // The fields vector collects the resulting field declarations.
  void GetAllChildOutputFields(uint32_t child_idx, std::string_view prefix, edsl::Struct *s) const;

 private:
  // The plan node.
  const planner::AbstractPlanNode &plan_;
  // The compilation context.
  CompilationContext *compilation_ctx_;
  // The pipeline the operator belongs to.
  Pipeline *pipeline_;

 protected:
  CodeGen *codegen_;
};

}  // namespace tpl::sql::codegen
