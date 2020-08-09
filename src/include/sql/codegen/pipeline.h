#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ast/identifier.h"
#include "common/common.h"
#include "sql/codegen/ast_fwd.h"
#include "sql/codegen/codegen_defs.h"
#include "sql/codegen/state_descriptor.h"
#include "util/region_containers.h"

namespace tpl::sql::codegen {

class CompilationUnit;
class CodeGen;
class CompilationContext;
class ExpressionTranslator;
class OperatorTranslator;
class PipelineDriver;
class PipelineGraph;

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
  enum class Parallelism : uint8_t { Serial, Parallel };

  /**
   * Enum class representing whether the pipeline is vectorized.
   */
  enum class Vectorization : uint8_t { Disabled, Enabled };

  /**
   * Enum class representing whether the pipeline is independent or composite.
   */
  enum class Type : uint8_t { Regular, Nested };

  /**
   * Create an empty pipeline in the given compilation context.
   * @param compilation_context The compilation context the pipeline is in.
   * @param pipeline_graph The pipeline graph to register in.
   */
  explicit Pipeline(CompilationContext *compilation_context, PipelineGraph *pipeline_graph);

  /**
   * Create a pipeline with the given operator as the root.
   * @param op The root operator of the pipeline.
   * @param pipeline_graph The pipeline graph to register in.
   * @param parallelism The operator's requested parallelism.
   */
  explicit Pipeline(OperatorTranslator *op, PipelineGraph *pipeline_graph, Parallelism parallelism);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(Pipeline);

  /**
   * Register an operator in this pipeline with a customized parallelism configuration.
   * @param op The operator to add to the pipeline.
   * @param parallelism The operator's requested parallelism.
   */
  void RegisterStep(OperatorTranslator *op);

  /**
   * Register the source/driver for the pipeline.
   * @param driver The single driver for the pipeline.
   * @param parallelism The driver's requested parallelism.
   */
  void RegisterSource(PipelineDriver *driver, Parallelism parallelism);

  /**
   * Update the current parallelism level for this pipeline to the value provided.
   * @param parallelism The desired parallelism level.
   */
  void UpdateParallelism(Parallelism parallelism);

  /**
   * Enable or disable the pipeline's parallelism check during register RegisterStep.
   * @param check Whether the to check for parallelism or not.
   */
  void SetParallelCheck(bool check);

  /**
   * Register an expression in this pipeline. This expression may or may not create/destroy state.
   * @param expression The expression to register.
   */
  void RegisterExpression(ExpressionTranslator *expression);

  /**
   * Declare an entry in this pipeline's state.
   * @param name The name of the element.
   * @param type_repr The TPL type representation of the element.
   * @return The slot where the inserted state exists.
   */
  StateDescriptor::Entry DeclarePipelineStateEntry(const std::string &name, ast::Expr *type_repr);

  /**
   * Add the provided pipeline as a dependency for this pipeline. In other words, this pipeline
   * cannot begin until the provided pipeline completes. Dependencies define an execution order.
   * @param dependency Another pipeline this pipeline is dependent on.
   */
  void AddDependency(const Pipeline &dependency) const;

  /**
   * Mark this pipeline as being nested within the provided outer/parent pipeline.
   * @param parent The pipeline that is nesting this pipeline.
   */
  void MarkNestedPipeline(Pipeline *parent);

  /**
   * Perform initialization logic before code generation.
   */
  void Prepare();

  /**
   * Generate all functions to execute this pipeline in the provided container.
   * @param container The code container.
   */
  std::vector<ast::FunctionDecl *> GeneratePipelineLogic() const;

  /**
   * @return The unique ID of this pipeline.
   */
  PipelineId GetId() const { return id_; }

  /**
   * @return The pipeline graph.
   */
  PipelineGraph *GetPipelineGraph() const { return pipeline_graph_; }

  /**
   * @return True if the pipeline is parallel; false otherwise.
   */
  bool IsParallel() const { return parallelism_ == Parallelism ::Parallel; }

  /**
   * @return True if this pipeline is fully vectorized; false otherwise.
   */
  bool IsVectorized() const { return false; }

  /**
   * @return Is the given operator the last in this pipeline?
   */
  bool IsLastOperator(const OperatorTranslator &op) const;

  /**
   * Typedef used to specify an iterator over the steps in a pipeline.
   */
  using Iterator = std::vector<OperatorTranslator *>::const_reverse_iterator;

  /**
   * @return An iterator over the operators in the pipeline.
   */
  Iterator Begin() const { return operators_.rbegin(); }

  /**
   * @return An iterator positioned at the end of the operators steps in the pipeline.
   */
  Iterator End() const { return operators_.rend(); }

  /**
   * @return True if the given operator is the driver for this pipeline; false otherwise.
   */
  bool IsDriver(const PipelineDriver *driver) const { return driver == driver_; }

  /**
   * @return The list of pipelines that nest this pipeline.
   */
  const std::vector<const Pipeline *> &GetParentPipelines() const { return parent_pipelines_; }

  /**
   * @return The list of pipelines nested within this pipeline.
   */
  const std::vector<const Pipeline *> &GetNestedPipelines() const { return child_pipelines_; }

  /**
   * @return Arguments common to all pipeline functions.
   */
  util::RegionVector<ast::FieldDecl *> PipelineParams() const;

  /**
   * @return A unique name for a function local to this pipeline.
   */
  std::string CreatePipelineFunctionName(const std::string &func_name) const;

  /**
   * @return The name of this pipeline. This a pretty-printed version of the operators that
   *         constitute the pipeline.
   */
  std::string BuildPipelineName() const;

 private:
  // Generate the pipeline state initialization logic.
  ast::FunctionDecl *GenerateSetupPipelineStateFunction() const;

  // Generate the pipeline state cleanup logic.
  ast::FunctionDecl *GenerateTearDownPipelineStateFunction() const;

  // Generate pipeline initialization logic.
  ast::FunctionDecl *GenerateInitPipelineFunction() const;

  // Generate the main pipeline work function.
  ast::FunctionDecl *GeneratePipelineWorkFunction() const;

  // Generate the main pipeline logic.
  ast::FunctionDecl *GenerateRunPipelineFunction() const;

  // Generate pipeline tear-down logic.
  ast::FunctionDecl *GenerateTearDownPipelineFunction() const;

 private:
  // A unique pipeline ID.
  PipelineId id_;
  // The compilation context this pipeline is part of.
  CompilationContext *compilation_ctx_;
  // The pipeline graph.
  PipelineGraph *pipeline_graph_;
  // The code generation instance.
  CodeGen *codegen_;
  // Operators making up the pipeline.
  std::vector<OperatorTranslator *> operators_;
  // The driver of the pipeline.
  PipelineDriver *driver_;
  // Expressions participating in the pipeline.
  std::vector<ExpressionTranslator *> expressions_;
  // Configured parallelism.
  Parallelism parallelism_;
  // Whether to check for parallelism in new pipeline elements.
  bool check_parallelism_;
  // The list of pipelines that nest this pipeline.
  std::vector<const Pipeline *> parent_pipelines_;
  // The list of pipelines nested inside this pipeline.
  std::vector<const Pipeline *> child_pipelines_;
  // This pipeline's type.
  Type type_;
  // Cache of common identifiers.
  ast::Identifier state_var_;
  // The pipeline state.
  StateDescriptor state_;
};

}  // namespace tpl::sql::codegen
