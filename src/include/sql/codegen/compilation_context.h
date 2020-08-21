#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "sql/codegen/codegen.h"
#include "sql/codegen/codegen_defs.h"
#include "sql/codegen/executable_query.h"
#include "sql/codegen/execution_plan.h"
#include "sql/codegen/state_descriptor.h"

namespace tpl::sql::planner {
class AbstractExpression;
class AbstractPlanNode;
}  // namespace tpl::sql::planner

namespace tpl::sql::codegen {

class CompilationUnit;
class ExpressionTranslator;
class OperatorTranslator;
class Pipeline;
class PipelineGraph;

/**
 * Context of all code-generation related structures. A temporary container that lives only during
 * code generation storing:
 * - Translators for relational operators
 * - Translators for expressions
 * - Pipelines making up the query plan.
 */
class CompilationContext {
 public:
  /**
   * Compile the given plan into an executable query.
   * @param plan The plan to compile.
   * @param mode The compilation mode.
   */
  static std::unique_ptr<ExecutableQuery> Compile(const planner::AbstractPlanNode &plan,
                                                  CompilationMode mode = CompilationMode::OneShot);

  /**
   * Prepare compilation for the given relational plan node participating in the provided pipeline.
   * @param plan The plan node.
   * @param pipeline The pipeline the node belongs to.
   */
  void Prepare(const planner::AbstractPlanNode &plan, Pipeline *pipeline);

  /**
   * Prepare compilation for the given expression.
   * @param expression The expression.
   */
  void Prepare(const planner::AbstractExpression &expression);

  /**
   * @return The code generator instance.
   */
  CodeGen *GetCodeGen() { return &codegen_; }

  /**
   * @return The query state.
   */
  StateDescriptor *GetQueryState() { return &query_state_; }

  /**
   * @return The translator for the given relational plan node; null if the provided plan node does
   *         not have a translator registered in this context.
   */
  OperatorTranslator *LookupTranslator(const planner::AbstractPlanNode &node) const;

  /**
   * @return The translator for the given expression; null if the provided expression does not have
   *         a translator registered in this context.
   */
  ExpressionTranslator *LookupTranslator(const planner::AbstractExpression &expr) const;

  /**
   * @return A common prefix for all functions generated in this module.
   */
  std::string GetFunctionPrefix() const;

  /**
   * @return The list of parameters common to all query functions. For now, just the query state.
   */
  util::RegionVector<ast::FieldDecl *> QueryParams() const;

  /**
   * @return The slot in the query state where the execution context can be found.
   */
  ast::Expr *GetExecutionContextPtrFromQueryState();

  /**
   * @return The compilation mode.
   */
  CompilationMode GetCompilationMode() const { return mode_; }

 private:
  // Private to force use of static Compile() function.
  explicit CompilationContext(ExecutableQuery *query, CompilationMode mode);

  // Given a plan node, compile it into a compiled query object.
  void GeneratePlan(const planner::AbstractPlanNode &plan);

  // Generate the query initialization function.
  ast::FunctionDecl *GenerateInitFunction();

  // Generate the query tear-down function.
  ast::FunctionDecl *GenerateTearDownFunction();

  // Prepare compilation for the output.
  void PrepareOut(const planner::AbstractPlanNode &plan, Pipeline *pipeline);

  // Declare and establish the pipeline dependencies.
  void EstablishPipelineDependencies();

  // Declare all query-level structure and functions.
  void DeclareStructsAndFunctions();

  // Create a new container.
  CompilationUnit *MakeContainer();

  using ContainerIndex = std::size_t;

  // Generate all pipeline code. The output vector 'steps' is populated with
  // list of execution steps to be evaluated in order. The output parameter
  // 'mapping' contains a mapping of the container that holds each pipeline's
  // generated code.
  void GeneratePipelineCode(const PipelineGraph &graph, const Pipeline &target,
                            std::vector<ExecutionStep> *steps,
                            std::unordered_map<PipelineId, ContainerIndex> *mapping);

  // Generate pipeline code in one-shot.
  void GeneratePipelineCode_OneShot(const PipelineGraph &graph, const Pipeline &target,
                                    std::vector<ExecutionStep> *steps,
                                    std::unordered_map<PipelineId, ContainerIndex> *mapping);

  // Generate pipeline code incremental. This approach interleaves
  // code-generation and execution.
  void GeneratePipelineCode_Incremental(const PipelineGraph &graph, const Pipeline &target,
                                        std::vector<ExecutionStep> *steps,
                                        std::unordered_map<PipelineId, ContainerIndex> *mapping);

 private:
  // Unique ID used as a prefix for all generated functions to ensure uniqueness.
  uint64_t unique_id_;
  // The compiled query object we'll update.
  ExecutableQuery *query_;
  // The compilation mode.
  CompilationMode mode_;
  // All allocated containers.
  std::vector<std::unique_ptr<CompilationUnit>> containers_;
  // The code generator instance.
  CodeGen codegen_;
  // Cached identifiers.
  ast::Identifier query_state_var_;
  ast::Identifier query_state_type_;
  // The query state and the slot in the state where the execution context is.
  StateDescriptor query_state_;
  StateDescriptor::Slot exec_ctx_;
  // The operator and expression translators.
  std::unordered_map<const planner::AbstractPlanNode *, std::unique_ptr<OperatorTranslator>>
      operators_;
  std::unordered_map<const planner::AbstractExpression *, std::unique_ptr<ExpressionTranslator>>
      expressions_;
};

}  // namespace tpl::sql::codegen
