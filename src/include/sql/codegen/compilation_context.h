#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "ast/identifier.h"
#include "sql/codegen/codegen.h"
#include "sql/codegen/codegen_defs.h"
#include "sql/codegen/execution_state.h"

namespace tpl::sql::planner {
class AbstractExpression;
class AbstractPlanNode;
}  // namespace tpl::sql::planner

namespace tpl::sql::codegen {

class CompilationUnit;
class ExecutableQuery;
class ExecutionStep;
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
  static std::unique_ptr<ExecutableQuery> Compile(const planner::AbstractPlanNode &plan);

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
  ExecutionState *GetQueryState() { return &query_state_; }

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
  std::vector<std::pair<ast::Identifier, ast::Type *>> QueryParams() const;

  /**
   * @return The execution context pointer from the query state.
   */
  edsl::Value<ast::x::ExecutionContext *> GetExecutionContextPtrFromQueryState();

 private:
  // Private to force use of static Compile() function.
  explicit CompilationContext(ExecutableQuery *query);

  // Given a plan node, compile it into a compiled query object.
  void GeneratePlan(const planner::AbstractPlanNode &plan);

  // Generate the query initialization function.
  ast::FunctionDeclaration *GenerateInitFunction();

  // Generate the query tear-down function.
  ast::FunctionDeclaration *GenerateTearDownFunction();

  // Declare common state.
  void DeclareCommonQueryState();

  // Prepare compilation for the output.
  void PrepareOut(const planner::AbstractPlanNode &plan, Pipeline *pipeline);

  // Declare and establish the pipeline dependencies.
  void EstablishPipelineDependencies();

  // Declare all query-level structure and functions.
  void DeclareCommonStructsAndFunctions();

  // Generate all query logic.
  void GenerateQueryLogic(const PipelineGraph &pipeline_graph, const Pipeline &main_pipeline);

  // Create a new container.
  CompilationUnit *MakeContainer();

 private:
  // Unique ID used as a prefix for all generated functions to ensure uniqueness.
  uint64_t unique_id_;
  // The compiled query object we'll update.
  ExecutableQuery *query_;
  // All allocated containers.
  std::vector<std::unique_ptr<CompilationUnit>> containers_;
  // The code generator instance.
  CodeGen codegen_;
  // Cached identifiers.
  ast::Identifier query_state_var_;
  // The query state and the slot in the state where the execution context is.
  ExecutionState query_state_;
  ExecutionState::Slot exec_ctx_;
  // The operator and expression translators.
  std::unordered_map<const planner::AbstractPlanNode *, std::unique_ptr<OperatorTranslator>>
      operators_;
  std::unordered_map<const planner::AbstractExpression *, std::unique_ptr<ExpressionTranslator>>
      expressions_;
};

}  // namespace tpl::sql::codegen
