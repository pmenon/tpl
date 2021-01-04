#include "sql/codegen/compilation_context.h"

#include <algorithm>
#include <atomic>

#include "spdlog/fmt/fmt.h"

#include "ast/ast.h"
#include "ast/context.h"
#include "common/exception.h"
#include "common/macros.h"
#include "logging/logger.h"
#include "sql/codegen/compilation_unit.h"
#include "sql/codegen/executable_query.h"
#include "sql/codegen/execution_plan.h"
#include "sql/codegen/expression//derived_value_translator.h"
#include "sql/codegen/expression/binary_expression_translator.h"
#include "sql/codegen/expression/case_translator.h"
#include "sql/codegen/expression/cast_translator.h"
#include "sql/codegen/expression/column_value_translator.h"
#include "sql/codegen/expression/comparison_translator.h"
#include "sql/codegen/expression/conjunction_translator.h"
#include "sql/codegen/expression/constant_translator.h"
#include "sql/codegen/expression/like_translator.h"
#include "sql/codegen/expression/unary_expression_translator.h"
#include "sql/codegen/function_builder.h"
#include "sql/codegen/operators/csv_scan_translator.h"
#include "sql/codegen/operators/hash_aggregation_translator.h"
#include "sql/codegen/operators/hash_join_translator.h"
#include "sql/codegen/operators/limit_translator.h"
#include "sql/codegen/operators/nested_loop_join_translator.h"
#include "sql/codegen/operators/operator_translator.h"
#include "sql/codegen/operators/output_translator.h"
#include "sql/codegen/operators/projection_translator.h"
#include "sql/codegen/operators/seq_scan_translator.h"
#include "sql/codegen/operators/sort_translator.h"
#include "sql/codegen/operators/static_aggregation_translator.h"
#include "sql/codegen/pipeline.h"
#include "sql/codegen/pipeline_graph.h"
#include "sql/planner/expressions/abstract_expression.h"
#include "sql/planner/expressions/case_expression.h"
#include "sql/planner/expressions/cast_expression.h"
#include "sql/planner/expressions/column_value_expression.h"
#include "sql/planner/expressions/comparison_expression.h"
#include "sql/planner/expressions/conjunction_expression.h"
#include "sql/planner/expressions/derived_value_expression.h"
#include "sql/planner/expressions/expression_util.h"
#include "sql/planner/expressions/unary_expression.h"
#include "sql/planner/plannodes/abstract_plan_node.h"
#include "sql/planner/plannodes/aggregate_plan_node.h"
#include "sql/planner/plannodes/csv_scan_plan_node.h"
#include "sql/planner/plannodes/hash_join_plan_node.h"
#include "sql/planner/plannodes/limit_plan_node.h"
#include "sql/planner/plannodes/nested_loop_join_plan_node.h"
#include "sql/planner/plannodes/order_by_plan_node.h"
#include "sql/planner/plannodes/projection_plan_node.h"
#include "sql/planner/plannodes/seq_scan_plan_node.h"
#include "sql/planner/plannodes/set_op_plan_node.h"
#include "util/timer.h"
#include "vm/module.h"

namespace tpl::sql::codegen {

namespace {
// A unique ID generator used to generate globally unique TPL function names.
std::atomic<uint64_t> kUniqueIds{0};
}  // namespace

CompilationContext::CompilationContext(ExecutableQuery *query)
    : unique_id_(kUniqueIds++),
      query_(query),
      codegen_(MakeContainer()),
      query_state_var_(codegen_.MakeIdentifier("q_state")),
      query_state_(&codegen_, "QueryState", [](CodeGen *codegen) {
        auto function = codegen->GetCurrentFunction();
        return function->GetParameterByPosition(0);
      }) {}

ast::FunctionDeclaration *CompilationContext::GenerateInitFunction() {
  const auto name = codegen_.MakeIdentifier(GetFunctionPrefix() + "_Init");
  FunctionBuilder builder(&codegen_, name, QueryParams(), codegen_.GetType<void>());
  {
    for (const auto &kv : operators_) {
      kv.second->InitializeQueryState(&builder);
    }
  }
  return builder.Finish();
}

ast::FunctionDeclaration *CompilationContext::GenerateTearDownFunction() {
  const auto name = codegen_.MakeIdentifier(GetFunctionPrefix() + "_TearDown");
  FunctionBuilder builder(&codegen_, name, QueryParams(), codegen_.GetType<void>());
  {
    for (const auto &kv : operators_) {
      kv.second->TearDownQueryState(&builder);
    }
  }
  return builder.Finish();
}

void CompilationContext::DeclareCommonQueryState() {
  exec_ctx_ =
      query_state_.DeclareStateEntry("exec_ctx", codegen_.GetType<ast::x::ExecutionContext *>());
}

void CompilationContext::EstablishPipelineDependencies() {
  for (const auto &kv : operators_) {
    kv.second->DeclarePipelineDependencies();
  }
}

void CompilationContext::DeclareCommonStructsAndFunctions() {
  // Let each operator declare helper elements.
  for (const auto &kv : operators_) {
    kv.second->DefineStructsAndFunctions();
  }
  // Finally, declare the query state.
  query_state_.ConstructFinalType();
}

void CompilationContext::GenerateQueryLogic(const PipelineGraph &pipeline_graph,
                                            const Pipeline &main_pipeline) {
  // Now we're ready to generate some code.
  // First, generate the query state initialization and tear-down logic.
  ast::FunctionDeclaration *init_fn = GenerateInitFunction();
  ast::FunctionDeclaration *tear_down_fn = GenerateTearDownFunction();

  // Next, generate all pipeline code.
  // Optimize (prematurely?) by reserving now.
  std::vector<ExecutionStep> steps;
  steps.reserve(pipeline_graph.NumPipelines() * 3);

  // Determine order.
  std::vector<const Pipeline *> pipeline_exec_order;
  pipeline_graph.CollectTransitiveDependencies(main_pipeline, &pipeline_exec_order);

  // Generate!
  for (auto pipeline : pipeline_exec_order) {
    // Prepare and generate the pipeline steps.
    auto exec_funcs = pipeline->GeneratePipelineLogic();
    // Each generated function becomes an execution step in the order
    // provided by the pipeline.
    for (auto func : exec_funcs) {
      steps.emplace_back(pipeline->GetId(), func->GetName().ToString());
    }
  }

  // Compile the single container containing all the code.
  std::vector<std::unique_ptr<vm::Module>> modules(1);
  modules[0] = containers_[0]->Compile();

  // Check compilation error.
  if (modules[0] == nullptr) {
    throw Exception(ExceptionType::CodeGen, "Error compiling query module!");
  }

  // Resolve all the steps.
  for (auto &step : steps) {
    step.Resolve(modules[0].get());
  }

  // Setup query and finish.
  vm::Module *main_module = modules[0].get();
  query_->Setup(std::move(modules),                  // All compiled modules.
                main_module,                         // Where init/teardown functions exist.
                init_fn->GetName().ToString(),       // The init() function.
                tear_down_fn->GetName().ToString(),  // The teardown() function.
                ExecutionPlan(std::move(steps)),     // The generated plan.
                query_state_.GetSizeRaw());
}

void CompilationContext::GeneratePlan(const planner::AbstractPlanNode &plan) {
  // Common state.
  DeclareCommonQueryState();

  // The graph of all pipelines.
  PipelineGraph pipeline_graph;

  // The main pipeline.
  Pipeline main_pipeline(this, &pipeline_graph);

  // Recursively prepare all translators for the query.
  PrepareOut(plan, &main_pipeline);

  // Let operators declare dependencies between their pipelines.
  // Some operators have complex pipeline relationships.
  EstablishPipelineDependencies();

  // Let operators declare helper structs and functions.
  // These are query-level and available to all pipeline functions.
  DeclareCommonStructsAndFunctions();

  // Generate all query logic. This includes pipelines.
  GenerateQueryLogic(pipeline_graph, main_pipeline);
}

// static
std::unique_ptr<ExecutableQuery> CompilationContext::Compile(
    const planner::AbstractPlanNode &plan) {
  // The query we're generating code for.
  auto query = std::make_unique<ExecutableQuery>(plan);

  // Time the generation/compilation process.
  util::Timer<std::milli> timer;
  timer.Start();

  // Generate the plan for the query.
  CompilationContext ctx(query.get());
  ctx.GeneratePlan(plan);

  timer.Stop();
  LOG_DEBUG("Compilation time: {:.2f} ms", timer.GetElapsed());

  return query;
}

void CompilationContext::PrepareOut(const planner::AbstractPlanNode &plan, Pipeline *pipeline) {
  auto translator = std::make_unique<OutputTranslator>(plan, this, pipeline);
  operators_[nullptr] = std::move(translator);
}

void CompilationContext::Prepare(const planner::AbstractPlanNode &plan, Pipeline *pipeline) {
  std::unique_ptr<OperatorTranslator> translator;

  switch (plan.GetPlanNodeType()) {
    case planner::PlanNodeType::AGGREGATE: {
      const auto &aggregation = static_cast<const planner::AggregatePlanNode &>(plan);
      if (aggregation.GetAggregateStrategyType() == planner::AggregateStrategyType::SORTED) {
        throw NotImplementedException("Code generation for sort-based aggregations");
      }
      if (aggregation.GetGroupByTerms().empty()) {
        translator = std::make_unique<StaticAggregationTranslator>(aggregation, this, pipeline);
      } else {
        translator = std::make_unique<HashAggregationTranslator>(aggregation, this, pipeline);
      }
      break;
    }
    case planner::PlanNodeType::CSVSCAN: {
      const auto &scan_plan = static_cast<const planner::CSVScanPlanNode &>(plan);
      translator = std::make_unique<CSVScanTranslator>(scan_plan, this, pipeline);
      break;
    }
    case planner::PlanNodeType::HASHJOIN: {
      const auto &hash_join = static_cast<const planner::HashJoinPlanNode &>(plan);
      translator = std::make_unique<HashJoinTranslator>(hash_join, this, pipeline);
      break;
    }
    case planner::PlanNodeType::LIMIT: {
      const auto &limit = static_cast<const planner::LimitPlanNode &>(plan);
      translator = std::make_unique<LimitTranslator>(limit, this, pipeline);
      break;
    }
    case planner::PlanNodeType::NESTLOOP: {
      const auto &nested_loop = static_cast<const planner::NestedLoopJoinPlanNode &>(plan);
      translator = std::make_unique<NestedLoopJoinTranslator>(nested_loop, this, pipeline);
      break;
    }
    case planner::PlanNodeType::ORDERBY: {
      const auto &sort = static_cast<const planner::OrderByPlanNode &>(plan);
      translator = std::make_unique<SortTranslator>(sort, this, pipeline);
      break;
    }
    case planner::PlanNodeType::PROJECTION: {
      const auto &projection = static_cast<const planner::ProjectionPlanNode &>(plan);
      translator = std::make_unique<ProjectionTranslator>(projection, this, pipeline);
      break;
    }
    case planner::PlanNodeType::SEQSCAN: {
      const auto &seq_scan = static_cast<const planner::SeqScanPlanNode &>(plan);
      translator = std::make_unique<SeqScanTranslator>(seq_scan, this, pipeline);
      break;
    }
    default: {
      throw NotImplementedException(
          fmt::format("code generation for plan node type '{}'",
                      planner::PlanNodeTypeToString(plan.GetPlanNodeType())));
    }
  }

  operators_[&plan] = std::move(translator);
}

void CompilationContext::Prepare(const planner::AbstractExpression &expression) {
  std::unique_ptr<ExpressionTranslator> translator;

  switch (expression.GetExpressionType()) {
    case planner::ExpressionType::CASE: {
      const auto &case_expr = static_cast<const planner::CaseExpression &>(expression);
      translator = std::make_unique<CaseTranslator>(case_expr, this);
      break;
    }
    case planner::ExpressionType::CAST: {
      const auto &cast_expr = static_cast<const planner::CastExpression &>(expression);
      translator = std::make_unique<CastTranslator>(cast_expr, this);
      break;
    }
    case planner::ExpressionType::COLUMN_VALUE: {
      const auto &column_value = static_cast<const planner::ColumnValueExpression &>(expression);
      translator = std::make_unique<ColumnValueTranslator>(column_value, this);
      break;
    }
    case planner::ExpressionType::COMPARISON: {
      const auto &comparison = static_cast<const planner::ComparisonExpression &>(expression);
      if (planner::ExpressionUtil::IsLikeComparison(comparison)) {
        translator = std::make_unique<LikeTranslator>(comparison, this);
      } else {
        translator = std::make_unique<ComparisonTranslator>(comparison, this);
      }
      break;
    }
    case planner::ExpressionType::CONJUNCTION: {
      const auto &conjunction = static_cast<const planner::ConjunctionExpression &>(expression);
      translator = std::make_unique<ConjunctionTranslator>(conjunction, this);
      break;
    }
    case planner::ExpressionType::UNARY_OPERATOR: {
      const auto &unary_expression = static_cast<const planner::UnaryExpression &>(expression);
      translator = std::make_unique<UnaryExpressionTranslator>(unary_expression, this);
      break;
    }
    case planner::ExpressionType::BINARY_OPERATOR: {
      const auto &binary_expression = static_cast<const planner::BinaryExpression &>(expression);
      translator = std::make_unique<BinaryExpressionTranslator>(binary_expression, this);
      break;
    }
    case planner::ExpressionType::CONSTANT: {
      const auto &constant = static_cast<const planner::ConstantValueExpression &>(expression);
      translator = std::make_unique<ConstantTranslator>(constant, this);
      break;
    }
    case planner::ExpressionType::DERIVED_VALUE: {
      const auto &derived_value = static_cast<const planner::DerivedValueExpression &>(expression);
      translator = std::make_unique<DerivedValueTranslator>(derived_value, this);
      break;
    }
    default: {
      throw NotImplementedException(
          fmt::format("Code generation for expression type '{}' not supported",
                      planner::ExpressionTypeToString(expression.GetExpressionType())));
    }
  }

  expressions_[&expression] = std::move(translator);
}

OperatorTranslator *CompilationContext::LookupTranslator(
    const planner::AbstractPlanNode &node) const {
  if (auto iter = operators_.find(&node); iter != operators_.end()) {
    return iter->second.get();
  }
  return nullptr;
}

ExpressionTranslator *CompilationContext::LookupTranslator(
    const planner::AbstractExpression &expr) const {
  if (auto iter = expressions_.find(&expr); iter != expressions_.end()) {
    return iter->second.get();
  }
  return nullptr;
}

std::string CompilationContext::GetFunctionPrefix() const {
  return "Query" + std::to_string(unique_id_);
}

std::vector<std::pair<ast::Identifier, ast::Type *>> CompilationContext::QueryParams() const {
  return {{query_state_var_, query_state_.GetPointerToType()}};
}

edsl::Value<ast::x::ExecutionContext *> CompilationContext::GetExecutionContextPtrFromQueryState() {
  return query_state_.GetStateEntry<ast::x::ExecutionContext *>(&codegen_, exec_ctx_);
}

CompilationUnit *CompilationContext::MakeContainer() {
  const auto container_id = fmt::format("CU{}", containers_.size());
  containers_.emplace_back(std::make_unique<CompilationUnit>(query_->GetContext(), container_id));
  return containers_.back().get();
}

}  // namespace tpl::sql::codegen
