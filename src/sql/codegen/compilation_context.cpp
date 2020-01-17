#include "sql/codegen/compilation_context.h"

#include <algorithm>
#include <atomic>

#include "spdlog/fmt/fmt.h"

#include "common/exception.h"
#include "common/macros.h"
#include "sql/codegen/codegen.h"
#include "sql/codegen/executable_query.h"
#include "sql/codegen/expression//derived_value_translator.h"
#include "sql/codegen/expression/arithmetic_translator.h"
#include "sql/codegen/expression/column_value_translator.h"
#include "sql/codegen/expression/comparison_translator.h"
#include "sql/codegen/expression/conjunction_translator.h"
#include "sql/codegen/expression/constant_translator.h"
#include "sql/codegen/function_builder.h"
#include "sql/codegen/operators/nested_loop_join_translator.h"
#include "sql/codegen/operators/operator_translator.h"
#include "sql/codegen/operators/seq_scan_translator.h"
#include "sql/codegen/operators/sort_translator.h"
#include "sql/codegen/pipeline.h"
#include "sql/codegen/top_level_declarations.h"
#include "sql/planner/expressions/abstract_expression.h"
#include "sql/planner/expressions/aggregate_expression.h"
#include "sql/planner/expressions/column_value_expression.h"
#include "sql/planner/expressions/comparison_expression.h"
#include "sql/planner/expressions/conjunction_expression.h"
#include "sql/planner/expressions/derived_value_expression.h"
#include "sql/planner/expressions/operator_expression.h"
#include "sql/planner/plannodes/abstract_plan_node.h"
#include "sql/planner/plannodes/aggregate_plan_node.h"
#include "sql/planner/plannodes/csv_scan_plan_node.h"
#include "sql/planner/plannodes/hash_join_plan_node.h"
#include "sql/planner/plannodes/nested_loop_join_plan_node.h"
#include "sql/planner/plannodes/order_by_plan_node.h"
#include "sql/planner/plannodes/seq_scan_plan_node.h"
#include "sql/planner/plannodes/set_op_plan_node.h"

namespace tpl::sql::codegen {

namespace {
std::atomic<uint64_t> kUniqueIds{0};
}  // namespace

CompilationContext::CompilationContext(ExecutableQuery *query, const CompilationMode mode)
    : unique_id_(kUniqueIds++),
      query_(query),
      mode_(mode),
      codegen_(query_->GetContext()),
      query_state_var_(codegen_.MakeIdentifier("queryState")),
      query_state_type_name_(codegen_.MakeIdentifier("QueryState")) {}

ast::FunctionDecl *CompilationContext::GenerateInitFunction() {
  const auto name = codegen_.MakeIdentifier(fmt::format("{}_Init", GetFunctionPrefix()));
  FunctionBuilder builder(&codegen_, name, QueryParams(), codegen_.Nil());
  {
    for (auto &[_, op] : ops_) {
      op->InitializeQueryState();
    }
  }
  return builder.Finish();
}

ast::FunctionDecl *CompilationContext::GenerateTearDownFunction() {
  const auto name = codegen_.MakeIdentifier(fmt::format("{}_TearDown", GetFunctionPrefix()));
  FunctionBuilder builder(&codegen_, name, QueryParams(), codegen_.Nil());
  {
    for (auto &[_, op] : ops_) {
      op->TearDownQueryState();
    }
  }
  return builder.Finish();
}

void CompilationContext::GenerateHelperStructsAndFunctions(TopLevelDeclarations *top_level_decls) {
  for (auto &[_, op] : ops_) {
    op->DefineHelperStructs(top_level_decls);
    op->DefineHelperFunctions(top_level_decls);
  }
}

void CompilationContext::GeneratePlan(const planner::AbstractPlanNode &plan) {
  exec_ctx_slot_ = query_state_.DeclareStateEntry(
      GetCodeGen(), "execCtx", codegen_.PointerType(ast::BuiltinType::ExecutionContext));

  // Prepare all translators for the plan.
  Pipeline main_pipeline(this);
  Prepare(plan, &main_pipeline);

  // The main init/tear down container.
  auto main_container = std::make_unique<CodeContainer>(query_->GetContext());

  // First, build-up and declare the query state for the whole query.
  main_container->RegisterStruct(
      query_state_.ConstructFinalType(GetCodeGen(), query_state_type_name_));

  // Declare helper structs and functions
  TopLevelDeclarations top_level_declarations;
  GenerateHelperStructsAndFunctions(&top_level_declarations);
  top_level_declarations.DeclareInContainer(main_container.get());

  // Find pipeline execution order now.
  std::vector<Pipeline *> execution_order;
  main_pipeline.CollectDependencies(&execution_order);

  for (auto *pipeline : execution_order) {
    pipeline->GeneratePipeline(main_container.get());
  }

  // Declare query init and tear-down functions.
  main_container->RegisterFunction(GenerateInitFunction());
  main_container->RegisterFunction(GenerateTearDownFunction());

  // Setup the query with the generated fragments and return.
  std::vector<std::unique_ptr<CodeContainer>> fragments;
  fragments.emplace_back(std::move(main_container));

  // TODO(pmenon): This isn't correct. We need to report errors back up.
  // Compile each
  for (auto &fragment : fragments) {
    fragment->Compile();
  }

  // Done
  query_->Setup(std::move(fragments), query_state_.GetSize());
}

// static
std::unique_ptr<ExecutableQuery> CompilationContext::Compile(const planner::AbstractPlanNode &plan,
                                                             const CompilationMode mode) {
  // The query we're generating code for.
  auto query = std::make_unique<ExecutableQuery>(plan);

  // Generate the plan for the query
  CompilationContext ctx(query.get(), mode);
  ctx.GeneratePlan(plan);

  // Done
  return query;
}

uint32_t CompilationContext::RegisterPipeline(Pipeline *pipeline) {
  TPL_ASSERT(std::find(pipelines_.begin(), pipelines_.end(), pipeline) == pipelines_.end(),
             "Duplicate pipeline in context");
  pipelines_.push_back(pipeline);
  return pipelines_.size();
}

void CompilationContext::Prepare(const planner::AbstractPlanNode &plan, Pipeline *pipeline) {
  std::unique_ptr<OperatorTranslator> translator;

  switch (plan.GetPlanNodeType()) {
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
    case planner::PlanNodeType::SEQSCAN: {
      const auto &seq_scan = static_cast<const planner::SeqScanPlanNode &>(plan);
      translator = std::make_unique<SeqScanTranslator>(seq_scan, this, pipeline);
      break;
    }
    default: {
      throw NotImplementedException("Code generation for plan node type '{}' not supported",
                                    planner::PlanNodeTypeToString(plan.GetPlanNodeType()));
    }
  }

  ops_[&plan] = std::move(translator);
}

void CompilationContext::Prepare(const planner::AbstractExpression &expression) {
  std::unique_ptr<ExpressionTranslator> translator;

  switch (expression.GetExpressionType()) {
    case planner::ExpressionType::COLUMN_VALUE: {
      const auto &column_value = static_cast<const planner::ColumnValueExpression &>(expression);
      translator = std::make_unique<ColumnValueTranslator>(column_value, this);
      break;
    }
    case planner::ExpressionType::COMPARE_EQUAL:
    case planner::ExpressionType::COMPARE_GREATER_THAN:
    case planner::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO:
    case planner::ExpressionType::COMPARE_LESS_THAN:
    case planner::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO:
    case planner::ExpressionType::COMPARE_NOT_EQUAL:
    case planner::ExpressionType::COMPARE_LIKE:
    case planner::ExpressionType::COMPARE_NOT_LIKE: {
      const auto &comparison = static_cast<const planner::ComparisonExpression &>(expression);
      translator = std::make_unique<ComparisonTranslator>(comparison, this);
      break;
    }
    case planner::ExpressionType::CONJUNCTION_AND:
    case planner::ExpressionType::CONJUNCTION_OR: {
      const auto &conjunction = static_cast<const planner::ConjunctionExpression &>(expression);
      translator = std::make_unique<ConjunctionTranslator>(conjunction, this);
      break;
    }
    case planner::ExpressionType::OPERATOR_PLUS:
    case planner::ExpressionType::OPERATOR_MINUS:
    case planner::ExpressionType::OPERATOR_MULTIPLY:
    case planner::ExpressionType::OPERATOR_DIVIDE:
    case planner::ExpressionType::OPERATOR_MOD: {
      const auto &operator_expr = static_cast<const planner::OperatorExpression &>(expression);
      translator = std::make_unique<ArithmeticTranslator>(operator_expr, this);
      break;
    }
    case planner::ExpressionType::VALUE_CONSTANT: {
      const auto &constant = static_cast<const planner::ConstantValueExpression &>(expression);
      translator = std::make_unique<ConstantTranslator>(constant, this);
      break;
    }
    case planner::ExpressionType::VALUE_TUPLE: {
      const auto &derived_value = static_cast<const planner::DerivedValueExpression &>(expression);
      translator = std::make_unique<DerivedValueTranslator>(derived_value, this);
      break;
    }
    default: {
      throw NotImplementedException(
          "Code generation for expression type '{}' not supported",
          planner::ExpressionTypeToString(expression.GetExpressionType(), false));
    }
  }

  expressions_[&expression] = std::move(translator);
}

OperatorTranslator *CompilationContext::LookupTranslator(
    const planner::AbstractPlanNode &node) const {
  if (auto iter = ops_.find(&node); iter != ops_.end()) {
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
  return fmt::format("Query{}", unique_id_);
}

util::RegionVector<ast::FieldDecl *> CompilationContext::QueryParams() const {
  ast::Expr *state_type = codegen_.PointerType(codegen_.MakeExpr(query_state_type_name_));
  ast::FieldDecl *field = codegen_.MakeField(query_state_var_, state_type);
  return codegen_.MakeFieldList({field});
}

}  // namespace tpl::sql::codegen
