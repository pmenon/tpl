#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "sql/planner/expressions/abstract_expression.h"
#include "sql/planner/expressions/aggregate_expression.h"
#include "sql/planner/expressions/column_value_expression.h"
#include "sql/planner/expressions/comparison_expression.h"
#include "sql/planner/expressions/conjunction_expression.h"
#include "sql/planner/expressions/constant_value_expression.h"
#include "sql/planner/expressions/derived_value_expression.h"
#include "sql/planner/expressions/operator_expression.h"
#include "sql/value.h"

namespace tpl::sql::planner {

/**
 * Helper class to reduce typing and increase readability when hand crafting expression.
 */
class ExpressionMaker {
  using NewExpression = std::unique_ptr<planner::AbstractExpression>;
  using NewAggExpression = std::unique_ptr<planner::AggregateExpression>;

 public:
  using Expression = const planner::AbstractExpression *;
  using AggExpression = const planner::AggregateExpression *;

  /**
   * Create an integer constant expression
   */
  Expression Constant(int32_t val) {
    return Alloc(
        std::make_unique<planner::ConstantValueExpression>(sql::GenericValue::CreateInteger(val)));
  }

  /**
   * Create a floating point constant expression
   */
  Expression Constant(float val) {
    return Alloc(
        std::make_unique<planner::ConstantValueExpression>(sql::GenericValue::CreateFloat(val)));
  }

  /**
   * Create a date constant expression
   */
  Expression Constant(int16_t year, uint8_t month, uint8_t day) {
    return Alloc(std::make_unique<planner::ConstantValueExpression>(
        sql::GenericValue::CreateDate(static_cast<uint32_t>(year), month, day)));
  }

  /**
   * Create a column value expression
   */
  Expression CVE(uint16_t column_oid, sql::TypeId type) {
    return Alloc(std::make_unique<planner::ColumnValueExpression>(column_oid, type));
  }

  /**
   * Create a derived value expression
   */
  Expression DVE(sql::TypeId type, int tuple_idx, int value_idx) {
    return Alloc(std::make_unique<planner::DerivedValueExpression>(type, tuple_idx, value_idx));
  }

  /**
   * Create a Comparison expression
   */
  Expression Compare(planner::ExpressionType comp_type, Expression child1, Expression child2) {
    return Alloc(std::make_unique<planner::ComparisonExpression>(
        comp_type, std::vector<Expression>{child1, child2}));
  }

  /**
   *  expression for child1 == child2
   */
  Expression CompareEq(Expression child1, Expression child2) {
    return Compare(planner::ExpressionType::COMPARE_EQUAL, child1, child2);
  }

  /**
   * Create expression for child1 == child2
   */
  Expression CompareNeq(Expression child1, Expression child2) {
    return Compare(planner::ExpressionType::COMPARE_NOT_EQUAL, child1, child2);
  }

  /**
   * Create expression for child1 < child2
   */
  Expression CompareLt(Expression child1, Expression child2) {
    return Compare(planner::ExpressionType::COMPARE_LESS_THAN, child1, child2);
  }

  /**
   * Create expression for child1 <= child2
   */
  Expression CompareLe(Expression child1, Expression child2) {
    return Compare(planner::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO, child1, child2);
  }

  /**
   * Create expression for child1 > child2
   */
  Expression CompareGt(Expression child1, Expression child2) {
    return Compare(planner::ExpressionType::COMPARE_GREATER_THAN, child1, child2);
  }

  /**
   * Create expression for child1 >= child2
   */
  Expression CompareGe(Expression child1, Expression child2) {
    return Compare(planner::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO, child1, child2);
  }

  /**
   * Create a unary operation expression
   */
  Expression Operator(planner::ExpressionType op_type, sql::TypeId ret_type, Expression child) {
    return Alloc(std::make_unique<planner::OperatorExpression>(op_type, ret_type,
                                                               std::vector<Expression>{child}));
  }

  /**
   * Create a binary operation expression
   */
  Expression Operator(planner::ExpressionType op_type, sql::TypeId ret_type, Expression child1,
                      Expression child2) {
    return Alloc(std::make_unique<planner::OperatorExpression>(
        op_type, ret_type, std::vector<Expression>{child1, child2}));
  }

  /**
   * create expression for child1 + child2
   */
  Expression OpSum(Expression child1, Expression child2) {
    return Operator(planner::ExpressionType::OPERATOR_PLUS, child1->GetReturnValueType(), child1,
                    child2);
  }

  /**
   * create expression for child1 - child2
   */
  Expression OpMin(Expression child1, Expression child2) {
    return Operator(planner::ExpressionType::OPERATOR_MINUS, child1->GetReturnValueType(), child1,
                    child2);
  }

  /**
   * create expression for child1 * child2
   */
  Expression OpMul(Expression child1, Expression child2) {
    return Operator(planner::ExpressionType::OPERATOR_MULTIPLY, child1->GetReturnValueType(),
                    child1, child2);
  }

  /**
   * create expression for child1 / child2
   */
  Expression OpDiv(Expression child1, Expression child2) {
    return Operator(planner::ExpressionType::OPERATOR_DIVIDE, child1->GetReturnValueType(), child1,
                    child2);
  }

  /**
   * Create expression for child1 AND/OR child2
   */
  Expression Conjunction(planner::ExpressionType op_type, Expression child1, Expression child2) {
    return Alloc(std::make_unique<planner::ConjunctionExpression>(
        op_type, std::vector<Expression>{child1, child2}));
  }

  /**
   * Create expression for child1 AND child2
   */
  Expression ConjunctionAnd(Expression child1, Expression child2) {
    return Conjunction(planner::ExpressionType::CONJUNCTION_AND, child1, child2);
  }

  /**
   * Create expression for child1 OR child2
   */
  Expression ConjunctionOr(Expression child1, Expression child2) {
    return Conjunction(planner::ExpressionType::CONJUNCTION_OR, child1, child2);
  }

  /**
   * Create an aggregate expression
   */
  AggExpression AggregateTerm(planner::ExpressionType agg_type, Expression child) {
    return Alloc(std::make_unique<planner::AggregateExpression>(
        agg_type, std::vector<Expression>{child}, false));
  }

  /**
   * Create a sum aggregate expression
   */
  AggExpression AggSum(Expression child) {
    return AggregateTerm(planner::ExpressionType::AGGREGATE_SUM, child);
  }

  /**
   * Create a avg aggregate expression
   */
  AggExpression AggAvg(Expression child) {
    return AggregateTerm(planner::ExpressionType::AGGREGATE_AVG, child);
  }

  /**
   * Create a count aggregate expression
   */
  AggExpression AggCount(Expression child) {
    return AggregateTerm(planner::ExpressionType::AGGREGATE_COUNT, child);
  }

 private:
  Expression Alloc(NewExpression &&new_expr) {
    allocated_exprs_.emplace_back(std::move(new_expr));
    return allocated_exprs_.back().get();
  }

  AggExpression Alloc(NewAggExpression &&agg_expr) {
    allocated_aggs_.emplace_back(std::move(agg_expr));
    return allocated_aggs_.back().get();
  }

  std::vector<NewExpression> allocated_exprs_;
  std::vector<NewAggExpression> allocated_aggs_;
};

}  // namespace tpl::sql::planner