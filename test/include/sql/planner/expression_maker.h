#pragma once

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ast/builtins.h"
#include "sql/planner/expressions/abstract_expression.h"
#include "sql/planner/expressions/aggregate_expression.h"
#include "sql/planner/expressions/binary_expression.h"
#include "sql/planner/expressions/case_expression.h"
#include "sql/planner/expressions/cast_expression.h"
#include "sql/planner/expressions/column_value_expression.h"
#include "sql/planner/expressions/comparison_expression.h"
#include "sql/planner/expressions/conjunction_expression.h"
#include "sql/planner/expressions/constant_value_expression.h"
#include "sql/planner/expressions/derived_value_expression.h"
#include "sql/planner/expressions/unary_expression.h"
#include "sql/schema.h"

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
   * @return A constant expression with the given boolean value.
   */
  Expression ConstantBool(bool val) {
    return Alloc(
        std::make_unique<planner::ConstantValueExpression>(GenericValue::CreateBoolean(val)));
  }

  /**
   * Create an integer constant expression
   */
  Expression Constant(int32_t val) {
    return Alloc(
        std::make_unique<planner::ConstantValueExpression>(GenericValue::CreateInteger(val)));
  }

  /**
   * Create a floating point constant expression
   */
  Expression Constant(float val) {
    return Alloc(
        std::make_unique<planner::ConstantValueExpression>(GenericValue::CreateFloat(val)));
  }

  /**
   * Create a date constant expression
   */
  Expression Constant(int16_t year, uint8_t month, uint8_t day) {
    return Alloc(std::make_unique<planner::ConstantValueExpression>(
        GenericValue::CreateDate(static_cast<uint32_t>(year), month, day)));
  }

  /**
   * Create a string constant expression
   */
  Expression Constant(const std::string &str) {
    return Alloc(
        std::make_unique<planner::ConstantValueExpression>(GenericValue::CreateVarchar(str)));
  }

  /**
   * Create a column value expression
   */
  Expression CVE(const Schema::ColumnInfo &col) {
    return Alloc(std::make_unique<planner::ColumnValueExpression>(col.oid, col.type));
  }

  /**
   * Create a column value expression
   */
  Expression CVE(uint16_t col_oid, const Type &type) {
    return Alloc(std::make_unique<planner::ColumnValueExpression>(col_oid, type));
  }

  /**
   * Create a derived value expression
   */
  Expression DVE(const Type &type, int tuple_idx, int value_idx) {
    return Alloc(std::make_unique<planner::DerivedValueExpression>(type, tuple_idx, value_idx));
  }

  /**
   * Create a Comparison expression
   */
  Expression Compare(planner::ComparisonKind cmp_kind, Expression child1, Expression child2) {
    return Alloc(std::make_unique<planner::ComparisonExpression>(
        cmp_kind, std::vector<Expression>{child1, child2}));
  }

  /**
   *  expression for child1 == child2
   */
  Expression CompareEq(Expression child1, Expression child2) {
    return Compare(planner::ComparisonKind::EQUAL, child1, child2);
  }

  /**
   * Create expression for child1 == child2
   */
  Expression CompareNeq(Expression child1, Expression child2) {
    return Compare(planner::ComparisonKind::NOT_EQUAL, child1, child2);
  }

  /**
   * Create expression for child1 < child2
   */
  Expression CompareLt(Expression child1, Expression child2) {
    return Compare(planner::ComparisonKind::LESS_THAN, child1, child2);
  }

  /**
   * Create expression for child1 <= child2
   */
  Expression CompareLe(Expression child1, Expression child2) {
    return Compare(planner::ComparisonKind::LESS_THAN_OR_EQUAL_TO, child1, child2);
  }

  /**
   * Create expression for child1 > child2
   */
  Expression CompareGt(Expression child1, Expression child2) {
    return Compare(planner::ComparisonKind::GREATER_THAN, child1, child2);
  }

  /**
   * Create expression for child1 >= child2
   */
  Expression CompareGe(Expression child1, Expression child2) {
    return Compare(planner::ComparisonKind::GREATER_THAN_OR_EQUAL_TO, child1, child2);
  }

  /**
   * Create a BETWEEN expression: low <= input <= high
   */
  Expression CompareBetween(Expression input, Expression low, Expression high) {
    return Alloc(std::make_unique<planner::ComparisonExpression>(
        planner::ComparisonKind::BETWEEN, std::vector<Expression>({input, low, high})));
  }

  /**
   * Create a LIKE() expression.
   */
  Expression CompareLike(Expression input, Expression s) {
    return Compare(planner::ComparisonKind::LIKE, input, s);
  }

  /**
   * Create a NOT LIKE() expression.
   */
  Expression CompareNotLike(Expression input, Expression s) {
    return Compare(planner::ComparisonKind::NOT_LIKE, input, s);
  }

  /**
   * Create a unary operation expression
   */
  Expression UnaryOperator(KnownOperator op, const Type &ret_type, Expression child) {
    return Alloc(std::make_unique<planner::UnaryExpression>(ret_type, op, child));
  }

  /**
   * Create a binary operation expression
   */
  Expression BinaryOperator(KnownOperator op, const Type &ret_type, Expression child1,
                            Expression child2) {
    return Alloc(std::make_unique<planner::BinaryExpression>(ret_type, op, child1, child2));
  }

  /**
   * Cast the input expression to the given type.
   */
  Expression OpCast(Expression input, const Type &to_type) {
    return Alloc(std::make_unique<planner::CastExpression>(to_type, input));
  }

  /**
   * create expression for child1 + child2
   */
  Expression OpSum(Expression child1, Expression child2) {
    return BinaryOperator(KnownOperator::Add, child1->GetReturnValueType(), child1, child2);
  }

  /**
   * create expression for child1 - child2
   */
  Expression OpMin(Expression child1, Expression child2) {
    return BinaryOperator(KnownOperator::Sub, child1->GetReturnValueType(), child1, child2);
  }

  /**
   * create expression for child1 * child2
   */
  Expression OpMul(Expression child1, Expression child2) {
    return BinaryOperator(KnownOperator::Mul, child1->GetReturnValueType(), child1, child2);
  }

  /**
   * create expression for child1 / child2
   */
  Expression OpDiv(Expression child1, Expression child2) {
    return BinaryOperator(KnownOperator::Div, child1->GetReturnValueType(), child1, child2);
  }

  /**
   * Create expression for NOT(child)
   */
  Expression OpNot(Expression child) {
    return UnaryOperator(KnownOperator::LogicalNot, Type::BooleanType(child->HasNullableValue()),
                         child);
  }

  /**
   * Create expression for child1 AND/OR child2
   */
  Expression Conjunction(planner::ConjunctionKind kind, Expression child1, Expression child2) {
    return Alloc(std::make_unique<planner::ConjunctionExpression>(
        kind, std::vector<Expression>{child1, child2}));
  }

  /**
   * Create expression for child1 AND child2
   */
  Expression ConjunctionAnd(Expression child1, Expression child2) {
    return Conjunction(planner::ConjunctionKind::AND, child1, child2);
  }

  /**
   * Create expression for child1 OR child2
   */
  Expression ConjunctionOr(Expression child1, Expression child2) {
    return Conjunction(planner::ConjunctionKind::OR, child1, child2);
  }

  /**
   * Create an aggregate expression
   */
  AggExpression AggregateTerm(planner::AggregateKind kind, Expression child, bool distinct) {
    return Alloc(std::make_unique<planner::AggregateExpression>(
        kind, std::vector<Expression>{child}, distinct));
  }

  /**
   * Create a sum aggregate expression
   */
  AggExpression AggSum(Expression child, bool distinct = false) {
    return AggregateTerm(planner::AggregateKind::SUM, child, distinct);
  }

  /**
   * Create a sum aggregate expression
   */
  AggExpression AggMin(Expression child, bool distinct = false) {
    return AggregateTerm(planner::AggregateKind::MIN, child, distinct);
  }

  /**
   * Create a sum aggregate expression
   */
  AggExpression AggMax(Expression child, bool distinct = false) {
    return AggregateTerm(planner::AggregateKind::MAX, child, distinct);
  }

  /**
   * Create a avg aggregate expression
   */
  AggExpression AggAvg(Expression child, bool distinct = false) {
    return AggregateTerm(planner::AggregateKind::AVG, child, distinct);
  }

  /**
   * Create a count aggregate expression
   */
  AggExpression AggCount(Expression child, bool distinct = false) {
    return AggregateTerm(planner::AggregateKind::COUNT, child, distinct);
  }

  /**
   * Create a count aggregate expression
   */
  AggExpression AggCountStar() {
    return AggregateTerm(planner::AggregateKind::COUNT_STAR, Constant(1), false);
  }

  /**
   * Create a case expression with no default result.
   */
  Expression Case(const std::vector<std::pair<Expression, Expression>> &cases) {
    return Case(cases, nullptr);
  }

  /**
   * Create a case expression.
   */
  Expression Case(const std::vector<std::pair<Expression, Expression>> &cases,
                  Expression default_val) {
    const auto ret_type = cases[0].second->GetReturnValueType();
    std::vector<CaseExpression::WhenClause> clauses;
    clauses.reserve(cases.size());
    std::ranges::transform(cases, std::back_inserter(clauses), [](auto &p) {
      return CaseExpression::WhenClause{p.first, p.second};
    });
    return Alloc(std::make_unique<CaseExpression>(ret_type, clauses, default_val));
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
