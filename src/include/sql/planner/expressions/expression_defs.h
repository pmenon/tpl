#pragma once

#include <string>

#include "common/macros.h"

namespace tpl::sql::planner {

/**
 * All possible expression types.
 * TODO(WAN): Do we really need some of this stuff? e.g. HASH_RANGE for elastic
 */
enum class ExpressionType : uint8_t {
  INVALID,

  OPERATOR_UNARY_MINUS,
  OPERATOR_PLUS,
  OPERATOR_MINUS,
  OPERATOR_MULTIPLY,
  OPERATOR_DIVIDE,
  OPERATOR_CONCAT,
  OPERATOR_MOD,
  OPERATOR_CAST,
  OPERATOR_NOT,
  OPERATOR_IS_NULL,
  OPERATOR_IS_NOT_NULL,
  OPERATOR_EXISTS,

  COMPARE_EQUAL,
  COMPARE_NOT_EQUAL,
  COMPARE_LESS_THAN,
  COMPARE_GREATER_THAN,
  COMPARE_LESS_THAN_OR_EQUAL_TO,
  COMPARE_GREATER_THAN_OR_EQUAL_TO,
  COMPARE_LIKE,
  COMPARE_NOT_LIKE,
  COMPARE_IN,
  COMPARE_IS_DISTINCT_FROM,

  CONJUNCTION_AND,
  CONJUNCTION_OR,

  COLUMN_VALUE,

  VALUE_CONSTANT,
  VALUE_PARAMETER,
  VALUE_TUPLE,
  VALUE_TUPLE_ADDRESS,
  VALUE_NULL,
  VALUE_VECTOR,
  VALUE_SCALAR,
  VALUE_DEFAULT,

  AGGREGATE_COUNT,
  AGGREGATE_SUM,
  AGGREGATE_MIN,
  AGGREGATE_MAX,
  AGGREGATE_AVG,

  FUNCTION,
  BUILTIN_FUNCTION,

  HASH_RANGE,

  OPERATOR_CASE_EXPR,
  OPERATOR_NULL_IF,
  OPERATOR_COALESCE,

  ROW_SUBQUERY,

  STAR,
  PLACEHOLDER,
  COLUMN_REF,
  FUNCTION_REF,
  TABLE_REF
};

/**
 * @return True if the given expression type is a comparison expression; false otherwise.
 */
static inline bool IsComparisonExpression(ExpressionType type) {
  return type <= planner::ExpressionType::COMPARE_IS_DISTINCT_FROM &&
         type >= planner::ExpressionType::COMPARE_EQUAL;
}

/**
 * @return True if the given expression type is an arithmetic expression; false otherwise.
 */
static inline bool IsArithmeticExpression(ExpressionType type) {
  return type <= planner::ExpressionType::OPERATOR_MOD &&
         type >= planner::ExpressionType::OPERATOR_PLUS;
}

/**
 * @return True if the given expression type is a column-reference expression; false otherwise.
 */
static inline bool IsColumnRefExpression(ExpressionType type) {
  return type == ExpressionType::COLUMN_VALUE;
}

/**
 * @return True if the given expression type is a constant value expression; false otherwise.
 */
static inline bool IsConstantExpression(ExpressionType type) {
  return type == ExpressionType::VALUE_CONSTANT;
}

/**
 * Convert an expression type into a string representation. When short_str is true, return a short
 * version of ExpressionType string. For example, "+" instead of "Operator_Plus".
 * @param type The expression type.
 * @param short_str If a terse version of the expression type should be returned.
 * @return A representation of the expression type.
 */
std::string ExpressionTypeToString(ExpressionType type, bool short_str);

}  // namespace tpl::sql::planner
