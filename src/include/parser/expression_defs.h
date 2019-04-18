#pragma once

#include "util/macros.h"

namespace tpl::parser {

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

  VALUE_CONSTANT,
  VALUE_PARAMETER,
  VALUE_TUPLE,
  VALUE_TUPLE_ADDRESS,
  VALUE_NULL,
  VALUE_VECTOR,
  VALUE_SCALAR,

  AGGREGATE_COUNT,
  AGGREGATE_COUNT_STAR,
  AGGREGATE_SUM,
  AGGREGATE_MIN,
  AGGREGATE_MAX,
  AGGREGATE_AVG,

  FUNCTION,

  HASH_RANGE,

  OPERATOR_CASE_EXPR,
  OPERATOR_NULL_IF,
  OPERATOR_COALESCE,

  ROW_SUBQUERY,
  SELECT_SUBQUERY,

  STAR,
  PLACEHOLDER,
  COLUMN_REF,
  FUNCTION_REF,
  TABLE_REF
};

}  // namespace terrier::parser
