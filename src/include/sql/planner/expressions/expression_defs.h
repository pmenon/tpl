#pragma once

#include <string>

#include "common/macros.h"

namespace tpl::sql::planner {

/**
 * All possible comparison types.
 */
enum class ComparisonKind : uint8_t {
  EQUAL,
  NOT_EQUAL,
  LESS_THAN,
  GREATER_THAN,
  LESS_THAN_OR_EQUAL_TO,
  GREATER_THAN_OR_EQUAL_TO,
  LIKE,
  NOT_LIKE,
  IN,
  BETWEEN,
};

/**
 * All possible aggregates.
 */
enum class AggregateKind : uint8_t {
  COUNT,
  COUNT_STAR,
  SUM,
  MIN,
  MAX,
  AVG,
};

/**
 * All possible conjunctions ... just two.
 */
enum class ConjunctionKind : uint8_t { AND, OR };

/**
 * All possible expression types.
 */
enum class ExpressionType : uint8_t {
  // Specialized.
  AGGREGATE,
  CASE,
  CAST,
  COLUMN_VALUE,
  COMPARISON,
  CONJUNCTION,
  CONSTANT,
  DERIVED_VALUE,
  // Generic (builtins).
  UNARY_OPERATOR,
  BINARY_OPERATOR,
  NARY_OPERATOR,
};

std::string ComparisonKindToString(ComparisonKind kind, bool short_str);

std::string AggregateKindToString(AggregateKind kind);

std::string ConjunctionKindToString(ConjunctionKind kind);

std::string ExpressionTypeToString(ExpressionType type);

}  // namespace tpl::sql::planner
