#include "sql/planner/expressions/expression_defs.h"

#include <string>

namespace tpl::sql::planner {

std::string ComparisonKindToString(ComparisonKind kind, bool short_str) {
  // clang-format off
  switch (kind) {
    case ComparisonKind::EQUAL:                    return short_str ? "==" : "EQUAL";
    case ComparisonKind::NOT_EQUAL:                return short_str ? "!=" : "NOT EQUAL";
    case ComparisonKind::LESS_THAN:                return short_str ? "<" : "LESS THAN";
    case ComparisonKind::GREATER_THAN:             return short_str ? ">" : "NOT EQUAL";
    case ComparisonKind::LESS_THAN_OR_EQUAL_TO:    return short_str ? "<=" : "NOT EQUAL";
    case ComparisonKind::GREATER_THAN_OR_EQUAL_TO: return short_str ? ">=" : "NOT EQUAL";
    case ComparisonKind::LIKE:                     return "LIKE";
    case ComparisonKind::NOT_LIKE:                 return "NOT LIKE";
    case ComparisonKind::IN:                       return "IN";
    case ComparisonKind::BETWEEN:                  return "BETWEEN";
  }
  // clang-format on
  UNREACHABLE("Impossible to reach. All comparison kinds handled.");
}

std::string AggregateKindToString(AggregateKind kind) {
  // clang-format off
  switch (kind) {
    case AggregateKind::COUNT:      return "COUNT";
    case AggregateKind::COUNT_STAR: return "COUNT-STAR";
    case AggregateKind::SUM:        return "SUM";
    case AggregateKind::MIN:        return "MIN";
    case AggregateKind::MAX:        return "MAX";
    case AggregateKind::AVG:        return "AVG";
  }
  // clang-format on
  UNREACHABLE("Impossible to reach. All aggregate kinds handled.");
}

std::string ConjunctionKindToString(ConjunctionKind kind) {
  // clang-format off
  switch (kind) {
    case ConjunctionKind::AND: return "AND";
    case ConjunctionKind::OR:  return "OR";
  }
  // clang-format on
  UNREACHABLE("Impossible to reach. All conjunction kinds handled.");
}

std::string ExpressionTypeToString(ExpressionType type) {
  // clang-format off
  switch (type) {
    case ExpressionType::AGGREGATE:       return "AGGREGATE";
    case ExpressionType::CASE:            return "CASE";
    case ExpressionType::CAST:            return "CAST";
    case ExpressionType::COLUMN_VALUE:    return "CVE";
    case ExpressionType::COMPARISON:      return "COMPARISON";
    case ExpressionType::CONJUNCTION:     return "CONJUNCTION";
    case ExpressionType::CONSTANT:        return "CONSTANT";
    case ExpressionType::DERIVED_VALUE:   return "DVE";
    case ExpressionType::UNARY_OPERATOR:  return "UNARY-OPERATOR";
    case ExpressionType::BINARY_OPERATOR: return "BINARY-OPERATOR";
    case ExpressionType::NARY_OPERATOR:   return "NARY-OPERATOR";
  }
  // clang-format on
  UNREACHABLE("Impossible to reach. All expression types handled.");
}

}  // namespace tpl::sql::planner
