#pragma once

#include <string>

namespace tpl::sql::planner {

//===--------------------------------------------------------------------===//
// Plan Node Types
//===--------------------------------------------------------------------===//

enum class PlanNodeType : uint8_t {
  // Scan Nodes
  SEQSCAN,
  INDEXSCAN,
  CSVSCAN,

  // Join Nodes
  NESTLOOP,
  HASHJOIN,
  INDEXNLJOIN,

  // Mutator Nodes
  UPDATE,
  INSERT,
  DELETE,

  // DDL Nodes
  CREATE_DATABASE,
  CREATE_NAMESPACE,
  CREATE_TABLE,
  CREATE_INDEX,
  CREATE_FUNC,
  CREATE_TRIGGER,
  CREATE_VIEW,
  DROP_DATABASE,
  DROP_NAMESPACE,
  DROP_TABLE,
  DROP_INDEX,
  DROP_TRIGGER,
  DROP_VIEW,
  ANALYZE,

  // Algebra Nodes
  AGGREGATE,
  ORDERBY,
  PROJECTION,
  LIMIT,
  DISTINCT,
  HASH,
  SETOP,

  // Utility
  EXPORT_EXTERNAL_FILE,
  RESULT,
};

/**
 * @return A string representation for the provided node type.
 */
std::string PlanNodeTypeToString(PlanNodeType type);

//===--------------------------------------------------------------------===//
// Aggregate Strategies
//===--------------------------------------------------------------------===//

enum class AggregateStrategyType : uint8_t {
  SORTED,
  HASH,
  PLAIN,  // no group-by
};

//===--------------------------------------------------------------------===//
// Order-by Orderings
//===--------------------------------------------------------------------===//

enum class OrderByOrderingType : uint8_t { ASC, DESC };

//===--------------------------------------------------------------------===//
// Logical Join Types
//===--------------------------------------------------------------------===//

enum class LogicalJoinType : uint8_t {
  LEFT,        // left
  RIGHT,       // right
  INNER,       // inner
  OUTER,       // outer
  SEMI,        // returns a row ONLY if it has a join partner, no duplicates
  ANTI,        // returns a row ONLY if it has NO join partner, no duplicates
  LEFT_SEMI,   // Left semi join
  RIGHT_SEMI,  // Right semi join
  RIGHT_ANTI   // Right anti join
};

/**
 * @return A string representation for the provided join type.
 */
std::string JoinTypeToString(LogicalJoinType type);

//===--------------------------------------------------------------------===//
// Set Operation Types
//===--------------------------------------------------------------------===//

enum class SetOpType : uint8_t { INTERSECT, INTERSECT_ALL, EXCEPT, EXCEPT_ALL, UNION, UNION_ALL };

//===--------------------------------------------------------------------===//
// CSV scan defaults
//===--------------------------------------------------------------------===//

static constexpr char kDefaultCsvDelimiterChar = ',';
static constexpr char kDefaultCsvQuoteChar = '"';
static constexpr char kDefaultCsvEscapeChar = '"';
static constexpr const char *kDefaultCsvNullString = "";

}  // namespace tpl::sql::planner
