#pragma once

#include <string>

namespace tpl::sql::planner {

constexpr int INVALID_TYPE_ID = 0;

//===--------------------------------------------------------------------===//
// Plan Node Types
//===--------------------------------------------------------------------===//

enum class PlanNodeType {
  INVALID = INVALID_TYPE_ID,

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

  // Test
  MOCK
};

/**
 * @return A string representation for the provided node type.
 */
std::string PlanNodeTypeToString(PlanNodeType type);

//===--------------------------------------------------------------------===//
// Aggregate Strategies
//===--------------------------------------------------------------------===//

enum class AggregateStrategyType {
  INVALID = INVALID_TYPE_ID,
  SORTED = 1,
  HASH = 2,
  PLAIN = 3  // no group-by
};

//===--------------------------------------------------------------------===//
// Order-by Orderings
//===--------------------------------------------------------------------===//

enum class OrderByOrderingType { ASC, DESC };

//===--------------------------------------------------------------------===//
// Logical Join Types
//===--------------------------------------------------------------------===//

enum class LogicalJoinType {
  INVALID = INVALID_TYPE_ID,  // invalid join type
  LEFT = 1,                   // left
  RIGHT = 2,                  // right
  INNER = 3,                  // inner
  OUTER = 4,                  // outer
  SEMI = 5,                   // returns a row ONLY if it has a join partner, no duplicates
  ANTI = 6                    // returns a row ONLY if it has NO join partner, no duplicates
};

/**
 * @return A string representation for the provided join type.
 */
std::string JoinTypeToString(LogicalJoinType type);

//===--------------------------------------------------------------------===//
// Set Operation Types
//===--------------------------------------------------------------------===//

enum class SetOpType {
  INVALID = INVALID_TYPE_ID,
  INTERSECT = 1,
  INTERSECT_ALL = 2,
  EXCEPT = 3,
  EXCEPT_ALL = 4
};

//===--------------------------------------------------------------------===//
// CSV scan defaults
//===--------------------------------------------------------------------===//

static constexpr char kDefaultCsvDelimiterChar = ',';
static constexpr char kDefaultCsvQuoteChar = '"';
static constexpr char kDefaultCsvEscapeChar = '"';
static constexpr const char *kDefaultCsvNullString = "";

}  // namespace tpl::sql::planner
