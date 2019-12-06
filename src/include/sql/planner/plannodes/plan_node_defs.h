#pragma once

#include <utility>
#include <vector>

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

//===--------------------------------------------------------------------===//
// Aggregate Stragegies
//===--------------------------------------------------------------------===//
enum class AggregateStrategyType {
  INVALID = INVALID_TYPE_ID,
  SORTED = 1,
  HASH = 2,
  PLAIN = 3  // no group-by
};

//===--------------------------------------------------------------------===//
// Order by Orderings
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
  SEMI = 5                    // IN+Subquery is SEMI
};

}  // namespace tpl::sql::planner
