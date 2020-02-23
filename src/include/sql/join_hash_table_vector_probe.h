#pragma once

#include <vector>

#include "common/common.h"
#include "sql/planner/plannodes/plan_node_defs.h"
#include "sql/tuple_id_list.h"
#include "sql/vector.h"

namespace tpl::sql {

class JoinHashTable;
class VectorProjection;

/**
 * Structure to capture a vector probe.
 */
class JoinHashTableVectorProbe {
 public:
  /**
   * Create a new probe structure.
   * @param table The hash table to probe.
   * @param join_type The type of join to perform.
   * @param join_key_indexes The indexes of the join keys in the input projection.
   */
  JoinHashTableVectorProbe(const JoinHashTable &table, planner::LogicalJoinType join_type,
                           std::vector<uint32_t> join_key_indexes);

  /**
   * Prepare a probe using the given probe keys.
   * @param input The probe keys.
   */
  void Init(VectorProjection *input);

  /**
   * Advance to the next set of matches for the input keys.
   */
  bool Next(VectorProjection *input);

  /**
   * @return The current set of matches.
   */
  const Vector *GetMatches();

  /**
   * Reset this probe to the state immediately after initialization. This enables re-iterating the
   * results of the probe for the same input batch.
   */
  void Reset();

 private:
  // Next operator for an inner join.
  bool NextInnerJoin(VectorProjection *input);
  // Next operator for a semi join.
  bool NextSemiJoin(VectorProjection *input);
  // Next operator for an anti join.
  bool NextAntiJoin(VectorProjection *input);
  // Next operator for a right outer join.
  bool NextRightJoin(VectorProjection *input);

  // Follow the chain for all non-null entries in 'matches'
  void FollowChainNext();

  // Given the input keys, check their equality to the current set of matches.
  void CheckKeyEquality(VectorProjection *input);

  // Common logic for semi and anti joins.
  template <bool Match>
  bool NextSemiOrAntiJoin(VectorProjection *input);

 private:
  // The join table.
  const JoinHashTable &table_;
  // The join type.
  const planner::LogicalJoinType join_type_;
  // The indexes of the join keys in the input.
  const std::vector<uint32_t> join_key_indexes_;
  // The list of TIDs that have valid key matches in the matches vector.
  TupleIdList key_matches_;
  TupleIdList semi_anti_key_matches_;
  // The list of TIDs that have non-null entries in the matches vector.
  TupleIdList non_null_entries_;
  // The vector of current matches.
  Vector matches_;
  // First 'next' call?
  bool first_;
};

}  // namespace tpl::sql
