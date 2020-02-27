#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "common/macros.h"
#include "sql/filter_manager.h"
#include "sql/sql.h"

namespace tpl::sql {

class HashTableEntry;
class JoinHashTable;
class JoinHashTableVectorProbe;
class VectorProjectionIterator;

/**
 * An adaptive join processor that dynamically adapts the ordering of a series of joins in order to
 * find an optimal ordering.
 */
class JoinManager {
 public:
  /**
   * Create a new join manager using the provided opaque context.
   * @param opaque_context An opaque context passed through each join function.
   */
  explicit JoinManager(void *opaque_context);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(JoinManager);

  /**
   * Destructor.
   */
  ~JoinManager();

  /**
   * Insert a join-probe step into the manager. The step will probe the provided join hash table
   * @em probe_table and use the columns indexes in @em key_cols as join keys.
   * @param table The table to probe in this step.
   * @param key_cols The indexes of the columns in the
   * @param match_fn The join function.
   */
  void InsertJoinStep(const JoinHashTable &table, const std::vector<uint32_t> &key_cols,
                      FilterManager::MatchFn match_fn);

  /**
   * Set the next set of input into the join.
   * @param input_vpi The next input batch into the join.
   */
  void SetInputBatch(VectorProjectionIterator *input_vpi);

  /**
   * Attempt to advance
   * @return True if there is more output from this fancy-ass multi-step join for the current input
   *         batch. False otherwise.
   */
  bool Next();

  /**
   * Get the next output from this fancy-ass multi-step join. It's assumed the matches vector is
   * large enough to store match vectors for each step.
   * @param[out] matches The array of match vectors, one for each join step.
   */
  void GetOutputBatch(const HashTableEntry **matches[]);

  /**
   * Perform a single join of the input batch against the provided join hash table.
   * @param input_batch The input into the join.
   * @param tid_list The list of TIDs in the input on which the join should be performed.
   * @param step_idx The index join to perform.
   */
  void PrepareSingleJoin(VectorProjection *input_batch, TupleIdList *tid_list, uint32_t step_idx);

 private:
  // Perform the initial join.
  bool AdvanceInitial(uint32_t idx);
  // Advance the probe at the given index.
  bool Advance(uint32_t idx);

 private:
  // The adaptive filter.
  FilterManager filter_;
  // The probe state for each join.
  std::vector<std::unique_ptr<JoinHashTableVectorProbe>> probes_;
  // Each iteration of the join filters different tuples. This saves the initial
  // set of active TIDs through iterations.
  TupleIdList input_tid_list_;
  // The current input batch.
  VectorProjectionIterator *curr_vpi_;
  // Has the initial join been performed?
  bool first_join_;
};

}  // namespace tpl::sql
