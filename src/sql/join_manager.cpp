#include "sql/join_manager.h"

#include "logging/logger.h"
#include "sql/join_hash_table.h"
#include "sql/join_hash_table_vector_probe.h"
#include "sql/planner/plannodes/plan_node_defs.h"
#include "sql/vector_projection.h"
#include "sql/vector_projection_iterator.h"

namespace tpl::sql {

JoinManager::JoinManager(void *opaque_context)
    : filter_(true, opaque_context),
      input_tid_list_(kDefaultVectorSize),
      curr_vpi_(nullptr),
      first_join_(true) {
  filter_.StartNewClause();
}

// Needed because we forward-declare JHTVP.
JoinManager::~JoinManager() = default;

void JoinManager::InsertJoinStep(const JoinHashTable &table, const std::vector<uint32_t> &key_cols,
                                 FilterManager::MatchFn match_fn) {
  // Create state for this join step.
  const auto join_type = planner::LogicalJoinType::INNER;
  probes_.emplace_back(std::make_unique<JoinHashTableVectorProbe>(table, join_type, key_cols));

  // Make a filtering step.
  filter_.InsertClauseTerm(match_fn);
}

// Called during a single filter-join step.
void JoinManager::PrepareSingleJoin(VectorProjection *input_batch, TupleIdList *tid_list,
                                    const uint32_t step_idx) {
  TPL_ASSERT(step_idx < probes_.size(), "Out-of-bounds join index access");

  // Filter the input first according to the current filter.
  input_batch->SetFilteredSelections(*tid_list);

  // Initialize the probe for current input batch.
  probes_[step_idx]->Init(input_batch);

  // Filter out NULL entries.
  tid_list->IntersectWith(*probes_[step_idx]->GetMatchList());
}

void JoinManager::SetInputBatch(VectorProjectionIterator *input_vpi) {
  curr_vpi_ = input_vpi;

  // Resize the input list, if need be.
  if (TPL_UNLIKELY(input_tid_list_.GetCapacity() != curr_vpi_->GetTotalTupleCount())) {
    input_tid_list_.Resize(curr_vpi_->GetTotalTupleCount());
  }

  // Apply initial join filter.
  filter_.RunFilters(curr_vpi_);

  // Save the input list.
  curr_vpi_->GetVectorProjection()->CopySelectionsTo(&input_tid_list_);

  // Set first join to trigger an initial join.
  first_join_ = true;
}

bool JoinManager::AdvanceInitial(const uint32_t idx) {
  auto input_batch = curr_vpi_->GetVectorProjection();

  // The match list from the outer probe and this probe.
  auto outer_match_list = idx == 0 ? &input_tid_list_ : probes_[idx - 1]->GetMatchList();
  auto match_list = probes_[idx]->GetMatchList();

  while (probes_[idx]->Next(input_batch)) {
    input_batch->SetFilteredSelections(*match_list);
    if (idx == probes_.size() - 1 || AdvanceInitial(idx + 1)) {
      return true;
    }
    input_batch->SetFilteredSelections(*outer_match_list);
  }

  // Reset in case we need to retry this step.
  if (idx != 0) probes_[idx]->Reset();

  // We didn't find anything.
  return false;
}

bool JoinManager::Advance(const uint32_t idx) {
  auto input_batch = curr_vpi_->GetVectorProjection();

  // The match list from the outer probe and this probe.
  auto outer_match_list = idx == 0 ? &input_tid_list_ : probes_[idx - 1]->GetMatchList();
  auto match_list = probes_[idx]->GetMatchList();

  while (true) {
    input_batch->SetFilteredSelections(*outer_match_list);
    if (probes_[idx]->Next(input_batch)) {
      input_batch->SetFilteredSelections(*match_list);
      return true;
    }

    if (idx == 0 || !Advance(idx - 1)) {
      return false;
    }

    probes_[idx]->Reset();
  }
}

bool JoinManager::Next() {
  TPL_ASSERT(curr_vpi_ != nullptr, "No input batch! Did you forget to call SetInputBatch()?");
  // Reset the TID list for this round.
  auto input_batch = curr_vpi_->GetVectorProjection();

  // Attempt to advance.
  bool advanced = first_join_ ? AdvanceInitial(0) : Advance(probes_.size() - 1);
  first_join_ = false;

  // If we've advanced, apply the filter.
  if (advanced) curr_vpi_->SetVectorProjection(input_batch);

  // Done.
  return advanced;
}

void JoinManager::GetOutputBatch(const HashTableEntry **matches[]) {
  for (uint32_t i = 0; i < probes_.size(); i++) {
    matches[i] = reinterpret_cast<const HashTableEntry **>(probes_[i]->GetMatches()->GetData());
  }
}

}  // namespace tpl::sql
