#include "sql/filter_manager.h"

#include <memory>
#include <numeric>

#include "ips4o/ips4o.hpp"

#include "common/settings.h"
#include "sql/vector_projection_iterator.h"
#include "util/timer.h"

namespace tpl::sql {

//===----------------------------------------------------------------------===//
//
// Filter Manager Clause
//
//===----------------------------------------------------------------------===//

FilterManager::Clause::Clause(const float stat_sample_freq)
    : sample_freq_(stat_sample_freq),
#ifndef NDEBUG
      // In DEBUG mode, use a fixed seed so we get predictable and repeatable randomness
      gen_(0),
#else
      gen_(std::random_device()()),
#endif
      dist_(0, 1) {
}

void FilterManager::Clause::Finalize() {
  // The initial "best" ordering of terms is the order they were inserted into the filter manager,
  // which also happens to be, presumably, the order the optimizer provided in the physical plan.
  // Let's stick with it now and revisit during runtime.
  optimal_term_order_.resize(terms_.size());
  std::iota(optimal_term_order_.begin(), optimal_term_order_.end(), 0);
}

bool FilterManager::Clause::ShouldReRank() { return dist_(gen_) < sample_freq_; }

void FilterManager::Clause::RunFilter(VectorProjection *vector_projection, TupleIdList *tid_list) {
  // With probability 'sample_freq_' we will collect statistics on each clause
  // term and re-rank them to form a potentially new, more optimal ordering.
  // The rank of a term is defined as:
  //
  //   rank = (1 - selectivity) / cost
  //
  // We use the elapsed time as a proxy for a term's cost. The below algorithm
  // uses the difference of selectivities to estimate the selectivity of a term.
  // This, of course, requires the assumption that terms are independent, which
  // is false. But, the point is that we can use this infrastructure to
  // implement any policy.
  // TODO(pmenon): Implement Babu et. al's adaptive policy

  if (!ShouldReRank()) {
    for (const auto &term_idx : optimal_term_order_) {
      terms_[term_idx].fn(vector_projection, tid_list);
    }
    return;
  }

  float selectivity = tid_list->ComputeSelectivity();
  for (const auto &index : optimal_term_order_) {
    util::Timer<std::nano> timer;
    timer.Start();

    auto &term = terms_[index];
    term.fn(vector_projection, tid_list);

    timer.Stop();

    const float new_selectivity = tid_list->ComputeSelectivity();
    term.rank = (1.0f - (selectivity - new_selectivity)) / timer.GetElapsed();
    selectivity = new_selectivity;
  }

  // Re-rank
  ips4o::sort(
      optimal_term_order_.begin(), optimal_term_order_.end(),
      [&](const auto idx1, const auto idx2) { return terms_[idx1].rank < terms_[idx2].rank; });
}

//===----------------------------------------------------------------------===//
//
// Filter Manager
//
//===----------------------------------------------------------------------===//

FilterManager::FilterManager()
    : input_list_(kDefaultVectorSize),
      tmp_list_(kDefaultVectorSize),
      output_list_(kDefaultVectorSize),
      finalized_(false) {}

FilterManager::~FilterManager() = default;

void FilterManager::StartNewClause() {
  TPL_ASSERT(!finalized_, "Cannot modify filter manager after finalization");
  const auto sample_freq =
      Settings::Instance()->GetDouble(Settings::Name::AdaptivePredicateOrderSamplingFrequency);
  clauses_.emplace_back(static_cast<float>(sample_freq));
}

void FilterManager::InsertClauseTerm(const FilterManager::MatchFn term) {
  TPL_ASSERT(!finalized_, "Cannot modify filter manager after finalization");
  TPL_ASSERT(!clauses_.empty(), "Inserting flavor without clause");
  clauses_.back().AddTerm(term);
}

void FilterManager::InsertClauseTerms(std::initializer_list<MatchFn> terms) {
  std::for_each(terms.begin(), terms.end(), [this](auto &term) { InsertClauseTerm(term); });
}

void FilterManager::Finalize() {
  if (IsFinalized()) {
    return;
  }

  // Initialize optimal orderings, initially in the order they appear
  optimal_clause_order_.resize(clauses_.size());
  std::iota(optimal_clause_order_.begin(), optimal_clause_order_.end(), 0);

  // Finalize each clause
  std::for_each(clauses_.begin(), clauses_.end(), [](auto &clause) { clause.Finalize(); });

  finalized_ = true;
}

void FilterManager::RunFilters(VectorProjection *vector_projection) {
  TPL_ASSERT(IsFinalized(), "Must finalize the filter before it can be used");

  // Initialize the input, output, and temporary TID lists for processing this projection
  if (const uint32_t projection_size = vector_projection->GetTotalTupleCount();
      projection_size != input_list_.GetCapacity()) {
    tmp_list_.Resize(projection_size);
    input_list_.Resize(projection_size);
    output_list_.Resize(projection_size);
  }

  if (auto *sel_vector = vector_projection->GetSelectionVector(); sel_vector != nullptr) {
    input_list_.BuildFromSelectionVector(sel_vector, vector_projection->GetSelectedTupleCount());
  } else {
    input_list_.AddAll();
  }
  output_list_.Clear();

  // Run through all summands in the order we believe to be optimal
  for (const uint32_t clause_index : optimal_clause_order_) {
    tmp_list_.AssignFrom(input_list_);
    tmp_list_.UnsetFrom(output_list_);

    // Quit
    if (tmp_list_.IsEmpty()) {
      break;
    }

    // Run the clause
    clauses_[clause_index].RunFilter(vector_projection, &tmp_list_);

    // Update output list with surviving TIDs
    output_list_.UnionWith(tmp_list_);
  }

  // Finish
  vector_projection->SetSelections(output_list_);
}

void FilterManager::RunFilters(VectorProjectionIterator *vpi) {
  VectorProjection *vector_projection = vpi->GetVectorProjection();
  RunFilters(vector_projection);
  vpi->Reset();
}

}  // namespace tpl::sql
