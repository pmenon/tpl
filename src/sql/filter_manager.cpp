#include "sql/filter_manager.h"

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
      // In DEBUG mode, use a fixed seed so we get repeatable randomness
      gen_(0),
#else
      gen_(std::random_device()()),
#endif
      dist_(0, 1) {
}

void FilterManager::Clause::Finalize() {
  // The initial "best" ordering of terms is the order they were inserted into
  // the filter manager, which also happens to be, presumably, the order the
  // optimizer provided in the physical plan. Let's stick with it now and
  // revisit during runtime.
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

  double selectivity = tid_list->ComputeSelectivity();
  for (const auto term_idx : optimal_term_order_) {
    Clause::Term &term = terms_[term_idx];
    const double exec_ns = util::Time<std::nano>([&] { term.fn(vector_projection, tid_list); });
    const double new_selectivity = tid_list->ComputeSelectivity();
    term.rank = (1.0f - (selectivity - new_selectivity)) / exec_ns;
    selectivity = new_selectivity;
  }

  // Re-rank
  ips4o::sort(optimal_term_order_.begin(), optimal_term_order_.end(),
              [&](const auto term_idx1, const auto term_idx2) {
                return terms_[term_idx1].rank < terms_[term_idx2].rank;
              });
}

//===----------------------------------------------------------------------===//
//
// Filter Manager
//
//===----------------------------------------------------------------------===//

FilterManager::FilterManager()
    : input_list_(kDefaultVectorSize),
      output_list_(kDefaultVectorSize),
      tmp_list_(kDefaultVectorSize),
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
  for (auto &term : terms) {
    InsertClauseTerm(term);
  }
}

void FilterManager::Finalize() {
  if (IsFinalized()) {
    return;
  }

  // Initialize optimal orderings, initially in the order they appear
  optimal_clause_order_.resize(clauses_.size());
  std::iota(optimal_clause_order_.begin(), optimal_clause_order_.end(), 0);

  // Finalize each clause
  for (auto &clause : clauses_) {
    clause.Finalize();
  }

  finalized_ = true;
}

void FilterManager::RunFilters(VectorProjection *vector_projection) {
  TPL_ASSERT(IsFinalized(), "Must finalize the filter before it can be used");

  // Initialize the input, output, and temporary tuple ID lists for processing
  // this projection. This check just ensures they're all the same shape.
  if (const uint32_t projection_size = vector_projection->GetTotalTupleCount();
      projection_size != input_list_.GetCapacity()) {
    input_list_.Resize(projection_size);
    output_list_.Resize(projection_size);
    tmp_list_.Resize(projection_size);
  }

  // Copy the input list from the input vector projection.
  if (vector_projection->IsFiltered()) {
    const auto *filter = vector_projection->GetFilteredTupleIdList();
    TPL_ASSERT(filter != nullptr, "No TID list filter for filtered projection");
    input_list_.AssignFrom(*filter);
  } else {
    input_list_.AddAll();
  }

  // The output list is initially empty: no tuples pass the filter. This list is
  // incrementally built up.
  output_list_.Clear();

  // Run through all summands in the order we believe to be optimal.
  for (const uint32_t clause_index : optimal_clause_order_) {
    // The set of TIDs that we need to check is everything in the input that
    // hasn't yet passed any previous clause.
    tmp_list_.AssignFrom(input_list_);
    tmp_list_.UnsetFrom(output_list_);

    // Quit.
    if (tmp_list_.IsEmpty()) {
      break;
    }

    // Run the clause.
    clauses_[clause_index].RunFilter(vector_projection, &tmp_list_);

    // Update output list with surviving TIDs.
    output_list_.UnionWith(tmp_list_);
  }

  vector_projection->SetFilteredSelections(output_list_);
}

void FilterManager::RunFilters(VectorProjectionIterator *vpi) {
  VectorProjection *vector_projection = vpi->GetVectorProjection();
  RunFilters(vector_projection);
  vpi->SetVectorProjection(vector_projection);
}

}  // namespace tpl::sql
