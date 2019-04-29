#include "sql/filter_manager.h"

#include <memory>
#include <numeric>
#include <utility>

#include "sql/vector_projection_iterator.h"

namespace tpl::sql {

// ---------------------------------------------------------
// Filter Manager
// ---------------------------------------------------------

FilterManager::FilterManager(
    util::RegionVector<FilterManager::Clause> &&clauses)
    : clauses_(std::move(clauses)) {}

// ---------------------------------------------------------
// Simple Filter Manager
// ---------------------------------------------------------

SimpleFilterManager::SimpleFilterManager(
    util::RegionVector<FilterManager::Clause> &&clauses)
    : FilterManager(std::move(clauses)) {}

void SimpleFilterManager::RunFilters(VectorProjectionIterator *vpi) {
  for (const auto &clause : clauses()) {
    FilterManager::MatchFn match_func = clause.flavors[0];
    match_func(vpi);
  }
}

// ---------------------------------------------------------
// Adaptive Filter Manager
// ---------------------------------------------------------

AdaptiveFilterManager::AdaptiveFilterManager(
    util::Region *region, util::RegionVector<FilterManager::Clause> &&_clauses)
    : FilterManager(std::move(_clauses)),
      optimal_clause_order_(clauses().size(), region),
      optimal_flavor_(clauses().size(), region) {
  // Initialize optimal orderings
  std::iota(optimal_clause_order_.begin(), optimal_clause_order_.end(), 0);
  std::fill(optimal_flavor_.begin(), optimal_flavor_.end(), 0);
}

void AdaptiveFilterManager::RunFilters(VectorProjectionIterator *vpi) {
  for (u32 idx = 0; idx < clauses().size(); idx++) {
    // The index of the clause to run, and the index of the flavor of the clause
    const u32 opt_clause_idx = optimal_clause_order_[idx];
    const u32 opt_flavor_idx = optimal_flavor_[idx];
    const FilterManager::Clause &opt_clause = clauses()[opt_clause_idx];
    FilterManager::MatchFn opt_match_func = opt_clause.flavors[opt_flavor_idx];

    // Run
    opt_match_func(vpi);
  }
}

// ---------------------------------------------------------
// Filter Manager Builder
// ---------------------------------------------------------

FilterManagerBuilder::FilterManagerBuilder(util::Region *region)
    : region_(region), clauses_(region), finalized_(false) {}

void FilterManagerBuilder::StartNewClause() { clauses_.emplace_back(region_); }

void FilterManagerBuilder::InsertClauseFlavor(FilterManager::MatchFn flavor) {
  curr_clause().flavors.push_back(flavor);
}

std::unique_ptr<FilterManager> FilterManagerBuilder::Build(bool adaptive) {
  if (finalized_) {
    return nullptr;
  }

  std::unique_ptr<FilterManager> result;
  if (adaptive) {
    result = std::unique_ptr<AdaptiveFilterManager>(
        new AdaptiveFilterManager(region_, std::move(clauses_)));
  } else {
    result = std::unique_ptr<SimpleFilterManager>(
        new SimpleFilterManager(std::move(clauses_)));
  }

  finalized_ = true;

  return result;
}

}  // namespace tpl::sql
