#pragma once

#include <memory>

#include "util/region.h"
#include "util/region_containers.h"

namespace tpl::sql {

class VectorProjectionIterator;

// ---------------------------------------------------------
// Filter Manager
// ---------------------------------------------------------

/**
 * Base class for all filter managers. A filter manager is composed of a list of
 * disjunctive filters. Each filter is composed of a set of conjunctive clauses.
 * Each clause comes with a set of flavors that implement the filter.
 *
 * Subclasses customize @em RunFilters() over an input vector projection.
 */
class FilterManager {
 public:
  /**
   * A generic filtering function over an input vector projection. Returns the
   * number of tuples that pass the filter.
   */
  using MatchFn = u32 (*)(VectorProjectionIterator *);

  /**
   * A clause in a multi-clause filter. Clauses come in multiple flavors.
   * Flavors are logically equivalent, but may differ in implementation, and
   * thus, exhibit different runtimes.
   */
  struct Clause {
    util::RegionVector<MatchFn> flavors;
    explicit Clause(util::Region *region) : flavors(region) {}
  };

  /**
   * Virtual destructor for subclasses
   */
  virtual ~FilterManager() = default;

  /**
   * Run the filter over all rows in the given input vector projection @em vpi.
   * @param vpi The input vector
   */
  virtual void RunFilters(VectorProjectionIterator *vpi) = 0;

 protected:
  // Construct a filter manager using the given input filters.
  explicit FilterManager(util::RegionVector<Clause> &&clauses);

  // Const-reference access to filters for subclasses
  const util::RegionVector<Clause> &clauses() const { return clauses_; }

 private:
  // The clauses in the filter
  util::RegionVector<Clause> clauses_;
};

// ---------------------------------------------------------
// Simple Filter Manager
// ---------------------------------------------------------

/**
 * A simple non-adaptive filter manager. This manage doesn't consider different
 * filter flavors, but rather just sticks to one.
 */
class SimpleFilterManager : public FilterManager {
 public:
  /**
   * Run the filters over the given vector projection @em vpi
   * @param vpi The input vector
   */
  void RunFilters(VectorProjectionIterator *vpi) override;

 private:
  friend class FilterManagerBuilder;
  // Private on purpose, use FilterManagerBuilder
  explicit SimpleFilterManager(util::RegionVector<Clause> &&clauses);
};

// ---------------------------------------------------------
// Adaptive Filter Manager
// ---------------------------------------------------------

/**
 * An adaptive filter manager that tries to discover the optimal filter
 * configuration.
 */
class AdaptiveFilterManager : public FilterManager {
 public:
  /**
   * Run the filters over the given vector projection @em vpi
   * @param vpi The input vector
   */
  void RunFilters(VectorProjectionIterator *vpi) override;

 private:
  friend class FilterManagerBuilder;
  // Private on purpose, use FilterManagerBuilder
  explicit AdaptiveFilterManager(util::Region *region,
                                 util::RegionVector<Clause> &&_clauses);

 private:
  // The optimal order to execute the clauses
  util::RegionVector<u32> optimal_clause_order_;
  // The index of the optimal flavor for each clause
  util::RegionVector<u32> optimal_flavor_;
};

// ---------------------------------------------------------
// Filter Manager Builder
// ---------------------------------------------------------

/**
 * Builders for FilterManagers. We need these builders to ensure that
 * FilterManagers are immutable after they've been constructed.
 */
class FilterManagerBuilder {
 public:
  explicit FilterManagerBuilder(util::Region *region);

  /**
   * Start a new clause.
   */
  void StartNewClause();

  /**
   * Insert a flavor for the current clause in the filter
   * @param flavor A filter flavor
   */
  void InsertClauseFlavor(FilterManager::MatchFn flavor);

  /**
   * Build a filter manager
   * @param adaptive Whether to build an adaptive filter or not
   * @return The constructed filter manager
   */
  std::unique_ptr<FilterManager> Build(bool adaptive = true);

 private:
  FilterManager::Clause &curr_clause() {
    TPL_ASSERT(!clauses_.empty(), "Missing call to StartNewClause()");
    return clauses_.back();
  }

 private:
  // Region
  util::Region *region_;

  // The filters
  util::RegionVector<FilterManager::Clause> clauses_;

  // Has this builder been finalized?
  bool finalized_;
};

}  // namespace tpl::sql
