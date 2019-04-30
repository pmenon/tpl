#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "bandit/policy.h"
#include "util/common.h"
#include "util/macros.h"

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
    std::vector<MatchFn> flavors;
    u32 num_flavors() const { return flavors.size(); }
  };

  /**
   * Virtual destructor for subclasses
   */
  virtual ~FilterManager() = default;

  /**
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(FilterManager);

  /**
   * Run the filter over all rows in the given input vector projection @em vpi.
   * @param vpi The input vector
   */
  virtual void RunFilters(VectorProjectionIterator *vpi) = 0;

 protected:
  // Construct a filter manager using the given input filters.
  explicit FilterManager(std::vector<Clause> &&clauses);

  // Const-reference access to filters for subclasses
  const std::vector<Clause> &clauses() const { return clauses_; }

  // The number of clauses
  u32 num_clauses() const { return clauses_.size(); }

  // Return the clause at the given index in the filter
  const Clause *ClauseAt(u32 index) const { return &clauses_[index]; }

 private:
  // The clauses in the filter
  std::vector<Clause> clauses_;
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
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(SimpleFilterManager);

  /**
   * Run the filters over the given vector projection @em vpi
   * @param vpi The input vector
   */
  void RunFilters(VectorProjectionIterator *vpi) override;

 private:
  friend class FilterManagerBuilder;
  // Private on purpose, use FilterManagerBuilder
  explicit SimpleFilterManager(std::vector<Clause> &&clauses);
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
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(AdaptiveFilterManager);

  /**
   * Run the filters over the given vector projection @em vpi
   * @param vpi The input vector
   */
  void RunFilters(VectorProjectionIterator *vpi) override;

  /**
   * Return the index of the current optimal implementation flavor for the
   * clause at index @em clause_index
   * @param clause_index The index of the clause
   * @return The index of the optimal flavor
   */
  u32 GetOptimalFlavorForClause(u32 clause_index) const;

 private:
  friend class FilterManagerBuilder;

  // Private on purpose, use FilterManagerBuilder
  explicit AdaptiveFilterManager(std::vector<Clause> &&_clauses,
                                 std::unique_ptr<bandit::Policy> policy);

  // Run a specific clause of the filter
  void RunFilterClause(VectorProjectionIterator *vpi, u32 clause_index);

  // Run the given matching function
  std::pair<u32, double> RunFilterClauseImpl(VectorProjectionIterator *vpi,
                                             FilterManager::MatchFn func);

  // Return the agent handling the clause at the given index
  bandit::Agent *GetAgentFor(u32 clause_index);
  const bandit::Agent *GetAgentFor(u32 clause_index) const;

 private:
  // The optimal order to execute the clauses
  std::vector<u32> optimal_clause_order_;
  // The adaptive policy to use
  std::unique_ptr<bandit::Policy> policy_;
  // The agents, one per clause
  std::vector<bandit::Agent> agents_;
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
  FilterManagerBuilder();

  /**
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(FilterManagerBuilder);

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
   * Build a simple filter manager using the configured filter clauses
   * @return The constructed filter manager
   */
  std::unique_ptr<FilterManager> BuildSimple();

  /**
   * Build an adaptive filter manager using the configured filter clauses using
   * the adaptive policy @em policy
   * @param policy_kind The kind of adaptivity to use
   * @return The constructed filter manager
   */
  std::unique_ptr<FilterManager> BuildAdaptive(
      bandit::Policy::Kind policy_kind = bandit::Policy::EpsilonGreedy);

 private:
  FilterManager::Clause &curr_clause() {
    TPL_ASSERT(!clauses_.empty(), "Missing call to StartNewClause()");
    return clauses_.back();
  }

 private:
  // The filters
  std::vector<FilterManager::Clause> clauses_;

  // Has this builder been finalized?
  bool finalized_;
};

}  // namespace tpl::sql
