#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "bandit/policy.h"
#include "util/common.h"
#include "util/macros.h"

namespace tpl::sql {

class VectorProjectionIterator;

/**
 * An adaptive filter manager that tries to discover the optimal filter
 * configuration.
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
   * Construct the filter using the given adaptive policy
   * @param policy_kind
   */
  explicit FilterManager(
      bandit::Policy::Kind policy_kind = bandit::Policy::EpsilonGreedy);

  /**
   * Destructor
   */
  ~FilterManager();

  /**
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(FilterManager);

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
   * Make the manager immutable.
   */
  void Finalize();

  /**
   * Run the filters over the given vector projection @em vpi
   * @param vpi The input vector
   */
  void RunFilters(VectorProjectionIterator *vpi);

  /**
   * Return the index of the current optimal implementation flavor for the
   * clause at index @em clause_index
   * @param clause_index The index of the clause
   * @return The index of the optimal flavor
   */
  u32 GetOptimalFlavorForClause(u32 clause_index) const;

 private:
  // Run a specific clause of the filter
  void RunFilterClause(VectorProjectionIterator *vpi, u32 clause_index);

  // Run the given matching function
  std::pair<u32, double> RunFilterClauseImpl(VectorProjectionIterator *vpi,
                                             FilterManager::MatchFn func);

  // Return the clause at the given index in the filter
  const Clause *ClauseAt(u32 index) const { return &clauses_[index]; }

  // Return the agent handling the clause at the given index
  bandit::Agent *GetAgentFor(u32 clause_index);
  const bandit::Agent *GetAgentFor(u32 clause_index) const;

 private:
  // The clauses in the filter
  std::vector<Clause> clauses_;
  // The optimal order to execute the clauses
  std::vector<u32> optimal_clause_order_;
  // The adaptive policy to use
  std::unique_ptr<bandit::Policy> policy_;
  // The agents, one per clause
  std::vector<bandit::Agent> agents_;
  // Has the manager's clauses been finalized?
  bool finalized_;
};

}  // namespace tpl::sql
