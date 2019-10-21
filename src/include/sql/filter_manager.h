#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "bandit/policy.h"
#include "common/common.h"
#include "common/macros.h"

namespace tpl::sql {

class VectorProjectionIterator;

/**
 * An adaptive filter manager that tries to discover the optimal filter configuration.
 */
class FilterManager {
 public:
  /**
   * A generic filtering function over an input vector projection. Returns the number of tuples that
   * pass the filter.
   */
  using MatchFn = uint32_t (*)(VectorProjectionIterator *);

  /**
   * A clause in a multi-clause filter. Clauses come in multiple flavors. Flavors are logically
   * equivalent, but may differ in implementation, and thus, exhibit different run times.
   */
  struct Clause {
    // The "flavors" or implementation versions of a given conjunctive clause.
    std::vector<MatchFn> flavors;

    /**
     * @return The number of flavors.
     */
    uint32_t GetFlavorCount() const { return flavors.size(); }
  };

  /**
   * Construct the filter using the given adaptive policy.
   * @param policy_kind The adaptive policy to use.
   */
  explicit FilterManager(bandit::Policy::Kind policy_kind = bandit::Policy::EpsilonGreedy);

  /**
   * Destructor.
   */
  ~FilterManager();

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(FilterManager);

  /**
   * Start a new clause.
   */
  void StartNewClause();

  /**
   * Insert a flavor for the current clause in the filter.
   * @param flavor A filter flavor.
   */
  void InsertClauseFlavor(FilterManager::MatchFn flavor);

  /**
   * Make the manager immutable.
   */
  void Finalize();

  /**
   * Run the filters over the given vector projection @em vpi.
   * @param vpi The input projection.
   */
  void RunFilters(VectorProjectionIterator *vpi);

  /**
   * @return The index of the optimal flavor for the clause at index @em clause_index.
   */
  uint32_t GetOptimalFlavorForClause(uint32_t clause_index) const;

 private:
  // Run a specific clause of the filter
  void RunFilterClause(VectorProjectionIterator *vpi, uint32_t clause_index);

  // Run the given matching function
  static std::pair<uint32_t, double> RunFilterClauseImpl(VectorProjectionIterator *vpi,
                                                         FilterManager::MatchFn func);

 private:
  // The clauses in the filter
  std::vector<Clause> clauses_;

  // The optimal order to execute the clauses
  std::vector<uint32_t> optimal_clause_order_;

  // The adaptive policy to use
  std::unique_ptr<bandit::Policy> policy_;

  // The agents, one per clause
  std::vector<bandit::Agent> agents_;

  // Has the manager's clauses been finalized?
  bool finalized_;
};

}  // namespace tpl::sql
