#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "bandit/policy.h"
#include "common/common.h"
#include "common/macros.h"
#include "sql/tuple_id_list.h"

namespace tpl::sql {

class VectorProjection;
class VectorProjectionIterator;

/**
 * An adaptive filter manager that tries to discover the optimal filter configuration.
 */
class FilterManager {
 public:
  /**
   * A vectorized filter function over a vector projection.
   */
  using MatchFn = void (*)(VectorProjection *, TupleIdList *);

  /**
   * A clause in a multi-clause filter. Clauses come in multiple flavors. Flavors are logically
   * equivalent, but may differ in implementation, and thus, exhibit different run times.
   */
  struct Clause {
    // The terms (i.e., factors) of the conjunction
    std::vector<MatchFn> terms;

    /**
     * @return The number of flavors.
     */
    uint32_t GetTermCount() const { return terms.size(); }
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
   * @param term A filter flavor.
   */
  void InsertClauseTerm(MatchFn term);

  /**
   * Make the manager immutable.
   */
  void Finalize();

  /**
   * Run the filters over the given vector projection.
   * @param vector_projection The projection to filter.
   */
  void RunFilters(VectorProjection *vector_projection);

  /**
   * Run all configured filters over the vector projection the input iterator is iterating over.
   * @param vpi The input projection iterator storing the projection to filter.
   */
  void RunFilters(VectorProjectionIterator *vpi);

  /**
   * @return If the filter manager has been finalized and frozen.
   */
  bool IsFinalized() const { return finalized_; }

 private:
  // The clauses in the filter
  std::vector<Clause> clauses_;

  // The optimal order to execute the clauses
  std::vector<uint32_t> optimal_clause_order_;

  // The adaptive policy to use
  std::unique_ptr<bandit::Policy> policy_;

  // List used for disjunctions
  TupleIdList input_list_;
  TupleIdList tmp_list_;
  TupleIdList output_list_;

  // Has the manager's clauses been finalized?
  bool finalized_;
};

}  // namespace tpl::sql
