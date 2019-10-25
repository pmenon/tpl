#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "bandit/policy.h"
#include "common/common.h"
#include "common/macros.h"
#include "sql/tuple_id_list.h"

namespace tpl::bandit {
class Agent;
}  // namespace tpl::bandit

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
   * The order of evaluation of terms in a clause. Each element represents the index of the term in
   * the clause to execute.
   */
  using TermEvaluationOrder = std::vector<uint16_t>;

  /**
   * A conjunctive clause in a multi-clause disjunctive normal form filter. A clause is composed of
   * one or more terms that form the factors of the conjunction. Factors can be reordered.
   */
  class Clause {
   public:
    /**
     * Create a new empty clause.
     */
    Clause();

    /**
     * Add a term to the clause.
     * @param term The term to add to this clause.
     */
    void AddTerm(MatchFn term) { terms.push_back(term); }

    /**
     * Finalize and prepare this clause for execution. After this call, the clause is immutable.
     */
    void Finalize(bandit::Policy::Kind policy_kind);

    /**
     * Run the clause over the given input projection.
     * @param vector_projection The projection to filter.
     * @param tid_list The input TID list
     */
    void RunFilter(VectorProjection *vector_projection, TupleIdList *tid_list);

    /**
     * @return The number of terms.
     */
    uint32_t GetTermCount() const { return terms.size(); }

    /**
     * @return The current optimal term ordering.
     */
    TermEvaluationOrder GetOptimalTermOrder() const;

   private:
    // The terms (i.e., factors) of the conjunction
    std::vector<MatchFn> terms;

    // Possible term orderings
    std::vector<TermEvaluationOrder> orderings_;

    // The adaptive policy
    std::unique_ptr<bandit::Policy> policy_;

    // The adaptive agent
    std::unique_ptr<bandit::Agent> agent_;
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
   * Insert a term in the current clause in the filter.
   * @param term A term.
   */
  void InsertClauseTerm(MatchFn term);

  /**
   * Insert a list of terms in the current clause in the filter.
   * @param terms The terms of the clause.
   */
  void InsertClauseTerms(std::initializer_list<MatchFn> terms);

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

  /**
   * @return The number of clauses in this filter.
   */
  uint32_t GetClauseCount() const { return clauses_.size(); }

  /**
   * @return The optimal term orderings for each clause in this filter.
   */
  std::vector<TermEvaluationOrder> GetOptimalOrderings() const;

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
