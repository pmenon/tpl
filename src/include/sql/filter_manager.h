#pragma once

#include <random>
#include <utility>
#include <vector>

#include "common/common.h"
#include "common/macros.h"
#include "sql/tuple_id_list.h"

namespace tpl::sql {

class VectorProjection;
class VectorProjectionIterator;

/**
 * An adaptive filter that tries to discover the optimal filter configuration. Users build up the
 * filter in disjunctive normal form (DNF). Each summand (i.e., clause in this context) begins with
 * a call to FilterManager::StartNewClause(). Factors (i.e., terms in this context) can be added to
 * the current summand through FilterManager::InsertClauseTerm(). Each summand must only be
 * composed of conjunctive terms. When finished, use FilterManager::Finalize(). The filter is
 * immutable after finalization.
 *
 * @code
 * FilterManager filter;
 * filter.StartNewClause();
 * filter.InsertClauseFilter(Clause0Term0);
 * // Remaining clauses and terms ...
 * filter.Finalize();
 *
 * // Once finalized, the filter can be applied to projections
 * VectorProjectionIterator *vpi = ...
 * filter.RunFilters(vpi);
 *
 * // At this point, iteration over the VPI will only hit selected, i.e., active, tuples.
 * for (; vpi->HasNextFiltered(); vpi->AdvanceFiltered()) {
 *   // Only touch unfiltered tuples ...
 * }
 * @endcode
 */
class FilterManager {
 public:
  /**
   * A vectorized filter function over a vector projection.
   */
  using MatchFn = void (*)(VectorProjection *, TupleIdList *);

  /**
   * A conjunctive clause in a multi-clause disjunctive normal form filter. A clause is composed of
   * one or more terms that form the factors of the conjunction. Factors can be reordered.
   */
  class Clause {
   public:
    /**
     * Create a new empty clause.
     * @param stat_sample_freq The frequency to sample term runtime/selectivity stats.
     */
    explicit Clause(float stat_sample_freq);

    /**
     * Add a term to the clause.
     * @param term The term to add to this clause.
     */
    void AddTerm(MatchFn term) { terms_.emplace_back(term); }

    /**
     * Finalize and prepare this clause for execution. After this call, the clause is immutable.
     */
    void Finalize();

    /**
     * Run the clause over the given input projection.
     * @param vector_projection The projection to filter.
     * @param tid_list The input TID list.
     */
    void RunFilter(VectorProjection *vector_projection, TupleIdList *tid_list);

    /**
     * @return The number of times the clause has samples its terms' selectivities.
     * */
    uint32_t GetResampleCount() const { return sample_count_; }

    /**
     * @return The order of application of the terms in this clause the filter manage believes is
     *         currently optimal. This order may change over the course of its usage.
     */
    const std::vector<uint32_t> &GetOptimalTermOrder() const { return optimal_term_order_; }

   private:
    // Indicates if statistics for all terms should be recollected.
    bool ShouldReRank();

    // A term in the clause.
    struct Term {
      // The function implementing the term.
      MatchFn fn;
      // The current rank.
      double rank;
      // Create a new term with no rank.
      explicit Term(MatchFn term_fn) : fn(term_fn), rank(0.0) {}
    };

   private:
    // The terms (i.e., factors) of the conjunction.
    std::vector<Term> terms_;

    // The optimal order to execute the terms.
    std::vector<uint32_t> optimal_term_order_;

    // Temporary lists used during re-sampling.
    TupleIdList input_copy_, temp_;

    // Frequency at which to sample stats, a number in the range [0.0, 1.0].
    float sample_freq_;
    // The number of times samples have been collected.
    uint32_t sample_count_;

    // Random number generator.
    std::mt19937 gen_;
    std::uniform_real_distribution<float> dist_;
  };

  /**
   * Construct an empty filter.
   */
  FilterManager();

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
   * @return The ordering of clauses this manager believe is currently optimal. This ordering may
   *         change over the course of a manager's use.
   */
  std::vector<const Clause *> GetOptimalClauseOrder() const;

 private:
  // The clauses in the filter
  std::vector<Clause> clauses_;

  // The optimal order to execute the clauses
  std::vector<uint32_t> optimal_clause_order_;

  // The input and output TID lists, and a temporary list. These are used during
  // filter evaluation to carry TIDs across disjunctive clauses.
  TupleIdList input_list_, output_list_, tmp_list_;

  // Has the manager's clauses been finalized?
  bool finalized_;
};

}  // namespace tpl::sql
