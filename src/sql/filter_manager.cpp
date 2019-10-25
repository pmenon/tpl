#include "sql/filter_manager.h"

#include <memory>
#include <numeric>

#include "bandit/agent.h"
#include "bandit/multi_armed_bandit.h"
#include "bandit/policy.h"
#include "sql/vector_projection_iterator.h"
#include "util/timer.h"

namespace tpl::sql {

namespace {

// TODO(pmenon): Move to some PolicyFactory
std::unique_ptr<bandit::Policy> CreatePolicy(bandit::Policy::Kind policy_kind) {
  switch (policy_kind) {
    case bandit::Policy::Kind::EpsilonGreedy:
      return std::make_unique<bandit::EpsilonGreedyPolicy>(
          bandit::EpsilonGreedyPolicy::kDefaultEpsilon);
    case bandit::Policy::Greedy:
      return std::make_unique<bandit::GreedyPolicy>();
    case bandit::Policy::Random:
      return std::make_unique<bandit::RandomPolicy>();
    case bandit::Policy::UCB:
      return std::make_unique<bandit::UCBPolicy>(bandit::UCBPolicy::kDefaultUCBHyperParam);
    case bandit::Policy::FixedAction:
      return std::make_unique<bandit::FixedActionPolicy>(0);
    case bandit::Policy::AnnealingEpsilonGreedy:
      return std::make_unique<bandit::AnnealingEpsilonGreedyPolicy>();
    default:
      UNREACHABLE("Impossible bandit policy kind");
  }
}

}  // namespace

//===----------------------------------------------------------------------===//
//
// Filter Manager Clause
//
//===----------------------------------------------------------------------===//

FilterManager::Clause::Clause() : agent_(nullptr) {}

void FilterManager::Clause::Finalize(bandit::Policy::Kind policy_kind) {
  // Create orderings

  // TODO(pmenon): This is retarded. Be smarter about this exploration.

  const uint32_t num_orderings = util::MathUtil::Factorial(GetTermCount());
  orderings_.reserve(num_orderings);

  TermEvaluationOrder order(GetTermCount());
  std::iota(order.begin(), order.end(), uint16_t{0});
  do {
    orderings_.push_back(order);
  } while (std::next_permutation(order.begin(), order.end()));

  policy_ = CreatePolicy(policy_kind);
  agent_ = std::make_unique<bandit::Agent>(policy_.get(), num_orderings);
}

void FilterManager::Clause::RunFilter(VectorProjection *vector_projection, TupleIdList *tid_list) {
  util::Timer<std::micro> timer;
  timer.Start();

  for (const auto &term_idx : orderings_[agent_->NextAction()]) {
    terms[term_idx](vector_projection, tid_list);
  }

  timer.Stop();

  double reward = bandit::MultiArmedBandit::ExecutionTimeToReward(timer.GetElapsed());
  agent_->Observe(reward);
}

FilterManager::TermEvaluationOrder FilterManager::Clause::GetOptimalTermOrder() const {
  const uint32_t opt_term_order_idx = agent_->GetCurrentOptimalAction();
  return orderings_[opt_term_order_idx];
}

//===----------------------------------------------------------------------===//
//
// Filter Manager
//
//===----------------------------------------------------------------------===//

FilterManager::FilterManager(const bandit::Policy::Kind policy_kind)
    : policy_(CreatePolicy(policy_kind)),
      input_list_(kDefaultVectorSize),
      tmp_list_(kDefaultVectorSize),
      output_list_(kDefaultVectorSize),
      finalized_(false) {}

FilterManager::~FilterManager() = default;

void FilterManager::StartNewClause() {
  TPL_ASSERT(!finalized_, "Cannot modify filter manager after finalization");
  clauses_.emplace_back();
}

void FilterManager::InsertClauseTerm(const FilterManager::MatchFn term) {
  TPL_ASSERT(!finalized_, "Cannot modify filter manager after finalization");
  TPL_ASSERT(!clauses_.empty(), "Inserting flavor without clause");
  clauses_.back().AddTerm(term);
}

void FilterManager::InsertClauseTerms(std::initializer_list<MatchFn> terms) {
  for (auto term : terms) {
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
    clause.Finalize(policy_->GetKind());
  }

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

std::vector<FilterManager::TermEvaluationOrder> FilterManager::GetOptimalOrderings() const {
  std::vector<FilterManager::TermEvaluationOrder> opt_order;
  opt_order.reserve(GetClauseCount());
  for (const auto &clause : clauses_) {
    opt_order.emplace_back(clause.GetOptimalTermOrder());
  }
  return opt_order;
}

}  // namespace tpl::sql
