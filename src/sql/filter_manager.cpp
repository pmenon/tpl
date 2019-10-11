#include "sql/filter_manager.h"

#include <memory>
#include <numeric>
#include <utility>
#include <vector>

#include "bandit/agent.h"
#include "bandit/multi_armed_bandit.h"
#include "bandit/policy.h"
#include "logging/logger.h"
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
    default: { UNREACHABLE("Impossible bandit policy kind"); }
  }
}

}  // namespace

FilterManager::FilterManager(const bandit::Policy::Kind policy_kind)
    : policy_(CreatePolicy(policy_kind)), finalized_(false) {}

FilterManager::~FilterManager() = default;

void FilterManager::StartNewClause() {
  TPL_ASSERT(!finalized_, "Cannot modify filter manager after finalization");
  clauses_.emplace_back();
}

void FilterManager::InsertClauseFlavor(const FilterManager::MatchFn flavor) {
  TPL_ASSERT(!finalized_, "Cannot modify filter manager after finalization");
  TPL_ASSERT(!clauses_.empty(), "Inserting flavor without clause");
  auto &current_clause = clauses_.back();
  current_clause.flavors.push_back(flavor);
}

void FilterManager::Finalize() {
  if (finalized_) {
    return;
  }

  optimal_clause_order_.resize(clauses_.size());

  // Initialize optimal orderings, initially in the order they appear
  std::iota(optimal_clause_order_.begin(), optimal_clause_order_.end(), 0);

  // Setup the agents, once per clause
  for (const auto &clause : clauses_) {
    agents_.emplace_back(policy_.get(), clause.GetFlavorCount());
  }

  finalized_ = true;
}

void FilterManager::RunFilters(VectorProjectionIterator *vpi) {
  TPL_ASSERT(finalized_, "Must finalize the filter before it can be used");

  // Execute the clauses in what we currently believe to be the optimal order
  for (const uint32_t opt_clause_idx : optimal_clause_order_) {
    RunFilterClause(vpi, opt_clause_idx);
  }
}

void FilterManager::RunFilterClause(VectorProjectionIterator *vpi, const uint32_t clause_index) {
  // This function will execute the clause at the given clause index. But, we'll be smart about it.
  // We'll use our multi-armed bandit agent to predict the implementation flavor to execute so as to
  // optimize the reward: the smallest execution time.
  //
  // Each clause has an agent tracking the execution history of the flavors. In this round, select
  // one using the agent's configured policy, execute it, convert it to a reward and update the
  // agent's state.

  // Select the apparent optimal flavor of the clause to execute
  bandit::Agent *agent = &agents_[clause_index];
  const uint32_t opt_flavor_idx = agent->NextAction();
  const auto opt_match_func = clauses_[clause_index].flavors[opt_flavor_idx];

  // Run the filter
  auto [_, exec_ms] = RunFilterClauseImpl(vpi, opt_match_func);
  (void)_;

  // Update the agent's state
  double reward = bandit::MultiArmedBandit::ExecutionTimeToReward(exec_ms);
  agent->Observe(reward);
  LOG_DEBUG("Clause {} observed reward {}", clause_index, reward);
}

std::pair<uint32_t, double> FilterManager::RunFilterClauseImpl(VectorProjectionIterator *vpi,
                                                               const FilterManager::MatchFn func) {
  // Time and execute the match function, returning the number of selected tuples and the execution
  // time in milliseconds
  util::Timer<> timer;
  timer.Start();
  const uint32_t num_selected = func(vpi);
  timer.Stop();
  return std::make_pair(num_selected, timer.elapsed());
}

uint32_t FilterManager::GetOptimalFlavorForClause(uint32_t clause_index) const {
  const bandit::Agent *agent = &agents_[clause_index];
  return agent->GetCurrentOptimalAction();
}

}  // namespace tpl::sql
