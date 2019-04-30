#include "sql/filter_manager.h"

#include <memory>
#include <numeric>
#include <utility>
#include <vector>

#include "bandit/agent.h"
#include "bandit/multi_armed_bandit.h"
#include "bandit/policy.h"
#include "sql/vector_projection_iterator.h"
#include "util/timer.h"

namespace tpl::sql {

// ---------------------------------------------------------
// Filter Manager
// ---------------------------------------------------------

FilterManager::FilterManager(std::vector<FilterManager::Clause> &&clauses)
    : clauses_(std::move(clauses)) {}

// ---------------------------------------------------------
// Simple Filter Manager
// ---------------------------------------------------------

SimpleFilterManager::SimpleFilterManager(
    std::vector<FilterManager::Clause> &&clauses)
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
    std::vector<FilterManager::Clause> &&_clauses,
    std::unique_ptr<bandit::Policy> policy)
    : FilterManager(std::move(_clauses)),
      optimal_clause_order_(num_clauses()),
      policy_(std::move(policy)) {
  // Initialize optimal orderings, initially in the order they appear
  std::iota(optimal_clause_order_.begin(), optimal_clause_order_.end(), 0);

  // Setup the agents, once per clause
  for (u32 idx = 0; idx < num_clauses(); idx++) {
    agents_.emplace_back(policy_.get(), ClauseAt(idx)->num_flavors());
  }
}

void AdaptiveFilterManager::RunFilters(VectorProjectionIterator *vpi) {
  // Execute the clauses in what we currently believe to be the optimal order
  for (const u32 opt_clause_idx : optimal_clause_order_) {
    RunFilterClause(vpi, opt_clause_idx);
  }
}

void AdaptiveFilterManager::RunFilterClause(VectorProjectionIterator *const vpi,
                                            const u32 clause_index) {
  //
  // This function will execute the clause at the given clause index. But, we'll
  // be smart about it. We'll use our multi-armed bandit agent to predict the
  // implementation flavor to execute so as to optimize the reward: the smallest
  // execution time.
  //
  // Each clause has an agent tracking the execution history of the flavors.
  // In this round, select one using the agent's configured policy, execute it,
  // convert it to a reward and update the agent's state.
  //

  // Select the apparent optimal flavor of the clause to execute
  bandit::Agent *agent = GetAgentFor(clause_index);
  const u32 opt_flavor_idx = agent->NextAction();
  const auto opt_match_func = ClauseAt(clause_index)->flavors[opt_flavor_idx];

  // Run the filter
  auto [_, exec_ms] = RunFilterClauseImpl(vpi, opt_match_func);
  (void)_;

  // Update the agent's state
  double reward = bandit::MultiArmedBandit::ExecutionTimeToReward(exec_ms);
  agent->Observe(reward);
  LOG_DEBUG("Clause {} observed reward {}", clause_index, reward);
}

std::pair<u32, double> AdaptiveFilterManager::RunFilterClauseImpl(
    VectorProjectionIterator *vpi, FilterManager::MatchFn func) {
  // Time and execute the match function, returning the number of selected
  // tuples and the execution time in milliseconds
  util::Timer<> timer;
  timer.Start();
  const u32 num_selected = func(vpi);
  timer.Stop();
  return std::make_pair(num_selected, timer.elapsed());
}

u32 AdaptiveFilterManager::GetOptimalFlavorForClause(u32 clause_index) const {
  const bandit::Agent *agent = GetAgentFor(clause_index);
  return agent->GetCurrentOptimalAction();
}

bandit::Agent *AdaptiveFilterManager::GetAgentFor(const u32 clause_index) {
  return &agents_[clause_index];
}

const bandit::Agent *AdaptiveFilterManager::GetAgentFor(
    const u32 clause_index) const {
  return &agents_[clause_index];
}

// ---------------------------------------------------------
// Filter Manager Builder
// ---------------------------------------------------------

FilterManagerBuilder::FilterManagerBuilder() : finalized_(false) {}

void FilterManagerBuilder::StartNewClause() { clauses_.emplace_back(); }

void FilterManagerBuilder::InsertClauseFlavor(FilterManager::MatchFn flavor) {
  curr_clause().flavors.push_back(flavor);
}

std::unique_ptr<FilterManager> FilterManagerBuilder::BuildSimple() {
  if (finalized_) {
    return nullptr;
  }

  std::unique_ptr<FilterManager> result = std::unique_ptr<SimpleFilterManager>(
      new SimpleFilterManager(std::move(clauses_)));

  finalized_ = true;

  return result;
}

std::unique_ptr<FilterManager> FilterManagerBuilder::BuildAdaptive(
    bandit::Policy::Kind policy_kind) {
  if (finalized_) {
    return nullptr;
  }

  // Create the policy
  // TODO(pmenon): Move this into some factory
  std::unique_ptr<bandit::Policy> policy;
  switch (policy_kind) {
    case bandit::Policy::Kind::EpsilonGreedy: {
      policy = std::make_unique<bandit::EpsilonGreedyPolicy>(
          bandit::EpsilonGreedyPolicy::kDefaultEpsilon);
      break;
    }
    case bandit::Policy::Greedy: {
      policy = std::make_unique<bandit::GreedyPolicy>();
      break;
    }
    case bandit::Policy::Random: {
      policy = std::make_unique<bandit::RandomPolicy>();
      break;
    }
    case bandit::Policy::UCB: {
      policy = std::make_unique<bandit::UCBPolicy>(
          bandit::UCBPolicy::kDefaultUCBHyperParam);
      break;
    }
    case bandit::Policy::FixedAction: {
      policy = std::make_unique<bandit::FixedActionPolicy>(0);
      break;
    }
    default: { UNREACHABLE("Impossible bandit policy kind"); }
  }

  // Now the filter
  auto result = std::unique_ptr<FilterManager>(
      new AdaptiveFilterManager(std::move(clauses_), std::move(policy)));

  finalized_ = true;

  return result;
}

}  // namespace tpl::sql
