#include "bandit/policy.h"

#include <algorithm>
#include <cfloat>
#include <cmath>
#include <limits>
#include <vector>

#include "bandit/agent.h"

#define MAX_EXPLORATION_VALUE 1000

namespace tpl::bandit {

namespace {

/**
 * Return the index of the maximum value. If multiple values are tied for the
 * maximum, then the index of a random value from that subset is returned.
 */
u32 ChooseBestIndex(const std::vector<double> &values,
                    std::mt19937 *generator) {
  auto max_value = *std::max_element(values.begin(), values.end());
  std::vector<u32> best_indices;

  for (u32 i = 0; i < values.size(); ++i) {
    if (std::fabs(values[i] - max_value) <=
        std::numeric_limits<double>::epsilon()) {
      best_indices.push_back(i);
    }
  }

  if (best_indices.size() == 1) {
    return best_indices[0];
  }
  return best_indices[(*generator)() % best_indices.size()];
}

}  // namespace

// With probability 1 - epsilon choose the action with the best value estimate.
// Otherwise choose a random action.
u32 EpsilonGreedyPolicy::NextAction(Agent *agent) {
  const auto &value_estimates = agent->value_estimates();
  if (real_(generator_) < epsilon_) {
    return generator_() % value_estimates.size();
  }
  return ChooseBestIndex(value_estimates, &generator_);
}

// Choose the policy that maximises
//                                        ln(timesteps)
// (value_estimate + c * sqrt( ------------------------------------ ) )
//                             number of times the action was taken
//
// where c is a hyperparamter.
// If an action was never taken, it gets infinite preference.
u32 UCBPolicy::NextAction(Agent *agent) {
  const auto &value_estimates = agent->value_estimates();
  const auto &action_attempts = agent->action_attempts();

  std::vector<double> values(action_attempts.size(), 0.0);
  std::vector<u32> best_actions;

  for (u32 i = 0; i < action_attempts.size(); ++i) {
    double exploration =
        action_attempts[i] == 0
            ? MAX_EXPLORATION_VALUE
            : std::sqrt((std::log(agent->timestep() + 1) / action_attempts[i]));
    values[i] = value_estimates[i] + c_ * exploration;
  }

  return ChooseBestIndex(values, &generator_);
}

}  // namespace tpl::bandit
