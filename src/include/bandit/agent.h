#pragma once

#include <vector>

#include "bandit/policy.h"
#include "common/common.h"

namespace tpl::bandit {

/**
 * An Agent is able to take one of a set of actions at each time step. The action is chosen using a
 * strategy based on the history of prior actions and outcome observations.
 */
class Agent {
 public:
  Agent(Policy *policy, uint32_t num_actions, double prior = 0, double gamma = -1);

  /**
   * Reset all state this agent has collected.
   */
  void Reset();

  /**
   * @return The next action to be taken.
   */
  uint32_t NextAction();

  /**
   * Update the state of the agent based on reward obtained by playing the action chosen earlier.
   */
  void Observe(double reward);

  /**
   * @return The current optimal action.
   */
  uint32_t GetCurrentOptimalAction() const;

  /**
   * @return The estimations of the value of each flavor/action.
   */
  const std::vector<double> &GetValueEstimates() const { return value_estimates_; }

  /**
   * @return The counts of the number of times each flavor/action was tried.
   */
  const std::vector<uint32_t> &GetActionAttempts() const { return action_attempts_; }

  /**
   * @return The current time step.
   */
  uint32_t GetTimeStep() const { return time_step_; }

 private:
  // Policy to use for choosing the next action.
  Policy *policy_;

  // Number of actions available.
  uint32_t num_actions_;

  // Prior value estimate of an action. This is the same for every action.
  double prior_;

  // Hyper-parameter to handle dynamic distributions. By default, this is set to
  // 1 / (number of times an action was taken). This equally weights every
  // reward obtained for the actions in the entire history. This can be set to
  // a constant in [0, 1) to weigh recent rewards more than older ones.
  double gamma_;

  // Estimated value for each action.
  std::vector<double> value_estimates_;

  // Number of times each action was taken.
  std::vector<uint32_t> action_attempts_;

  // The current time step in the run.
  uint32_t time_step_;

  // Last action that was taken.
  uint32_t last_action_;
};

}  // namespace tpl::bandit
