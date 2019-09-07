#pragma once

#include <vector>

#include "bandit/agent.h"
#include "bandit/multi_armed_bandit.h"
#include "util/common.h"

namespace tpl::bandit {

/**
 * The environment for running the multi-armed bandit.
 */
class Environment {
 public:
  Environment(MultiArmedBandit *bandit, Agent *agent) : bandit_(bandit), agent_(agent) {}

  /**
   * Reset the state of the environment.
   */
  void Reset();

  /**
   * Run the multi-armed bandit for num_trials on different partitions of the
   * table.
   * The reward obtained after each action is returned in rewards.
   * The action chosen is returned in actions.
   * If shuffle is set to true, then the partitions to execute an action is
   * chosen randomly without replacement. Else, it is chosen from 0 to
   * num_trials.
   */
  void Run(uint32_t num_trials, std::vector<double> *rewards, std::vector<uint32_t> *actions,
           bool shuffle = false);

 private:
  // Bandit that executes the actions.
  MultiArmedBandit *bandit_;

  // The agent who manages the state.
  Agent *agent_;
};

}  // namespace tpl::bandit
