#pragma once

#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "common/common.h"
#include "common/macros.h"

namespace tpl::vm {
class Module;
}  // namespace tpl::vm

namespace tpl::bandit {

/**
 * A multi-armed bandit that executes an action and returns the reward.
 */
class MultiArmedBandit {
 public:
  MultiArmedBandit(vm::Module *module, std::vector<std::string> action_names, uint32_t optimal = 0)
      : module_(module), action_names_(std::move(action_names)) {}

  double ExecuteAction(uint32_t action);

  /**
   * Translates execution time to reward.
   */
  static double ExecutionTimeToReward(double time);

  /**
   * Translates reward to execution time.
   */
  static double RewardToExecutionTime(double reward);

 private:
  // Not owned. It's is the responsibility of the user to make sure that this
  // is not deleted.
  vm::Module *module_;

  // The names of the  actions.
  std::vector<std::string> action_names_;
};

}  // namespace tpl::bandit
