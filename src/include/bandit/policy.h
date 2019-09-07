#pragma once

#include <random>
#include <vector>

#include "common/common.h"

namespace tpl::bandit {

class Agent;

/**
 * A policy prescribes an action to be taken based on the memory of an agent.
 */
class Policy {
 public:
  /**
   * An enumeration capturing different policies for choosing actions.
   */
  enum Kind : uint8_t {
    EpsilonGreedy = 0,
    Greedy = 1,
    Random = 2,
    UCB = 3,
    FixedAction = 4,
    AnnealingEpsilonGreedy = 5,
  };

  /**
   * Construct a policy of the given kind
   *
   * @param kind The specific kind of policy this is
   */
  explicit Policy(Kind kind);

  /**
   * Virtual destructor
   */
  virtual ~Policy() = default;

  /**
   * Returns the next action to take according to the policy
   */
  virtual uint32_t NextAction(Agent *agent) = 0;

  /**
   * Returns the specific kind of policy this is
   */
  Kind kind() { return kind_; }

 protected:
  Kind kind_;
  std::mt19937 generator_;
};

/**
 * The Epsilon-Greedy policy will choose a random action with probability
 * epsilon and take the best apparent approach with probability 1-epsilon. If
 * multiple actions are tied for best choice, then a random action from that
 * subset is selected.
 */
class EpsilonGreedyPolicy : public Policy {
 public:
  static constexpr const double kDefaultEpsilon = 0.1;

  explicit EpsilonGreedyPolicy(double epsilon, Kind kind = Kind::EpsilonGreedy)
      : Policy(kind), epsilon_(epsilon), real_(0, 1) {}

  uint32_t NextAction(Agent *agent) override;

 protected:
  void set_epsilon(const double epsilon) { epsilon_ = epsilon; }

 private:
  double epsilon_;
  // Real-valued distribution between [0,1]
  std::uniform_real_distribution<double> real_;
};

/**
 * The Greedy policy only takes the best apparent action, with ties broken by
 * random selection. This can be seen as a special case of EpsilonGreedy where
 * epsilon = 0 i.e. always exploit.
 */
class GreedyPolicy : public EpsilonGreedyPolicy {
 public:
  GreedyPolicy() : EpsilonGreedyPolicy(0, Kind::Greedy) {}
};

/**
 * The Random policy randomly selects from all available actions with no
 * consideration to which is apparently best. This can be seen as a special
 * case of EpsilonGreedy where epsilon = 1 i.e. always explore.
 */
class RandomPolicy : public EpsilonGreedyPolicy {
 public:
  RandomPolicy() : EpsilonGreedyPolicy(1, Kind::Random) {}
};

/**
 * The Upper Confidence Bound algorithm (UCB). It applies an exploration
 * factor to the expected value of each arm which can influence a greedy
 * selection strategy to more intelligently explore less confident options.
 */
class UCBPolicy : public Policy {
 public:
  static constexpr const double kDefaultUCBHyperParam = 1.0;

  explicit UCBPolicy(double c) : Policy(Kind::UCB), c_(c) {}

  uint32_t NextAction(Agent *agent) override;

 private:
  // Hyper-parameter that decides the weight of the penalty term.
  double c_;
};

/**
 * Fixed Action Policy. It returns a fixed action irrespective of the rewards
 * obtained.
 */
class FixedActionPolicy : public Policy {
 public:
  explicit FixedActionPolicy(uint32_t action) : Policy(Kind::FixedAction), action_(action) {}

  uint32_t NextAction(Agent *agent) override { return action_; }

 private:
  uint32_t action_;
};

/**
 * An annealing epsilon greedy policy is one that decays the epsilon value over
 * time. This obviates the need to tune the epsilon hyper-parameter.
 */
class AnnealingEpsilonGreedyPolicy : public EpsilonGreedyPolicy {
 public:
  AnnealingEpsilonGreedyPolicy() : EpsilonGreedyPolicy(Kind::AnnealingEpsilonGreedy) {}

  uint32_t NextAction(Agent *agent) override;
};

}  // namespace tpl::bandit
