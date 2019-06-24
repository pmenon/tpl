#pragma once

#include <condition_variable>
#include <mutex>

#include "util/common.h"
#include "util/macros.h"

namespace tpl::util {

/**
 * A cyclic barrier
 */
class Barrier {
 public:
  /**
   * Create a new barrier with the given count.
   */
  explicit Barrier(const u32 count)
      : generation_(0), count_(count), reset_value_(count) {}

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(Barrier);

  /**
   * Wait at the barrier until the generation ends.
   */
  void Wait() {
    std::unique_lock<std::mutex> lock(mutex_);

    // If we're the last to reach the barrier, bump the generation and notify
    // everyone waiting.
    if (--count_ == 0) {
      generation_++;
      count_ = reset_value_;
      cv_.notify_all();
      return;
    }

    // We're not the last at the barrier, so we wait
    const u32 gen = generation_;
    cv_.wait(lock, [&]() { return gen != generation_; });
  }

  /**
   * Get the current generation the barrier is on.
   */
  u32 GetGeneration() {
    std::unique_lock<std::mutex> lock(mutex_);
    return generation_;
  }

 private:
  std::mutex mutex_;
  std::condition_variable cv_;

  // The current generation
  u32 generation_;
  // The current outstanding count
  u32 count_;
  // The value to reset the count to when rolling into a new generation
  u32 reset_value_;
};

}  // namespace tpl::util
