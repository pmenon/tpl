#pragma once

#include <condition_variable>  // NOLINT
#include <mutex>               // NOLINT

#include "util/common.h"
#include "util/macros.h"

namespace tpl::util {

/**
 * A cyclic barrier is a synchronization construct that allows multiple threads
 * to wait for each other to reach a common barrier point. The barrier is
 * configured with a particular number of threads (N) and, as each thread
 * reaches the barrier, must wait until the remaining N threads arrive. Once the
 * last thread arrive at the barrier point, all waiting threads proceed and the
 * barrier is reset.
 *
 * The barrier is considered "cyclic" because it can be reused.
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
   * Wait at the barrier point until all remaining threads arrive.
   */
  void Wait() {
    std::unique_lock<std::mutex> lock(mutex_);

    // If we're the last thread to arrive, bump the generation, reset the count
    // and wake up all waiting threads.
    if (--count_ == 0) {
      generation_++;
      count_ = reset_value_;
      cv_.notify_all();
      return;
    }

    // Otherwise, we wait for some thread (i.e., the last thread to arrive) to
    // bump the generation and notify us.
    const u32 gen = generation_;
    cv_.wait(lock, [&]() { return gen != generation_; });
  }

  /**
   * Get the current generation the barrier is in.
   */
  u32 GetGeneration() {
    std::unique_lock<std::mutex> lock(mutex_);
    return generation_;
  }

 private:
  // The mutex used to protect all fields
  std::mutex mutex_;
  // The condition variable threads wait on
  std::condition_variable cv_;

  // The current generation
  u32 generation_;
  // The current outstanding count
  u32 count_;
  // The value to reset the count to when rolling into a new generation
  u32 reset_value_;
};

}  // namespace tpl::util
