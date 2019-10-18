#pragma once

#include <thread>  // NOLINT
#include <vector>

#include "gtest/gtest.h"

#include "common/common.h"
#include "common/cpu_info.h"
#include "logging/logger.h"
#include "util/barrier.h"
#include "util/timer.h"

namespace tpl {

class TplTest : public ::testing::Test {
 public:
  TplTest() {
    logging::InitLogger();
    CpuInfo::Instance();
  }

  ~TplTest() override { logging::ShutdownLogger(); }

  const char *GetTestName() const {
    return ::testing::UnitTest::GetInstance()->current_test_info()->name();
  }
};

template <typename F>
static inline double Bench(uint32_t repeat, const F &f) {
  if (repeat > 4) {
    // Warmup
    f();
    repeat--;
  }

  util::Timer<std::milli> timer;
  timer.Start();

  for (uint32_t i = 0; i < repeat; i++) {
    f();
  }

  timer.Stop();
  return timer.GetElapsed() / static_cast<double>(repeat);
}

template <typename F>
static inline void LaunchParallel(uint32_t num_threads, const F &f) {
  util::Barrier barrier(num_threads + 1);

  std::vector<std::thread> thread_group;

  for (uint32_t thread_idx = 0; thread_idx < num_threads; thread_idx++) {
    thread_group.emplace_back(
        [&](auto tid) {
          barrier.Wait();
          f(tid);
        },
        thread_idx);
  }

  barrier.Wait();

  for (uint32_t i = 0; i < num_threads; i++) {
    thread_group[i].join();
  }
}

}  // namespace tpl
