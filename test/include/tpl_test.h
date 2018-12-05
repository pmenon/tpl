#pragma once

#include "gtest/gtest.h"

#include "logging/logger.h"
#include "util/common.h"
#include "util/timer.h"

namespace tpl {

class TplTest : public ::testing::Test {
 protected:
  void SetUp() override { logging::init_logger(); }

  void TearDown() override {
    // shutdown loggers
    spdlog::shutdown();
  }
};

template <typename F>
static inline double Bench(u32 repeat, const F &f) {
  if (repeat > 4) {
    // Warmup
    f();
    repeat--;
  }

  util::Timer<std::milli> timer;
  timer.Start();

  for (u32 i = 0; i < repeat; i++) {
    f();
  }

  timer.Stop();
  return timer.elapsed() / static_cast<double>(repeat);
}

}  // namespace tpl