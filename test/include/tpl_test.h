#pragma once

#include "gtest/gtest.h"

#include "logging/logger.h"
#include "util/common.h"
#include "util/cpu_info.h"
#include "util/timer.h"

namespace tpl {

class TplTest : public ::testing::Test {
 public:
  TplTest() {
    CpuInfo::Instance();
    logging::InitLogger();
  }

  virtual ~TplTest() { logging::ShutdownLogger(); }

  const char *GetTestName() const {
    return ::testing::UnitTest::GetInstance()->current_test_info()->name();
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
