#pragma once

#include "gtest/gtest.h"

#include "logging/logger.h"

namespace tpl {

class TplTest : public ::testing::Test {
 protected:
  void SetUp() override { logging::init_logger(); }

  void TearDown() override {
    // shutdown loggers
    spdlog::shutdown();
  }
};

}  // namespace tpl