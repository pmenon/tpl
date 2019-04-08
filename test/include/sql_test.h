#pragma once

#include "gtest/gtest.h"

#include "tpl_test.h"  // NOLINT

#include "sql/catalog.h"

namespace tpl {

class SqlBasedTest : public TplTest {
 protected:
  void SetUp() override {
    TplTest::SetUp();
    sql::Catalog::Instance();
  }
};

}  // namespace tpl
