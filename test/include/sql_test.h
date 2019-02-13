#pragma once

#include "gtest/gtest.h"

#include "sql/catalog.h"
#include "tpl_test.h"

namespace tpl {

class SqlBasedTest : public TplTest {
 protected:
  void SetUp() override {
    TplTest::SetUp();
    sql::Catalog::Instance();
  }
};

}  // namespace tpl
