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

  static u16 TableIdToNum(sql::TableId table_id) {
    return static_cast<u16>(table_id);
  }
};

}  // namespace tpl
