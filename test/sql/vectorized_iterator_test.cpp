#include "gtest/gtest.h"

#include "tpl_test.h"

#include "sql/catalog.h"
#include "sql/table_iterator.h"
#include "sql/vectorized_iterator.h"

namespace tpl::sema::test {

class VectorizedIteratorTest : public TplTest {};

TEST_F(VectorizedIteratorTest, SimpleScanTest) {
  auto table = sql::Catalog::instance()->LookupTableById(sql::TableId::Test1);

  u32 num_blocks = 0;
  for (sql::VectorizedIterator iter(*table); iter.HasNext(); iter.Next()) {
    num_blocks++;
  }

  EXPECT_EQ(table->blocks().size(), num_blocks);

  u32 num_rows = 0;
  for (sql::TableIterator iter(*table); iter.HasNext(); iter.Next()) {
    num_rows++;
  }

  EXPECT_EQ(table->num_rows(), num_rows);
}

TEST_F(VectorizedIteratorTest, DISABLED_PerfTest) {
  auto table = sql::Catalog::instance()->LookupTableById(sql::TableId::Test1);

  double t1 = Bench(4, [&table]() {
    u32 num_rows = 0;
    for (sql::TableIterator iter(*table); iter.HasNext(); iter.Next()) {
      num_rows++;
    }
    std::cout << num_rows << std::endl;
  });

  std::cout << t1 << std::endl;

  //  double t2 = Bench(4, [&table]() {
  //    u32 num_rows = 0;
  //    for (sql::TableIterator iter(table); iter.Next();) {
  //      num_rows++;
  //    }
  //    std::cout << num_rows << std::endl;
  //  });
  //
  //  std::cout << t2 << std::endl;
}

}  // namespace tpl::sema::test