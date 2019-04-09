#include "sql_test.h"  // NOLINT

#include "sql/catalog.h"
#include "sql/table_vector_iterator.h"
#include "util/timer.h"

namespace tpl::sql::test {

class TableVectorIteratorTest : public SqlBasedTest {};

TEST_F(TableVectorIteratorTest, EmptyIteratorTest) {
  //
  // Check to see that iteration doesn't begin without an input block
  //

  TableVectorIterator iter(static_cast<u16>(TableId::EmptyTable));

  EXPECT_TRUE(iter.Init());

  while (iter.Advance()) {
    FAIL() << "Empty table should have no tuples";
  }
}

TEST_F(TableVectorIteratorTest, SimpleIteratorTest) {
  //
  // Simple test to ensure we iterate over the whole table
  //

  TableVectorIterator iter(static_cast<u16>(TableId::EmptyTable));

  EXPECT_TRUE(iter.Init());

  VectorProjectionIterator *vpi = iter.vector_projection_iterator();

  u32 num_tuples = 0;
  while (iter.Advance()) {
    for (; vpi->HasNext(); vpi->Advance()) {
      num_tuples++;
    }
    vpi->Reset();
  }

  EXPECT_EQ(iter.table()->num_tuples(), num_tuples);
}

}  // namespace tpl::sql::test
