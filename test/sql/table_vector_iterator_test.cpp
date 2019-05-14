#include <tuple>
#include <vector>

#include "sql_test.h"  // NOLINT

#include "sql/catalog.h"
#include "sql/execution_context.h"
#include "sql/table_vector_iterator.h"
#include "util/timer.h"

#include "sql/filter_manager.h"

namespace tpl::sql::test {

class TableVectorIteratorTest : public SqlBasedTest {};

TEST_F(TableVectorIteratorTest, InvalidBlockRangeIteratorTest) {
  auto table_id = TableIdToNum(TableId::Test1);
  auto *table = Catalog::Instance()->LookupTableById(TableId::Test1);

  const std::tuple<u32, u32, bool> test_cases[] = {
      {0, 10, true},
      {10, 0, false},
      {-10, 2, false},
      {0, table->num_blocks(), true},
      {10, table->num_blocks(), true},
      {10, table->num_blocks() + 1, false},
  };

  for (auto [start_idx, end_idx, valid] : test_cases) {
    TableVectorIterator iter(table_id, start_idx, end_idx);
    EXPECT_EQ(valid, iter.Init());
  }
}

TEST_F(TableVectorIteratorTest, EmptyIteratorTest) {
  //
  // Check to see that iteration doesn't begin without an input block
  //

  TableVectorIterator iter(TableIdToNum(TableId::EmptyTable));

  EXPECT_TRUE(iter.Init());

  while (iter.Advance()) {
    FAIL() << "Empty table should have no tuples";
  }
}

TEST_F(TableVectorIteratorTest, SimpleIteratorTest) {
  //
  // Simple test to ensure we iterate over the whole table
  //

  TableVectorIterator iter(TableIdToNum(TableId::EmptyTable));

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

TEST_F(TableVectorIteratorTest, ParallelScanTest) {
  //
  // Simple test to ensure we iterate over the whole table in parallel
  //

  struct Counter {
    u32 c;
  };

  auto scanner = [](ExecutionContext *ctx, TableVectorIterator *tvi) {
    auto *counter = ctx->GetThreadLocalStateAs<Counter>();
    while (tvi->Advance()) {
      counter->c++;
    }
  };

  // Setup thread states
  util::Region tmp("exec");
  ExecutionContext ctx(&tmp, 0);
  ctx.ResetThreadLocalState(sizeof(Counter));

  // Scan
  TableVectorIterator::ParallelScan(TableIdToNum(TableId::Test1), &ctx,
                                    scanner);

  // Combine counters
  std::vector<byte *> counters;
  ctx.CollectThreadLocalStates(counters);

  for (auto *counter : counters) {
    LOG_INFO("Count: {}", reinterpret_cast<Counter*>(counter)->c);
  }
}

}  // namespace tpl::sql::test
