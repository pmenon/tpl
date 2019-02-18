#include "tpl_test.h"

#include <random>

#include "sql/concise_hash_table.h"
#include "util/hash.h"
#include "util/macros.h"

namespace tpl::sql::test {

/// This is the tuple we insert into the hash table
struct Tuple {
  u64 a, b, c, d;
};

/// The function to determine whether two tuples have equivalent keys
static inline bool TupleKeyEq(UNUSED void *_, void *probe_tuple,
                              void *table_tuple) {
  auto *lhs = reinterpret_cast<const Tuple *>(probe_tuple);
  auto *rhs = reinterpret_cast<const Tuple *>(table_tuple);
  return lhs->a == rhs->a;
}

class ConciseHashTableTest : public TplTest {};

TEST_F(ConciseHashTableTest, InsertTest) {
  const u32 num_tuples = 10;
  const u32 probe_length = 2;

  ConciseHashTable table(probe_length);
  table.SetSize(num_tuples);

  EXPECT_EQ(32u, table.Capacity());

  // 0 should go into the zero-th slot
  auto _0_slot = table.Insert(0);
  EXPECT_FALSE(_0_slot.IsOverflow());
  EXPECT_EQ(0u, _0_slot.GetIndex());

  // 1 should go into the second slot
  auto _1_slot = table.Insert(1);
  EXPECT_FALSE(_1_slot.IsOverflow());
  EXPECT_EQ(1u, _1_slot.GetIndex());

  // 32 should go into the zero-th slot because the capacity of the table is 32
  // But, the first two slots are occupied (by 0 and 1). Since the probe length
  // is 2, 32 should fall to the overflow
  auto _32_slot = table.Insert(32);
  EXPECT_TRUE(_32_slot.IsOverflow());

  // 2 should into the third slot
  auto _2_slot = table.Insert(2);
  EXPECT_FALSE(_2_slot.IsOverflow());
  EXPECT_EQ(2u, _2_slot.GetIndex());
}

TEST_F(ConciseHashTableTest, InsertOverflowTest) {
  const u32 num_tuples = 20;
  const u32 probe_length = 2;

  //
  // Create a CHT with one bucket with 64 slots
  //

  ConciseHashTable table(probe_length);
  table.SetSize(num_tuples);

  EXPECT_EQ(64u, table.Capacity());

  // 33 should go into the 33rd slot
  auto _33_slot = table.Insert(33);
  EXPECT_FALSE(_33_slot.IsOverflow());
  EXPECT_EQ(33u, _33_slot.GetIndex());

  // A second 33 should go into the 34th slot
  auto _33_v2_slot = table.Insert(33);
  EXPECT_FALSE(_33_v2_slot.IsOverflow());
  EXPECT_EQ(34u, _33_v2_slot.GetIndex());

  // A fourth 33 should overflow since probe length is 2
  auto _33_v3_slot = table.Insert(33);
  EXPECT_TRUE(_33_v3_slot.IsOverflow());

  // 34 should go into the 35th bucket (since the 34th is occupied by 33 v2)
  auto _34_slot = table.Insert(34);
  EXPECT_FALSE(_34_slot.IsOverflow());
  EXPECT_EQ(35u, _34_slot.GetIndex());
}

TEST_F(ConciseHashTableTest, MultiGroupInsertTest) {
  const u32 num_tuples = 100;
  const u32 probe_length = 2;

  //
  // Create a CHT with four buckets (each with 64 slots)
  //

  ConciseHashTable table(probe_length);
  table.SetSize(num_tuples);

  EXPECT_EQ(256u, table.Capacity());

  // 33 goes in the first group, in the 33rd slot
  auto _33_slot = table.Insert(33);
  EXPECT_FALSE(_33_slot.IsOverflow());
  EXPECT_EQ(33u, _33_slot.GetIndex());

  // 97 (64+33) goes in the second group in the 33rd group bit, but the 97th
  // overall slot
  auto _97_slot = table.Insert(97);
  EXPECT_FALSE(_97_slot.IsOverflow());
  EXPECT_EQ(97u, _97_slot.GetIndex());

  // 161 (64+64+33) goes in the third group in the 33rd group bit, but the 130th
  // overall slot
  auto _161_slot = table.Insert(161);
  EXPECT_FALSE(_161_slot.IsOverflow());
  EXPECT_EQ(161u, _161_slot.GetIndex());

  // 225 (64+64+64+33) goes in the fourth (and last) group, in the 33rd group
  // bit, but the 194th overall slot
  auto _225_slot = table.Insert(225);
  EXPECT_FALSE(_225_slot.IsOverflow());
  EXPECT_EQ(225u, _225_slot.GetIndex());

  // 289 (64+64+64+33) cycles back into the **first** group, in the 34th group
  // bit (the 33rd is occupied by the first insert), hence takes the 34th
  // overall slot
  auto _289_slot = table.Insert(289);
  EXPECT_FALSE(_289_slot.IsOverflow());
  EXPECT_EQ(34u, _289_slot.GetIndex());
}

TEST_F(ConciseHashTableTest, BuildTest) {
  const u32 num_tuples = 20;
  const u32 probe_length = 2;

  //
  // Table composed of single bucket with 64 bits
  //

  ConciseHashTable table(probe_length);
  table.SetSize(num_tuples);

  EXPECT_EQ(64u, table.Capacity());

  std::vector<ConciseHashTableSlot> inserted;

  for (u32 i = 1; i < 64; i += 2) {
    inserted.push_back(table.Insert(i));
  }

  table.Build();

  for (u32 i = 0; i < inserted.size(); i++) {
    EXPECT_EQ(i, table.NumOccupiedSlotsBefore(inserted[i]));
  }
}

TEST_F(ConciseHashTableTest, MultiGroupBuildTest) {
  const u32 num_tuples = 40;
  const u32 probe_length = 2;

  //
  // Table composed of single bucket with 128 bits
  //

  ConciseHashTable table(probe_length);
  table.SetSize(num_tuples);

  EXPECT_EQ(128u, table.Capacity());

  std::vector<ConciseHashTableSlot> inserted;

  for (u32 i = 1; i < 128; i += 2) {
    inserted.push_back(table.Insert(i));
  }

  table.Build();

  for (u32 i = 0; i < inserted.size(); i++) {
    EXPECT_EQ(i, table.NumOccupiedSlotsBefore(inserted[i]));
  }
}

TEST_F(ConciseHashTableTest, DISABLED_PerfTest) {
  const u32 num_tuples = 10000000;

  //
  // Build random input
  //

  std::vector<Tuple> tuples(num_tuples);

  {
    std::random_device random;
    auto genrand = [&random]() { return Tuple{random(), 0, 0, 0}; };
    std::generate(tuples.begin(), tuples.end(), genrand);
  }

  util::Timer<std::milli> timer;
  timer.Start();

  ConciseHashTable table;
  table.SetSize(num_tuples);

  for (const auto &tuple : tuples) {
    table.Insert(tuple.a);
  }

  table.Build();

  timer.Stop();

  auto mtps = (num_tuples / timer.elapsed()) / 1000.0;
  LOG_INFO("# Tuples    : {}", num_tuples)
  LOG_INFO("Table size  : {} KB", table.GetTotalMemoryUsage() / 1024.0);
  LOG_INFO("Insert+Build: {} ms ({:.2f} Mtps)", timer.elapsed(), mtps);
}

}  // namespace tpl::sql::test