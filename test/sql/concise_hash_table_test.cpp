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
  const u32 probe_length = 1;

  ConciseHashTable table(probe_length);
  table.SetSize(num_tuples);

  // Check minimum capacity is enforced
  EXPECT_EQ(64u, table.capacity());

  // 0 should go into the zero-th slot
  auto _0_slot = table.Insert(0);
  EXPECT_EQ(0u, _0_slot.GetSlotIndex());

  // 1 should go into the second slot
  auto _1_slot = table.Insert(1);
  EXPECT_EQ(1u, _1_slot.GetSlotIndex());

  // 2 should into the third slot
  auto _2_slot = table.Insert(2);
  EXPECT_EQ(2u, _2_slot.GetSlotIndex());
}

TEST_F(ConciseHashTableTest, InsertOverflowTest) {
  const u32 num_tuples = 20;
  const u32 probe_length = 1;

  //
  // Create a CHT with one slot group, 64 slots total
  //

  ConciseHashTable table(probe_length);
  table.SetSize(num_tuples);

  EXPECT_EQ(64u, table.capacity());

  // 33 should go into the 33rd slot
  auto _33_slot = table.Insert(33);
  EXPECT_EQ(33u, _33_slot.GetSlotIndex());

  // A second 33 should go into the 34th slot
  auto _33_v2_slot = table.Insert(33);
  EXPECT_EQ(34u, _33_v2_slot.GetSlotIndex());

  // A fourth 33 should overflow since probe length is 2
  auto _33_v3_slot = table.Insert(33);
  EXPECT_EQ(34u, _33_v3_slot.GetSlotIndex());

  // 34 should go into the 35th slot (since the 34th is occupied by 33 v2)
  auto _34_slot = table.Insert(34);
  EXPECT_EQ(35u, _34_slot.GetSlotIndex());
}

TEST_F(ConciseHashTableTest, MultiGroupInsertTest) {
  const u32 num_tuples = 100;
  const u32 probe_length = 1;

  //
  // Create a CHT with four slot-groups, each having 64 slots
  //

  ConciseHashTable table(probe_length);
  table.SetSize(num_tuples);

  // 4 groups * 64 slots-per-group = 256 slots
  EXPECT_EQ(256u, table.capacity());

  // 33 goes in the first group, in the 33rd slot
  auto _33_slot = table.Insert(33);
  EXPECT_EQ(33u, _33_slot.GetSlotIndex());

  // 97 (64+33) goes in the second group in the 33rd group bit, but the 97th
  // overall slot
  auto _97_slot = table.Insert(97);
  EXPECT_EQ(97u, _97_slot.GetSlotIndex());

  // 161 (64+64+33) goes in the third group in the 33rd group bit, but the 130th
  // overall slot
  auto _161_slot = table.Insert(161);
  EXPECT_EQ(161u, _161_slot.GetSlotIndex());

  // 225 (64+64+64+33) goes in the fourth (and last) group, in the 33rd group
  // bit, but the 225th overall slot
  auto _225_slot = table.Insert(225);
  EXPECT_EQ(225u, _225_slot.GetSlotIndex());

  // 289 (64+64+64+64+33) cycles back into the **FIRST** group, in the 34th
  // group bit since the 33rd is occupied by the first insert
  auto _289_slot = table.Insert(289);
  EXPECT_EQ(34u, _289_slot.GetSlotIndex());

  // Inserting 289 again should overflow: 33rd slot is occupied by '33', 34th
  // slot is occupied by the first '289'. With probe length of 1, the second
  // '289' will overflow.
  auto _289_v2_slot = table.Insert(289);
  EXPECT_EQ(34u, _289_v2_slot.GetSlotIndex());
}

TEST_F(ConciseHashTableTest, CornerCaseTest) {
  const u32 num_tuples = 20;
  const u32 probe_length = 4;

  ConciseHashTable table(probe_length);
  table.SetSize(num_tuples);

  EXPECT_EQ(64u, table.capacity());

  // 63 should go into the 63rd slot
  auto _63_slot = table.Insert(63);
  EXPECT_EQ(63u, _63_slot.GetSlotIndex());

  // A second 63 should overflow even though the probe length is 4. Probing
  // doesn't cross slot group boundaries.
  auto _63_v2_slot = table.Insert(63);
  EXPECT_EQ(63u, _63_v2_slot.GetSlotIndex());

  // 62 should go into the 62nd slot
  auto _62_slot = table.Insert(62);
  EXPECT_EQ(62u, _62_slot.GetSlotIndex());

  // A second 62 should overflow onto the 63rd slot since probing doesn't cross
  // slot group boundaries
  auto _62_v2_slot = table.Insert(62);
  EXPECT_EQ(63u, _62_v2_slot.GetSlotIndex());
}

TEST_F(ConciseHashTableTest, BuildTest) {
  const u32 num_tuples = 20;
  const u32 probe_length = 2;

  //
  // Table composed of single group with 64 bits
  //

  ConciseHashTable table(probe_length);
  table.SetSize(num_tuples);

  EXPECT_EQ(64u, table.capacity());

  std::vector<ConciseHashTableSlot> inserted;

  for (u32 i = 1; i < 64; i += 2) {
    inserted.push_back(table.Insert(i));
  }

  table.Build();

  for (u32 i = 0; i < inserted.size(); i++) {
    EXPECT_EQ(i, table.NumFilledSlotsBefore(inserted[i]));
  }
}

TEST_F(ConciseHashTableTest, MultiGroupBuildTest) {
  const u32 num_tuples = 40;
  const u32 probe_length = 2;

  //
  // Table composed of two groups totaling 128 bits
  //

  ConciseHashTable table(probe_length);
  table.SetSize(num_tuples);

  EXPECT_EQ(128u, table.capacity());

  std::vector<ConciseHashTableSlot> inserted;

  for (u32 i = 1; i < 128; i += 2) {
    inserted.push_back(table.Insert(i));
  }

  table.Build();

  for (u32 i = 0; i < inserted.size(); i++) {
    EXPECT_EQ(i, table.NumFilledSlotsBefore(inserted[i]));
  }
}

}  // namespace tpl::sql::test