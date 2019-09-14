#include "sql/tuple_id_list.h"
#include "util/test_harness.h"

namespace tpl::sql {

class TupleIdListTest : public TplTest {};

TEST_F(TupleIdListTest, Add) {
  TupleIdList list;

  // Initially, no TIDs
  for (uint32_t tid = 0; tid < list.GetCapacity(); tid++) {
    EXPECT_FALSE(list.Contains(tid));
  }

  list.Add(3);
  EXPECT_TRUE(list.Contains(3));
}

TEST_F(TupleIdListTest, AddAll) {
  TupleIdList list;

  list.AddAll();
  for (uint32_t tid = 0; tid < list.GetCapacity(); tid++) {
    EXPECT_TRUE(list.Contains(tid));
  }
}

TEST_F(TupleIdListTest, AddRange) {
  TupleIdList list;

  list.AddRange(4, 6);

  EXPECT_EQ(2u, list.GetTupleCount());

  for (uint32_t tid = 0; tid < list.GetCapacity(); tid++) {
    EXPECT_EQ(4 <= tid && tid < 6, list.Contains(tid));
  }
}

TEST_F(TupleIdListTest, Enable) {
  TupleIdList list;
  list.Enable(4, false);
  EXPECT_FALSE(list.Contains(4));

  list.Enable(4, true);
  EXPECT_TRUE(list.Contains(4));

  list.Enable(4, false);
  EXPECT_FALSE(list.Contains(4));
}

TEST_F(TupleIdListTest, Remove) {
  TupleIdList list;
  list.Add(4);
  list.Add(9);
  EXPECT_TRUE(list.Contains(4));
  EXPECT_TRUE(list.Contains(9));

  list.Remove(4);
  EXPECT_FALSE(list.Contains(4));
  EXPECT_TRUE(list.Contains(9));

  // Try removing a TID that doesn't exist
  list.Remove(7);
  list.Remove(9);
  EXPECT_FALSE(list.Contains(4));
  EXPECT_FALSE(list.Contains(7));
  EXPECT_FALSE(list.Contains(9));
}

TEST_F(TupleIdListTest, Clear) {
  TupleIdList list;
  list.AddAll();
  list.Clear();
  for (uint32_t tid = 0; tid < list.GetCapacity(); tid++) {
    EXPECT_FALSE(list.Contains(tid));
  }
}

TEST_F(TupleIdListTest, IsFull) {
  TupleIdList list(10);
  EXPECT_FALSE(list.IsFull());

  list.Add(9);
  EXPECT_FALSE(list.IsFull());

  list.AddRange(0, 9);
  EXPECT_TRUE(list.IsFull());
}

TEST_F(TupleIdListTest, Empty) {
  TupleIdList list;
  EXPECT_TRUE(list.IsEmpty());

  list.Add(3);
  EXPECT_FALSE(list.IsEmpty());

  list.Clear();
  EXPECT_TRUE(list.IsEmpty());

  list.Add(4);
  EXPECT_FALSE(list.IsEmpty());
}

TEST_F(TupleIdListTest, Intersection) {
  TupleIdList list1, list2;

  // list1 = [1, 3, 4, 7, 8, 9];
  // list2 = [0, 2, 4, 8, 9];

  list1.Add(1);
  list1.AddRange(3, 5);
  list1.AddRange(7, 10);

  list2.Add(0);
  list2.Add(2);
  list2.Add(4);
  list2.AddRange(8, 10);

  // list1 = list1 ∩ list2 = [4, 8, 9]
  list1.IntersectWith(list2);

  for (uint32_t tid = 0; tid < 10; tid++) {
    EXPECT_EQ(tid == 4 || tid == 8 || tid == 9, list1.Contains(tid));
  }
}

TEST_F(TupleIdListTest, Union) {
  TupleIdList list1, list2;

  // list1 = [1, 3, 4, 7, 8, 9];
  // list2 = [0, 2, 4, 8, 9];

  list1.Add(1);
  list1.AddRange(3, 5);
  list1.AddRange(7, 10);

  list2.Add(0);
  list2.Add(2);
  list2.Add(4);
  list2.AddRange(8, 10);

  // list1 = list1 ∪ list2 = [0, 1, 2, 3, 4, 7, 8, 9]
  list1.UnionWith(list2);
  for (uint32_t tid = 0; tid < 10; tid++) {
    EXPECT_EQ(!(tid == 5 || tid == 6), list1.Contains(tid));
  }
}

TEST_F(TupleIdListTest, UnsetFrom) {
  TupleIdList list1, list2;

  // list1 = [1, 3, 4, 7, 8, 9];
  // list2 = [0, 2, 4, 8, 9];

  list1.Add(1);
  list1.AddRange(3, 5);
  list1.AddRange(7, 10);

  list2.Add(0);
  list2.Add(2);
  list2.Add(4);
  list2.AddRange(8, 10);

  // list1 = list1 - list2 = [1, 3, 7]
  list1.UnsetFrom(list2);
  for (uint32_t tid = 0; tid < 10; tid++) {
    EXPECT_EQ(tid == 1 || tid == 3 || tid == 7, list1.Contains(tid));
  }
}

TEST_F(TupleIdListTest, Selectivity) {
  TupleIdList list;

  EXPECT_FLOAT_EQ(0.0, list.ComputeSelectivity());

  list.Add(0);
  EXPECT_FLOAT_EQ(1.0 / list.GetCapacity(), list.ComputeSelectivity());

  list.AddRange(0, 5);
  EXPECT_FLOAT_EQ(5.0 / list.GetCapacity(), list.ComputeSelectivity());

  list.AddRange(0, 10);
  EXPECT_FLOAT_EQ(10.0 / list.GetCapacity(), list.ComputeSelectivity());
}

TEST_F(TupleIdListTest, ConvertToSelectionVector) {
  uint16_t sel[kDefaultVectorSize];

  TupleIdList list;
  list.Add(0);
  list.AddRange(3, 7);

  uint32_t n = list.AsSelectionVector(sel);
  EXPECT_EQ(5u, n);
  EXPECT_EQ(0u, sel[0]);
  EXPECT_EQ(3u, sel[1]);
  EXPECT_EQ(4u, sel[2]);
  EXPECT_EQ(5u, sel[3]);
  EXPECT_EQ(6u, sel[4]);
}

TEST_F(TupleIdListTest, BuildFromSelectionVector) {
  TupleIdList list(10);

  // Empty selection vector
  {
    sel_t sel[] = {};
    list.BuildFromSelectionVector(sel, sizeof(sel) / sizeof(sel[0]));
    EXPECT_TRUE(list.IsEmpty());
  }

  list.Clear();

  // Simple
  {
    sel_t sel[] = {0, 4, 9};
    list.BuildFromSelectionVector(sel, sizeof(sel) / sizeof(sel[0]));
    EXPECT_EQ(3u, list.GetTupleCount());
    EXPECT_TRUE(list.Contains(0));
    EXPECT_TRUE(list.Contains(4));
    EXPECT_TRUE(list.Contains(9));
  }

  list.Clear();

  // All
  {
    sel_t sel[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    list.BuildFromSelectionVector(sel, sizeof(sel) / sizeof(sel[0]));
    EXPECT_TRUE(list.IsFull());
  }
}

}  // namespace tpl::sql
