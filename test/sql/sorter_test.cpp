#include "tpl_test.h"
#include <vector>
#include <algorithm>
#include <random>
#include "util/region.h"
#include "sql/sorter.h"
#include <iostream>

namespace tpl::sql::test {
class SorterTest : public TplTest {};

TEST_F(SorterTest, SortTest) {
  // Generate random vector
  const i32 num_elems = 10000;
  std::vector<i32> V;
  for (i32 i = 0; i < num_elems; i++) V.push_back(i);
  std::default_random_engine generator;
  std::shuffle(V.begin(), V.end(), generator);

  // Create a sorter and insert elements in non-sorted order.
  util::Region tmp("tmp");
  auto comp_fn = [](const byte* a, const byte* b) -> int {
    return *reinterpret_cast<const i32*>(a) - *reinterpret_cast<const i32*>(b);
  };
  sql::Sorter sorter(&tmp, comp_fn, static_cast<u32>(sizeof(i32)));
  for (i32 i = 0; i < num_elems; i++) {
    byte* elem = sorter.AllocInputTuple();
    *reinterpret_cast<i32 *>(elem) = V[i];
  }
  // Sort
  sorter.Sort();

  // Check that elements are ordered.
  sql::SorterIterator iter(&sorter);
  for (i32 i = 0; i < num_elems; i++) {
    ASSERT_EQ(*reinterpret_cast<const i32*>(*iter), i);
    ++iter;
  }
}

TEST_F(SorterTest, TopKTest) {
  // Generate random vector and keep track of the top k elements
  const i32 num_elems = 10000;
  const i32 top_k = 1000;
  std::set<i32> topK_elems; // Stores the top k elements
  std::vector<i32> V;
  for (i32 i = 0; i < num_elems; i++) {
    V.push_back(i);
    if (i < top_k) topK_elems.emplace(i);
  }
  std::default_random_engine generator;
  std::shuffle(V.begin(), V.end(), generator);

  // Create a sorter and insert elements in non-sorted order.
  util::Region tmp("tmp");
  auto comp_fn = [](const byte* a, const byte* b) -> int {
    return *reinterpret_cast<const i32*>(a) - *reinterpret_cast<const i32*>(b);
  };
  sql::Sorter sorter(&tmp, comp_fn, static_cast<u32>(sizeof(i32)));
  for (i32 i = 0; i < num_elems; i++) {
    byte* elem = sorter.AllocInputTupleTopK(top_k);
    *reinterpret_cast<i32 *>(elem) = V[i];
    sorter.AllocInputTupleTopKFinish(top_k);
  }

  // Check that only the top l elements are left.
  sql::SorterIterator iter(&sorter);
  for (i32 i = 0; i < top_k; i++) {
    ASSERT_TRUE(topK_elems.count(*reinterpret_cast<const i32*>(*iter)));
    topK_elems.erase(*reinterpret_cast<const i32*>(*iter)); // Make sure each element is present once.
    ++iter;
  }
}

} // tpl::sql::test