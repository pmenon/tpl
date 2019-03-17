#include "tpl_test.h"
#include <algorithm>
#include <random>
#include <vector>
#include "sql/sorter.h"
#include "util/region.h"

#define TestAllSigned(FuncName, Args...) \
  FuncName<i8>(Args); \
  FuncName<i16>(Args); \
  FuncName<i32>(Args); \
  FuncName<i64>(Args);

#define TestAllUnsigned(FuncName, Args...) \
  FuncName<u8>(Args); \
  FuncName<u16>(Args); \
  FuncName<u32>(Args); \
  FuncName<u64>(Args);

#define TestAllIntegral(FuncName, Args...) \
  TestAllSigned(FuncName, Args) \
  TestAllUnsigned(FuncName, Args)

namespace tpl::sql::test {

class SorterTest : public TplTest {
 public:
  std::default_random_engine generator_;
};

template <typename IntType, typename Random>
void TestSortRandomTupleSize(const u32 num_iters, const u32 max_elems, Random *generator) {
  std::uniform_int_distribution<IntType> rng(std::numeric_limits<IntType>::min(), std::numeric_limits<IntType>::max());

  // We insert tuples of size IntType. It would be nice to std::memcmp, but cmp_fn must be a function pointer
  // which means the lambda cannot capture tuple_size, so this is not possible without heavy macro abuse or
  // passing in something that we can sizeof(). Limiting ourselves to IntType should be fine.
  const auto tuple_size = sizeof(IntType);
  auto cmp_fn = [](const byte *a, const byte *b) -> int {
    // Explicit comparison to avoid overflows
    IntType a_val = *reinterpret_cast<const IntType *>(a);
    IntType b_val = *reinterpret_cast<const IntType *>(b);
    if (a_val < b_val) {
      return -1;
    } else if (a_val > b_val) {
      return 1;
    }
    return 0;
  };

  for (u32 curr_iter = 0; curr_iter < num_iters; curr_iter++) {
    // Test a random number of elements.
    const auto num_elems = (std::abs(rng(*generator)) % max_elems) + 1;
    // Create a reference vector. This contains our real data, and sorter should match it at the end.
    std::vector<IntType> reference;
    reference.reserve(num_elems);

    // Create a sorter.
    util::Region tmp("tmp");
    sql::Sorter sorter(&tmp, cmp_fn, tuple_size);

    // Randomly create and insert elements to both sorter and reference.
    for (u32 i = 0; i < num_elems; i++) {
      const auto rand_data = rng(*generator);
      reference.emplace_back(rand_data);
      auto *elem = reinterpret_cast<IntType *>(sorter.AllocInputTuple());
      *elem = rand_data;
    }

    // Sort the data from small to big.
    std::sort(reference.begin(), reference.end());
    sorter.Sort();

    // Check that the elements are in the same order.
    sql::SorterIterator iter(&sorter);
    for (u32 i = 0; i < num_elems; i++) {
      EXPECT_EQ(*reinterpret_cast<const IntType *>(*iter), reference[i]);
      ++iter;
    }
  }
}

// NOLINTNEXTLINE
TEST_F(SorterTest, SortTest) {
  const uint32_t num_iters = 20;
  const uint32_t max_elems = 10000;
  TestSortRandomTupleSize<i32>(num_iters, max_elems, &generator_);
//  TestAllSigned(TestSortRandomTupleSize, num_iters, max_elems, &generator_);
//  TestAllUnsigned(TestSortRandomTupleSize, num_iters, max_elems, &generator_);
}

// NOLINTNEXTLINE
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