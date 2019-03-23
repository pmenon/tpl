#include "sql/sorter.h"
#include <algorithm>
#include <queue>
#include <random>
#include <vector>
#include "tpl_test.h"
#include "util/region.h"

#define TestAllSigned(FuncName, Args...) \
  FuncName<i8>(Args);                    \
  FuncName<i16>(Args);                   \
  FuncName<i32>(Args);                   \
  FuncName<i64>(Args);

#define TestAllUnsigned(FuncName, Args...) \
  FuncName<u8>(Args);                      \
  FuncName<u16>(Args);                     \
  FuncName<u32>(Args);                     \
  FuncName<u64>(Args);

#define TestAllIntegral(FuncName, Args...) \
  TestAllSigned(FuncName, Args) TestAllUnsigned(FuncName, Args)

namespace tpl::sql::test {

class SorterTest : public TplTest {
 public:
  std::default_random_engine generator_;
};

template <typename IntType, typename Random>
void TestSortRandomTupleSize(const u32 num_iters, const u32 max_elems,
                             Random *generator) {
  std::uniform_int_distribution<IntType> rng(
      std::numeric_limits<IntType>::min(), std::numeric_limits<IntType>::max());
  std::uniform_int_distribution<u32> rng_elems(std::numeric_limits<u32>::min(),
                                               std::numeric_limits<u32>::max());

  // We insert tuples of size IntType. It would be nice to std::memcmp, but
  // cmp_fn must be a function pointer which means the lambda cannot capture
  // tuple_size, so this is not possible without heavy macro abuse or passing in
  // something that we can sizeof(). Limiting ourselves to IntType should be
  // fine.
  const auto tuple_size = sizeof(IntType);
  auto cmp_fn = [](const byte *a, const byte *b) -> int {
    const auto val_a = *reinterpret_cast<const IntType *>(a);
    const auto val_b = *reinterpret_cast<const IntType *>(b);
    return val_a < val_b ? -1 : (val_a == val_b ? 0 : 1);
  };

  for (u32 curr_iter = 0; curr_iter < num_iters; curr_iter++) {
    // Test a random number of elements.
    const auto num_elems = (rng_elems(*generator) % max_elems) + 1;

    // Create a reference vector. This contains our real data, and sorter should
    // match it at the end.
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

template <typename IntType, typename Random>
void TestTopKRandomTupleSize(const u32 num_iters, const u32 max_elems,
                             Random *generator) {
  std::uniform_int_distribution<IntType> rng(
      std::numeric_limits<IntType>::min(), std::numeric_limits<IntType>::max());
  std::uniform_int_distribution<u32> rng_elems(std::numeric_limits<u32>::min(),
                                               std::numeric_limits<u32>::max());

  // We insert tuples of size IntType. It would be nice to std::memcmp, but
  // cmp_fn must be a function pointer which means the lambda cannot capture
  // tuple_size, so this is not possible without heavy macro abuse or passing in
  // something that we can sizeof(). Limiting ourselves to IntType should be
  // fine.
  const auto tuple_size = sizeof(IntType);
  auto cmp_fn = [](const byte *a, const byte *b) -> int {
    const auto val_a = *reinterpret_cast<const IntType *>(a);
    const auto val_b = *reinterpret_cast<const IntType *>(b);
    return val_a < val_b ? -1 : (val_a == val_b ? 0 : 1);
  };

  for (u32 curr_iter = 0; curr_iter < num_iters; curr_iter++) {
    // Test a random number of elements.
    const auto num_elems = (rng_elems(*generator) % max_elems) + 1;
    // For a random number of top k.
    const auto top_k =
        std::uniform_int_distribution<u32>(1, num_elems)(*generator);

    // Create a reference top-K min-heap. This contains our real data, and
    // sorter should match it at the end.
    std::priority_queue<IntType, std::vector<IntType>, std::greater<>>
        reference;

    // Create a sorter.
    util::Region tmp("tmp");
    sql::Sorter sorter(&tmp, cmp_fn, tuple_size);

    // Randomly create and insert elements to both sorter and reference.
    for (u32 i = 0; i < num_elems; i++) {
      const auto rand_data = rng(*generator);
      reference.push(rand_data);

      auto *elem =
          reinterpret_cast<IntType *>(sorter.AllocInputTupleTopK(top_k));
      *elem = rand_data;
      sorter.AllocInputTupleTopKFinish(top_k);
    }

    // Check that only the top k elements are left.
    sql::SorterIterator iter(&sorter);
    for (u32 i = 0; i < top_k; i++) {
      const auto ref_elem = reference.top();
      reference.pop();
      EXPECT_EQ(*reinterpret_cast<const IntType *>(*iter), ref_elem);
      ++iter;
    }
  }
}

TEST_F(SorterTest, SortTest) {
  const uint32_t num_iters = 200;
  const uint32_t max_elems = 10000;
  TestAllIntegral(TestSortRandomTupleSize, num_iters, max_elems, &generator_);
}

TEST_F(SorterTest, TopKTest) {
  const uint32_t num_iters = 200;
  const uint32_t max_elems = 10000;
  TestAllIntegral(TestTopKRandomTupleSize, num_iters, max_elems, &generator_);
}

}  // namespace tpl::sql::test