#include <algorithm>
#include <memory>
#include <random>
#include <ranges>
#include <utility>
#include <vector>

// Third-party IPS4O.
#include "ips4o/ips4o.hpp"

// Test.
#include "util/test_harness.h"

#include "util/chunked_vector.h"
#include "util/region.h"
#include "util/region_containers.h"

namespace tpl::util {

class ChunkedVectorTest : public TplTest {};

TEST_F(ChunkedVectorTest, InsertAndIndexTest) {
  const uint32_t num_elems = 10;

  ChunkedVectorT<uint32_t> vec;

  EXPECT_TRUE(vec.empty());

  for (uint32_t i = 0; i < num_elems; i++) {
    vec.push_back(i);
  }

  EXPECT_FALSE(vec.empty());
  EXPECT_EQ(num_elems, vec.size());
}

TEST_F(ChunkedVectorTest, RandomLookupTest) {
  const uint32_t num_elems = 1000;

  ChunkedVectorT<uint32_t> vec;

  EXPECT_TRUE(vec.empty());

  for (uint32_t i = 0; i < num_elems; i++) {
    vec.push_back(i);
  }

  // Do a bunch of random lookup
  std::random_device random = RandomDevice();
  for (uint32_t i = 0; i < 1000; i++) {
    auto idx = random() % num_elems;
    EXPECT_EQ(idx, vec[idx]);
  }
}

namespace {

void PopulateAndCheckIteration(const std::size_t s) {
  ChunkedVectorT<uint32_t> vec;

  for (uint32_t i = 0; i < s; i++) {
    vec.emplace_back(i);
  }

  // Forward iterate.
  uint32_t counter = 0;
  for (const auto x : vec) {
    EXPECT_EQ(counter++, x);
  }

  // Reverse iterate.
  for (auto iter = vec.rbegin(), end = vec.rend(); iter != end; ++iter) {
    EXPECT_EQ(--counter, *iter);
  }
}

}  // namespace

TEST_F(ChunkedVectorTest, IterationTest) {
  // Zero-sizes tables.
  PopulateAndCheckIteration(0);

  // Power-of-two tests for corner cases.
  for (uint32_t p = 0; p < 14; p++) {
    PopulateAndCheckIteration(1u << p);
  }

  // Some random prime-number sizes tables for fun.
  for (uint32_t size : {379, 2503, 10529, 134639, 1071223}) {
    PopulateAndCheckIteration(size);
  }
}

TEST_F(ChunkedVectorTest, PopBackTest) {
  ChunkedVectorT<uint32_t> vec;

  for (uint32_t i = 0; i < 10; i++) {
    vec.push_back(i);
  }

  vec.pop_back();
  EXPECT_EQ(9u, vec.size());

  vec.pop_back();
  EXPECT_EQ(8u, vec.size());

  for (uint32_t i = 0; i < vec.size(); i++) {
    EXPECT_EQ(i, vec[i]);
  }
}

TEST_F(ChunkedVectorTest, FrontBackTest) {
  ChunkedVectorT<uint32_t> vec;

  for (uint32_t i = 0; i < 10; i++) {
    vec.push_back(i);
  }

  EXPECT_EQ(0u, vec.front());
  EXPECT_EQ(9u, vec.back());

  vec.front() = 44;
  vec.back() = 100;

  EXPECT_EQ(44u, vec[0]);
  EXPECT_EQ(100u, vec[9]);

  vec.pop_back();
  EXPECT_EQ(8u, vec.back());
}

// Object that allocates 20-bytes of data. Used to ensure objects pushed into vector are destroyed.
class Simple {
 public:
  // Not thread-safe!
  static uint32_t count;

  explicit Simple(uint32_t _id) : id_(_id), ptr_(std::make_unique<char[]>(20)) { count++; }

  ~Simple() { count--; }

  uint32_t id() const noexcept { return id_; }

 private:
  uint32_t id_;
  std::unique_ptr<char[]> ptr_;
};

uint32_t Simple::count = 0;

TEST_F(ChunkedVectorTest, ClearTest) {
  ChunkedVectorT<Simple> v;
  v.emplace_back(1);
  v.emplace_back(2);
  v.emplace_back(3);

  EXPECT_EQ(3u, v.size());
  EXPECT_EQ(1u, v[0].id());
  EXPECT_EQ(2u, v[1].id());
  EXPECT_EQ(3u, v[2].id());

  v.clear();

  EXPECT_EQ(0u, v.size());

  v.emplace_back(10);
  v.emplace_back(11);
  v.emplace_back(12);

  EXPECT_EQ(3u, v.size());
  EXPECT_EQ(10u, v[0].id());
  EXPECT_EQ(11u, v[1].id());
  EXPECT_EQ(12u, v[2].id());
}

TEST_F(ChunkedVectorTest, ElementConstructDestructTest) {
  util::Region tmp("tmp");
  ChunkedVectorT<Simple> vec;

  for (uint32_t i = 0; i < 1000; i++) {
    vec.emplace_back(i);
  }
  EXPECT_EQ(1000u, Simple::count);

  vec.pop_back();
  EXPECT_EQ(999u, Simple::count);

  for (uint32_t i = 0; i < 999; i++) {
    vec.pop_back();
  }
  EXPECT_EQ(0u, Simple::count);
}

TEST_F(ChunkedVectorTest, MoveConstructorTest) {
  const uint32_t num_elems = 1000;

  // Populate vec1
  ChunkedVectorT<Simple> vec1;
  for (uint32_t i = 0; i < num_elems; i++) {
    vec1.emplace_back(i);
  }

  ChunkedVectorT<Simple> vec2(std::move(vec1));
  EXPECT_EQ(num_elems, vec2.size());
}

TEST_F(ChunkedVectorTest, AssignmentMoveTest) {
  const uint32_t num_elems = 1000;

  // Populate vec1
  ChunkedVectorT<Simple> vec1;
  for (uint32_t i = 0; i < num_elems; i++) {
    vec1.emplace_back(i);
  }

  ChunkedVectorT<Simple> vec2;
  EXPECT_EQ(0u, vec2.size());

  // Move vec1 into vec2
  vec2 = std::move(vec1);
  EXPECT_EQ(num_elems, vec2.size());
}

TEST_F(ChunkedVectorTest, ResizeTest) {
  static constexpr auto kSize1 = 97;  // This must be smalled than elements-per-chunk.
  static constexpr auto kSize2 = 1097;
  static constexpr auto kSize3 = 11;
  static constexpr auto kSize4 = 5;

  const auto explicit_size_check = [](auto &vec, auto size) {
    EXPECT_EQ(size, vec.size());
    uint64_t c = 0;
    for (UNUSED auto _ : vec) c++;
    EXPECT_EQ(size, c);
  };

  ChunkedVectorT<uint64_t> vec;

  // Case 1: corner-case of 0-element resize.
  vec.resize(0);
  explicit_size_check(vec, 0);

  // Case 2: resize+grow, but within same block.
  vec.resize(kSize1);
  explicit_size_check(vec, kSize1);

  // Case 3: resize+grow, but allocate blocks.
  vec.resize(kSize2);
  explicit_size_check(vec, kSize2);

  // Set elements in ascending order.
  for (uint32_t i = 0; i < kSize2; i++) vec[i] = i;

  // Validate contents.
  EXPECT_TRUE(std::ranges::equal(vec, std::views::iota(0, kSize2)));

  // Case 4: resize+shrink, deallocating blocks.
  vec.resize(kSize3);
  explicit_size_check(vec, kSize3);

  // Validate contents.
  EXPECT_TRUE(std::ranges::equal(vec, std::views::iota(0, kSize3)));

  // Overwrite elements.
  std::ranges::fill(vec, 44);

  // Case 5: resize+shrink, but within block.
  vec.resize(kSize4);
  explicit_size_check(vec, kSize4);

  // Validate contents.
  EXPECT_TRUE(std::ranges::all_of(vec, [](auto x) { return x == 44; }));

  // Case 6: Resize to zero from non-zero size.
  vec.resize(0);
  explicit_size_check(vec, 0);

  // Case 7: Append after resize 0.
  for (uint32_t i = 0; i < kSize2; i++) vec.push_back(i);
  explicit_size_check(vec, kSize2);
  EXPECT_TRUE(std::ranges::equal(vec, std::views::iota(0, kSize2)));
}

// Check that adding random integers to the iterator works.
TEST_F(ChunkedVectorTest, RandomIteratorAdditionTest) {
  const uint32_t num_elems = 1000;
  const uint32_t num_rolls = 1000000;  // Number of additions to make
  // Create vector
  ChunkedVectorT<uint32_t> vec;
  for (uint32_t i = 0; i < num_elems; i++) vec.push_back(i);

  // Jump at random offsets within the vector.
  std::default_random_engine generator;
  std::uniform_int_distribution<int> distribution(0, num_elems - 1);
  auto iter = vec.begin();
  int32_t prev_idx = 0;
  for (uint32_t i = 0; i < num_rolls; i++) {
    int32_t new_idx = distribution(generator);
    iter += (new_idx - prev_idx);
    ASSERT_EQ(*iter, static_cast<uint32_t>(new_idx));
    prev_idx = new_idx;
  }
}

// Check that subtracting random integers from the iterator works.
TEST_F(ChunkedVectorTest, RandomIteratorSubtractionTest) {
  const uint32_t num_elems = 1000;
  const uint32_t num_rolls = 1000000;  // number of subtractions to make
  // Create vector
  ChunkedVectorT<uint32_t> vec;
  for (uint32_t i = 0; i < num_elems; i++) vec.push_back(i);

  // Jump at random offsets within the vector
  std::default_random_engine generator;
  std::uniform_int_distribution<int32_t> distribution(0, num_elems - 1);
  auto iter = vec.begin();
  int32_t prev_idx = 0;
  for (uint32_t i = 0; i < num_rolls; i++) {
    int32_t new_idx = distribution(generator);
    iter -= (prev_idx - new_idx);
    ASSERT_EQ(*iter, static_cast<uint32_t>(new_idx));
    prev_idx = new_idx;
  }
}

// Check that all binary operators are working.
// <, <=, >, >=, ==, !=, -.
TEST_F(ChunkedVectorTest, RandomIteratorBinaryOpsTest) {
  const uint32_t num_elems = 1000;
  const uint32_t num_rolls = 1000000;  // Number of checks to make
  // Create vector
  util::Region tmp("tmp");
  ChunkedVectorT<uint32_t> vec;
  for (uint32_t i = 0; i < num_elems; i++) vec.push_back(i);

  // Perform binary operations on random indices.
  std::default_random_engine generator;
  std::uniform_int_distribution<int> distribution(0, num_elems - 1);
  for (uint32_t i = 0; i < num_rolls; i++) {
    int32_t idx1 = distribution(generator);
    int32_t idx2 = distribution(generator);
    auto iter1 = vec.begin() + idx1;
    auto iter2 = vec.begin() + idx2;
    ASSERT_EQ(idx1 < idx2, iter1 < iter2);
    ASSERT_EQ(idx1 > idx2, iter1 > iter2);
    ASSERT_EQ(idx1 <= idx2, iter1 <= iter2);
    ASSERT_EQ(idx1 >= idx2, iter1 >= iter2);
    ASSERT_EQ(idx1 == idx2, iter1 == iter2);
    ASSERT_EQ(idx1 != idx2, iter1 != iter2);
    ASSERT_EQ(idx1 - idx2, iter1 - iter2);
    ASSERT_EQ(idx2 - idx1, iter2 - iter1);
  }
}

// Check that pre-incrementing works.
TEST_F(ChunkedVectorTest, RandomIteratorPreIncrementTest) {
  const uint32_t num_elems = 512;

  // Generate random elements
  std::vector<uint32_t> std_vec;
  for (uint32_t i = 0; i < num_elems; i++) {
    std_vec.push_back(i);
  }

  std::default_random_engine generator;
  std::shuffle(std_vec.begin(), std_vec.end(), generator);

  // Create chunked vector
  util::Region tmp("tmp");
  ChunkedVectorT<uint32_t> vec;
  for (uint32_t i = 0; i < num_elems; i++) {
    vec.push_back(std_vec[i]);
  }

  auto iter = vec.begin();
  for (uint32_t i = 0; i < num_elems; i++) {
    ASSERT_EQ(*iter, std_vec[i]);
    ++iter;
  }
}

// Check that pre-decrementing works.
TEST_F(ChunkedVectorTest, RandomIteratorPreDecrementTest) {
  const uint32_t num_elems = 512;

  // Generate random elements
  std::vector<uint32_t> std_vec;
  for (uint32_t i = 0; i < num_elems; i++) {
    std_vec.push_back(i);
  }

  std::default_random_engine generator;
  std::shuffle(std_vec.begin(), std_vec.end(), generator);

  // Create chunked vector
  util::Region tmp("tmp");
  ChunkedVectorT<uint32_t> vec;
  for (uint32_t i = 0; i < num_elems; i++) {
    vec.push_back(std_vec[i]);
  }

  auto iter = vec.end();
  for (uint32_t i = 0; i < num_elems; i++) {
    --iter;
    ASSERT_EQ(*iter, std_vec[num_elems - i - 1]);
  }
}

TEST_F(ChunkedVectorTest, SortTest) {
  const uint32_t num_elems = 1000;
  util::Region tmp("tmp");
  ChunkedVectorT<uint32_t> vec;

  // Insert elements
  for (uint32_t i = 0; i < num_elems; i++) {
    vec.push_back(i);
  }

  // Shuffle
  std::default_random_engine generator;
  std::shuffle(vec.begin(), vec.end(), generator);

  // Sort
  ips4o::sort(vec.begin(), vec.end());

  // Verify
  for (uint32_t i = 0; i < num_elems; i++) {
    ASSERT_EQ(vec[i], i);
  }
}

}  // namespace tpl::util
