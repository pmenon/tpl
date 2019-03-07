#include "tpl_test.h"

#include <deque>
#include <random>

#include "util/chunked_vector.h"

namespace tpl::util::test {

class GenericChunkedVectorTest : public TplTest {};

TEST_F(GenericChunkedVectorTest, InsertAndIndexTest) {
  const u32 num_elems = 10;

  util::Region tmp("tmp");
  ChunkedVectorT<u32> vec(&tmp);

  EXPECT_TRUE(vec.empty());

  for (u32 i = 0; i < num_elems; i++) {
    vec.push_back(i);
  }

  EXPECT_FALSE(vec.empty());
  EXPECT_EQ(num_elems, vec.size());
}

TEST_F(GenericChunkedVectorTest, RandomLookupTest) {
  const u32 num_elems = 1000;

  util::Region tmp("tmp");
  ChunkedVectorT<u32> vec(&tmp);

  EXPECT_TRUE(vec.empty());

  for (u32 i = 0; i < num_elems; i++) {
    vec.push_back(i);
  }

  // Do a bunch of random lookup
  std::random_device random;
  for (u32 i = 0; i < 1000; i++) {
    auto idx = random() % num_elems;
    EXPECT_EQ(idx, vec[idx]);
  }
}

TEST_F(GenericChunkedVectorTest, IterationTest) {
  util::Region tmp("tmp");
  ChunkedVectorT<u32> vec(&tmp);

  for (u32 i = 0; i < 10; i++) {
    vec.push_back(i);
  }

  {
    u32 i = 0;
    for (auto x : vec) {
      EXPECT_EQ(i++, x);
    }
  }
}

TEST_F(GenericChunkedVectorTest, DISABLED_PerfInsertTest) {
  auto stdvec_ms = Bench(3, []() {
    std::vector<u32> v;
    for (u32 i = 0; i < 10000000; i++) {
      v.push_back(i);
    }
  });

  auto stddeque_ms = Bench(3, []() {
    std::deque<u32> v;
    for (u32 i = 0; i < 10000000; i++) {
      v.push_back(i);
    }
  });

  auto chunked_ms = Bench(3, []() {
    util::Region tmp("tmp");
    ChunkedVectorT<u32> v(&tmp);
    for (u32 i = 0; i < 10000000; i++) {
      v.push_back(i);
    }
  });

  std::cout << "std::vector  : " << stdvec_ms << " ms" << std::endl;
  std::cout << "std::deque   : " << stddeque_ms << " ms" << std::endl;
  std::cout << "ChunkedVector: " << chunked_ms << " ms" << std::endl;
}

TEST_F(GenericChunkedVectorTest, DISABLED_PerfScanTest) {
  static const u32 num_elems = 10000000;

  std::vector<u32> stdvec;
  std::deque<u32> stddeque;
  util::Region tmp("tmp");
  ChunkedVectorT<u32> chunkedvec(&tmp);
  for (u32 i = 0; i < num_elems; i++) {
    stdvec.push_back(i);
    stddeque.push_back(i);
    chunkedvec.push_back(i);
  }

  auto stdvec_ms = Bench(10, [&stdvec]() {
    auto c = 0;
    for (auto x : stdvec) {
      c += x;
    }
    return c;
  });

  auto stddeque_ms = Bench(10, [&stddeque]() {
    auto c = 0;
    for (auto x : stddeque) {
      c += x;
    }
    return c;
  });

  auto chunked_ms = Bench(10, [&chunkedvec]() {
    u32 c = 0;
    for (auto x : chunkedvec) {
      c += x;
    }
    return c;
  });

  std::cout << "std::vector  : " << stdvec_ms << " ms" << std::endl;
  std::cout << "std::deque   : " << stddeque_ms << " ms" << std::endl;
  std::cout << "ChunkedVector: " << chunked_ms << " ms" << std::endl;
}

}  // namespace tpl::util::test
