#include "tpl_test.h"

#include <deque>
#include <random>

#include "util/chunked_vector.h"

namespace tpl::util::test {

class GenericChunkedVectorTest : public TplTest {};

// A typed chunked vector. We use this to make the tests easier to understand.
template <typename T>
class ChunkedVectorT : public ChunkedVector {
 public:
  explicit ChunkedVectorT(util::Region *region) noexcept
      : ChunkedVector(region, sizeof(T)) {}

  // -------------------------------------------------------
  // Iterators
  // -------------------------------------------------------

  class Iterator {
   public:
    explicit Iterator(ChunkedVectorIterator iter) : iter_(iter) {}

    T &operator*() const noexcept { return *reinterpret_cast<T *>(*iter_); }

    Iterator &operator++() noexcept {
      ++iter_;
      return *this;
    }

    bool operator==(const Iterator &that) const { return iter_ == that.iter_; }

    bool operator!=(const Iterator &that) const { return !(*this == that); }

   private:
    ChunkedVectorIterator iter_;
  };

  Iterator begin() { return Iterator(ChunkedVector::begin()); }
  Iterator end() { return Iterator(ChunkedVector::end()); }

  // -------------------------------------------------------
  // Element access
  // -------------------------------------------------------

  T &operator[](std::size_t idx) {
    auto *ptr = ChunkedVector::operator[](idx);
    return *reinterpret_cast<T *>(ptr);
  }
  const T &operator[](std::size_t idx) const {
    auto *ptr = ChunkedVector::operator[](idx);
    return *reinterpret_cast<T *>(ptr);
  }

  // -------------------------------------------------------
  // Modifiers
  // -------------------------------------------------------

  template <class... Args>
  void emplace_back(Args &&... args) {
    auto *new_elem = append();
    *reinterpret_cast<T *>(new_elem) =
        std::move(T(std::forward<Args>(args)...));
  }

  void push_back(const T &elem) {
    auto *new_elem = append();
    *reinterpret_cast<T *>(new_elem) = elem;
  }

  void push_back(T &&elem) {
    auto *new_elem = append();
    *reinterpret_cast<T *>(new_elem) = std::move(elem);
  }
};

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
