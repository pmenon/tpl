#include "tpl_test.h"

#include <sys/mman.h>
#include <random>

#include "util/timer.h"
#include "util/vector_util.h"

namespace tpl::util::test {

class VectorUtilTest : public TplTest {
 private:
  struct AllocInfo {
    void *ptr;
    std::size_t size;

    AllocInfo(void *ptr, size_t size) : ptr(ptr), size(size) {}
  };

 public:
  ~VectorUtilTest() override {
    for (auto &alloc : allocations_) {
      FreeHuge(alloc.ptr, alloc.size);
    }
  }

  template <typename T>
  T *AllocArray(std::size_t num_elems) {
    std::size_t size = sizeof(T) * num_elems;
    void *ptr = MallocHuge(size);
    allocations_.emplace_back(ptr, size);
    return reinterpret_cast<T *>(ptr);
  }

 private:
  void *MallocHuge(std::size_t size) {
    void *p = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    madvise(p, size, MADV_HUGEPAGE);
    memset(p, 0, size);
    return p;
  }

  void FreeHuge(void *ptr, std::size_t size) { munmap(ptr, size); }

 private:
  std::vector<AllocInfo> allocations_;
};

TEST_F(VectorUtilTest, AccessTest) {
  {
    simd::Vec8 in(44);
    for (u32 i = 0; i < simd::Vec8::Size(); i++) {
      EXPECT_EQ(44, in[i]);
    }
  }

  {
    i32 base = 44;
    simd::Vec8 in(base, base + 1, base + 2, base + 3, base + 4, base + 5,
                  base + 6, base + 7);
    for (i32 i = 0; i < static_cast<i32>(simd::Vec8::Size()); i++) {
      EXPECT_EQ(base + i, in[i]);
    }
  }
}

TEST_F(VectorUtilTest, ComparisonsTest) {
  // Equality
  {
    simd::Vec8 in(10, 20, 30, 40, 50, 60, 70, 80);
    simd::Vec8 check = 40;
    simd::Vec8Mask mask = in == check;
    EXPECT_FALSE(mask[0]);
    EXPECT_FALSE(mask[1]);
    EXPECT_FALSE(mask[2]);
    EXPECT_TRUE(mask[3]);
    EXPECT_FALSE(mask[4]);
    EXPECT_FALSE(mask[5]);
    EXPECT_FALSE(mask[6]);
    EXPECT_FALSE(mask[7]);
  }

  // Greater Than
  {
    simd::Vec8 in(10, 20, 30, 40, 50, 60, 70, 80);
    simd::Vec8 check = 40;
    simd::Vec8Mask mask = in > check;
    EXPECT_FALSE(mask[0]);
    EXPECT_FALSE(mask[1]);
    EXPECT_FALSE(mask[2]);
    EXPECT_FALSE(mask[3]);
    EXPECT_TRUE(mask[4]);
    EXPECT_TRUE(mask[5]);
    EXPECT_TRUE(mask[6]);
    EXPECT_TRUE(mask[7]);
  }

  // Greater Than Equal
  {
    simd::Vec8 in(10, 20, 30, 40, 50, 60, 70, 80);
    simd::Vec8 check = 40;
    simd::Vec8Mask mask = in >= check;
    EXPECT_FALSE(mask[0]);
    EXPECT_FALSE(mask[1]);
    EXPECT_FALSE(mask[2]);
    EXPECT_TRUE(mask[3]);
    EXPECT_TRUE(mask[4]);
    EXPECT_TRUE(mask[5]);
    EXPECT_TRUE(mask[6]);
    EXPECT_TRUE(mask[7]);
  }

  // Less Than
  {
    simd::Vec8 in(10, 20, 30, 40, 50, 60, 70, 80);
    simd::Vec8 check = 40;
    simd::Vec8Mask mask = in < check;
    EXPECT_TRUE(mask[0]);
    EXPECT_TRUE(mask[1]);
    EXPECT_TRUE(mask[2]);
    EXPECT_FALSE(mask[3]);
    EXPECT_FALSE(mask[4]);
    EXPECT_FALSE(mask[5]);
    EXPECT_FALSE(mask[6]);
    EXPECT_FALSE(mask[7]);
  }

  // Less Than Equal
  {
    simd::Vec8 in(10, 20, 30, 40, 50, 60, 70, 80);
    simd::Vec8 check = 40;
    simd::Vec8Mask mask = (in <= check);
    EXPECT_TRUE(mask[0]);
    EXPECT_TRUE(mask[1]);
    EXPECT_TRUE(mask[2]);
    EXPECT_TRUE(mask[3]);
    EXPECT_FALSE(mask[4]);
    EXPECT_FALSE(mask[5]);
    EXPECT_FALSE(mask[6]);
    EXPECT_FALSE(mask[7]);
  }

  // Not Equal
  {
    simd::Vec8 in(10, 20, 30, 40, 50, 60, 70, 80);
    simd::Vec8 check = 40;
    simd::Vec8Mask mask = (in == check);
    EXPECT_FALSE(mask[0]);
    EXPECT_FALSE(mask[1]);
    EXPECT_FALSE(mask[2]);
    EXPECT_TRUE(mask[3]);
    EXPECT_FALSE(mask[4]);
    EXPECT_FALSE(mask[5]);
    EXPECT_FALSE(mask[6]);
    EXPECT_FALSE(mask[7]);
  }
}

TEST_F(VectorUtilTest, MaskToPositionTest) {
  simd::Vec8 vec(3, 0, 4, 1, 5, 2, 6, 2);
  simd::Vec8 val(2);

  simd::Vec8Mask mask = vec > val;

  u32 positions[8] = {0};

  u32 count = mask.ToPositions(positions, 0);
  EXPECT_EQ(4u, count);

  EXPECT_EQ(0u, positions[0]);
  EXPECT_EQ(2u, positions[1]);
  EXPECT_EQ(4u, positions[2]);
  EXPECT_EQ(6u, positions[3]);
}

template <typename T>
void SmallScale_NeedleTest(VectorUtilTest *test) {
  static_assert(std::is_integral_v<T>, "This only works for integral types");

  constexpr const u32 num_elems = 4400;
  constexpr const u32 chunk_size = 1024;
  constexpr const T needle = 16;

  T *arr = test->AllocArray<T>(num_elems);

  u32 actual_count = 0;

  // Load
  {
    std::mt19937 gen;
    std::uniform_int_distribution<T> dist(0, 100);
    for (u32 i = 0; i < num_elems; i++) {
      arr[i] = dist(gen);
      if (arr[i] == needle) {
        actual_count++;
      }
    }
  }

  u32 out[chunk_size] = {0};

  u32 count = 0;
  for (u32 offset = 0; offset < num_elems; offset += chunk_size) {
    auto size = std::min(chunk_size, num_elems - offset);
    auto found = VectorUtil::FilterEq(arr + offset, size, needle, out, nullptr);
    count += found;

    // Check each element in this vector
    for (u32 i = 0; i < found; i++) {
      EXPECT_EQ(needle, arr[offset + out[i]]);
    }
  }

  // Ensure total found through vector util matches what we generated
  EXPECT_EQ(actual_count, count);
};

template <typename T>
void SmallScale_MultiFilterTest(VectorUtilTest *test) {
  static_assert(std::is_integral_v<T>, "This only works for integral types");

  constexpr const u32 num_elems = 4400;
  constexpr const u32 chunk_size = 1024;
  constexpr const T needle_1 = 16;
  constexpr const T needle_2 = 10;

  T *arr_1 = test->AllocArray<T>(num_elems);
  T *arr_2 = test->AllocArray<T>(num_elems);

  u32 actual_count = 0;

  // Load
  {
    std::mt19937 gen;
    std::uniform_int_distribution<T> dist(0, 100);
    for (u32 i = 0; i < num_elems; i++) {
      arr_1[i] = dist(gen);
      arr_2[i] = dist(gen);
      if (arr_1[i] < needle_1 && arr_2[i] < needle_2) {
        actual_count++;
      }
    }
  }

  alignas(32) u32 out[chunk_size] = {0};
  alignas(32) u32 sel[chunk_size] = {0};

  u32 count = 0;
  for (u32 offset = 0; offset < num_elems; offset += chunk_size) {
    auto size = std::min(chunk_size, num_elems - offset);

    // Apply first filter
    auto found =
        VectorUtil::FilterLt(arr_1 + offset, size, needle_1, sel, nullptr);

    // Apply second filter
    found = VectorUtil::FilterLt(arr_2 + offset, found, needle_2, out, sel);

    // Count
    count += found;

    // Check each element in this vector
    for (u32 i = 0; i < found; i++) {
      EXPECT_LT(arr_1[offset + out[i]], needle_1);
      EXPECT_LT(arr_2[offset + out[i]], needle_2);
    }
  }

  // Ensure total found through vector util matches what we generated
  EXPECT_EQ(actual_count, count);
};

TEST_F(VectorUtilTest, SimpleFilterTest) {
  SmallScale_NeedleTest<i8>(this);
  SmallScale_NeedleTest<u8>(this);
  SmallScale_NeedleTest<i16>(this);
  SmallScale_NeedleTest<u16>(this);
  SmallScale_NeedleTest<i32>(this);
  SmallScale_NeedleTest<u32>(this);
  SmallScale_NeedleTest<i64>(this);
  SmallScale_NeedleTest<u64>(this);
}

TEST_F(VectorUtilTest, MultiFilterTest) {
  SmallScale_MultiFilterTest<i8>(this);
  SmallScale_MultiFilterTest<u8>(this);
  SmallScale_MultiFilterTest<i16>(this);
  SmallScale_MultiFilterTest<u16>(this);
  SmallScale_MultiFilterTest<i32>(this);
  SmallScale_MultiFilterTest<u32>(this);
  SmallScale_MultiFilterTest<i64>(this);
  SmallScale_MultiFilterTest<u64>(this);
}

TEST_F(VectorUtilTest, DISABLED_PerfSelectTest) {
  constexpr u32 num_elems = 128 * 1024u * 1024u;
  constexpr const u32 chunk_size = 1024;

  i32 *arr = AllocArray<i32>(num_elems);

  double load_time = 0.0;
  {
    ScopedTimer<std::milli> timer(&load_time);

    std::mt19937 gen;
    std::uniform_int_distribution<u32> dist(0, 100);
    for (u32 i = 0; i < num_elems; i++) {
      arr[i] = dist(gen);
    }
  }

  std::cout << "Load time: " << load_time << " ms" << std::endl;

  u32 out[chunk_size] = {0};

  for (i32 sel : {1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 99}) {
    u32 count = 0;
    double time = 0.0;

    {
      ScopedTimer<std::milli> timer(&time);
      for (u32 offset = 0; offset < num_elems; offset += chunk_size) {
        auto size = std::min(chunk_size, num_elems - offset);
        auto found =
            VectorUtil::FilterLt(arr + offset, size, sel, out, nullptr);
        count += found;
      }
    }

    std::cout << "Sel: " << ((double)sel / 100) << ", count: " << count
              << ", time: " << time << " ms" << std::endl;
  }
}

}  // namespace tpl::util::test