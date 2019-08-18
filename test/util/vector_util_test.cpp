#include <sys/mman.h>
#include <algorithm>
#include <random>
#include <utility>
#include <vector>

#include "tpl_test.h"  // NOLINT

#include "sql/memory_pool.h"
#include "util/bit_vector.h"
#include "util/fast_rand.h"
#include "util/timer.h"
#include "util/vector_util.h"

namespace tpl::util {

class VectorUtilTest : public TplTest {};

TEST(VectorUtilTest, Access) {
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

TEST(VectorUtilTest, Arithmetic) {
  // Addition
  {
    simd::Vec8 v(1, 2, 3, 4, 5, 6, 7, 8);
    simd::Vec8 add = v + simd::Vec8(10);
    for (i32 i = 0; i < 8; i++) {
      EXPECT_EQ(10 + (i + 1), add[i]);
    }

    // Update the original vector
    v += simd::Vec8(10);
    for (i32 i = 0; i < 8; i++) {
      EXPECT_EQ(10 + (i + 1), v[i]);
    }
  }

  // Subtraction
  {
    simd::Vec8 v(11, 12, 13, 14, 15, 16, 17, 18);
    simd::Vec8 sub = v - simd::Vec8(10);
    for (i32 i = 0; i < 8; i++) {
      EXPECT_EQ(i + 1, sub[i]);
    }

    // Update the original vector
    v -= simd::Vec8(10);
    for (i32 i = 0; i < 8; i++) {
      EXPECT_EQ(i + 1, v[i]);
    }
  }

  // Multiplication
  {
    simd::Vec8 v(1, 2, 3, 4, 5, 6, 7, 8);
    simd::Vec8 add = v * simd::Vec8(10);
    for (i32 i = 0; i < 8; i++) {
      EXPECT_EQ(10 * (i + 1), add[i]);
    }

    // Update the original vector
    v *= simd::Vec8(10);
    for (i32 i = 0; i < 8; i++) {
      EXPECT_EQ(10 * (i + 1), v[i]);
    }
  }
}

TEST(VectorUtilTest, BitwiseOperations) {
  // Left shift
  {
    simd::Vec8 v(1, 2, 3, 4, 5, 6, 7, 8);
    simd::Vec8 left_shift = v << 2;
    for (i32 i = 0; i < 8; i++) {
      EXPECT_EQ(v[i] * 4, left_shift[i]);
    }

    // Update the original vector
    v <<= 2;
    for (i32 i = 0; i < 8; i++) {
      EXPECT_EQ(left_shift[i], v[i]);
    }
  }

  // Right shift
  {
    simd::Vec8 v(4, 8, 16, 32, 64, 128, 256, 512);
    simd::Vec8 right_shift = v >> 1;
    for (i32 i = 0; i < 8; i++) {
      EXPECT_EQ(v[i] / 2, right_shift[i]);
    }

    // Update the original vector
    v >>= 1;
    for (i32 i = 0; i < 8; i++) {
      EXPECT_EQ(right_shift[i], v[i]);
    }
  }
}

TEST(VectorUtilTest, Comparisons) {
  // Equality
  {
    simd::Vec8 in(10, 20, 30, 40, 50, 60, 70, 80);
    auto check = simd::Vec8(40);
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
    auto check = simd::Vec8(40);
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
    auto check = simd::Vec8(40);
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
    auto check = simd::Vec8(40);
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
    auto check = simd::Vec8(40);
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
    auto check = simd::Vec8(40);
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

TEST(VectorUtilTest, MaskToPosition) {
  simd::Vec8 vec(3, 0, 4, 1, 5, 2, 6, 2);
  simd::Vec8 val(2);

  simd::Vec8Mask mask = vec > val;

  alignas(64) u16 positions[8] = {0};

  u32 count = mask.ToPositions(positions, 0);
  EXPECT_EQ(4u, count);

  EXPECT_EQ(0u, positions[0]);
  EXPECT_EQ(2u, positions[1]);
  EXPECT_EQ(4u, positions[2]);
  EXPECT_EQ(6u, positions[3]);
}

template <typename T>
void SmallScale_NeedleTest() {
  static_assert(std::is_integral_v<T>, "This only works for integral types");

  constexpr const u32 num_elems = 4400;
  constexpr const u32 chunk_size = 1024;
  constexpr const T needle = 16;

  std::vector<T> arr(num_elems);

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

  sel_t out[chunk_size] = {0};

  u32 count = 0;
  for (u32 offset = 0; offset < num_elems; offset += chunk_size) {
    auto size = std::min(chunk_size, num_elems - offset);
    auto found = VectorUtil::FilterEq(&arr[offset], size, needle, out);
    count += found;

    // Check each element in this vector
    for (u32 i = 0; i < found; i++) {
      EXPECT_EQ(needle, arr[offset + out[i]]);
    }
  }

  // Ensure total found through vector util matches what we generated
  EXPECT_EQ(actual_count, count);
}

TEST(VectorUtilTest, SimpleFilter) {
  SmallScale_NeedleTest<i8>();
  SmallScale_NeedleTest<u8>();
  SmallScale_NeedleTest<i16>();
  SmallScale_NeedleTest<u16>();
  SmallScale_NeedleTest<i32>();
  SmallScale_NeedleTest<u32>();
  SmallScale_NeedleTest<i64>();
  SmallScale_NeedleTest<u64>();
}

TEST(VectorUtilTest, VectorVectorFilter) {
  //
  // Test: two arrays, a1 and a2; a1 contains sequential numbers in the range
  //       [0, 1000), and s2 contains sequential numbers in range [1,1001).
  //       - a1 == a2, a1 >= a2, and a1 > a2 should return 0 results
  //       - a1 <= a2, a1 < a2, and a1 != a2 should return all results
  //

  const u32 num_elems = 10000;

  std::vector<i32> arr_1(num_elems);
  std::vector<i32> arr_2(num_elems);

  // Load
  std::iota(arr_1.begin(), arr_1.end(), 0);
  std::iota(arr_2.begin(), arr_2.end(), 1);

  alignas(CACHELINE_SIZE) sel_t out[kDefaultVectorSize] = {0};

#define CHECK(op, expected_count)                                            \
  {                                                                          \
    u32 count = 0;                                                           \
    for (u32 offset = 0; offset < num_elems; offset += kDefaultVectorSize) { \
      auto size = std::min(kDefaultVectorSize, num_elems - offset);          \
      auto found =                                                           \
          VectorUtil::Filter##op(&arr_1[offset], &arr_2[offset], size, out); \
      count += found;                                                        \
    }                                                                        \
    EXPECT_EQ(expected_count, count);                                        \
  }

  CHECK(Eq, 0u)
  CHECK(Ge, 0u)
  CHECK(Gt, 0u)
  CHECK(Le, num_elems)
  CHECK(Lt, num_elems)
  CHECK(Ne, num_elems)

#undef CHECK

  //
  // Test: fill a1 and a2 with random data. Verify filter with scalar versions.
  //

#define CHECK(vec_op, scalar_op)                                              \
  {                                                                           \
    std::random_device random;                                                \
    for (u32 idx = 0; idx < num_elems; idx++) {                               \
      arr_1[idx] = (random() % 100);                                          \
      arr_2[idx] = (random() % 100);                                          \
    }                                                                         \
    u32 vec_count = 0, scalar_count = 0;                                      \
    for (u32 offset = 0; offset < num_elems; offset += kDefaultVectorSize) {  \
      auto size = std::min(kDefaultVectorSize, num_elems - offset);           \
      /* Vector filter*/                                                      \
      auto found = VectorUtil::Filter##vec_op(&arr_1[offset], &arr_2[offset], \
                                              size, out);                     \
      vec_count += found;                                                     \
      /* Scalar filter */                                                     \
      for (u32 iter = offset, end = iter + size; iter != end; iter++) {       \
        scalar_count += arr_1[iter] scalar_op arr_2[iter];                    \
      }                                                                       \
    }                                                                         \
    EXPECT_EQ(scalar_count, vec_count);                                       \
  }

  CHECK(Eq, ==)
  CHECK(Ge, >=)
  CHECK(Gt, >)
  CHECK(Le, <=)
  CHECK(Lt, <)
  CHECK(Ne, !=)

#undef CHECK
}

TEST(VectorUtilTest, ByteToSelectionVector) {
  constexpr u32 n = 14;
  u8 bytes[n] = {0xFF, 0x00, 0x00, 0xFF, 0x00, 0xFF, 0x00,
                 0xFF, 0xFF, 0x00, 0xFF, 0xFF, 0xFF, 0x00};
  sel_t sel[n];

  u32 size = VectorUtil::ByteVectorToSelectionVector(n, bytes, sel);
  EXPECT_EQ(8u, size);
  EXPECT_EQ(0u, sel[0]);
  EXPECT_EQ(3u, sel[1]);
  EXPECT_EQ(5u, sel[2]);
  EXPECT_EQ(7u, sel[3]);
  EXPECT_EQ(8u, sel[4]);
  EXPECT_EQ(10u, sel[5]);
  EXPECT_EQ(11u, sel[6]);
  EXPECT_EQ(12u, sel[7]);
}

TEST(VectorUtilTest, BitToByteVector) {
  // Large enough to run through both vector-loop and scalar tail
  constexpr u32 num_bits = 97;

  // The input bit vector output byte vector
  BitVector bv(num_bits);
  u8 bytes[num_bits];

  // Set some random bits
  bv.Set(1);
  bv.Set(2);
  bv.Set(7);
  bv.Set(31);
  bv.Set(44);
  bv.Set(73);

  util::VectorUtil::BitVectorToByteVector(bv.num_bits(), bv.data_array(),
                                          bytes);

  for (u32 i = 0; i < bv.num_bits(); i++) {
    EXPECT_EQ(bv[i], bytes[i] == 0xFF);
  }
}

TEST(VectorUtilTest, BitToSelectionVector) {
  // 126-bit vector and the output selection vector
  constexpr u32 num_bits = 126;
  BitVector bv(num_bits);
  sel_t sel[kDefaultVectorSize];

  // Set even bits
  for (u32 i = 0; i < num_bits; i++) {
    bv.SetTo(i, i % 2 == 0);
  }

  // Transform
  u32 size = util::VectorUtil::BitVectorToSelectionVector(num_bits,
                                                          bv.data_array(), sel);

  // Only 63 bits are set (remember there are only 126-bits)
  EXPECT_EQ(63u, size);

  // Ensure the indexes that are set are even
  for (u32 i = 0; i < size; i++) {
    EXPECT_TRUE(sel[i] % 2 == 0);
  }
}

TEST(VectorUtilTest, DiffSelected) {
  sel_t input[kDefaultVectorSize] = {0, 2, 3, 5, 7, 9};
  sel_t output[kDefaultVectorSize];
  u32 out_count = VectorUtil::DiffSelected_Scalar(10, input, 6, output);
  EXPECT_EQ(4u, out_count);
  EXPECT_EQ(1u, output[0]);
  EXPECT_EQ(4u, output[1]);
  EXPECT_EQ(6u, output[2]);
  EXPECT_EQ(8u, output[3]);
}

TEST(VectorUtilTest, DiffSelectedWithScratcPad) {
  sel_t input[kDefaultVectorSize] = {2, 3, 5, 7, 9};
  sel_t output[kDefaultVectorSize];
  u8 scratchpad[kDefaultVectorSize];

  auto count =
      VectorUtil::DiffSelected_WithScratchpad(10, input, 5, output, scratchpad);
  EXPECT_EQ(5u, count);
  EXPECT_EQ(0u, output[0]);
  EXPECT_EQ(1u, output[1]);
  EXPECT_EQ(4u, output[2]);
  EXPECT_EQ(6u, output[3]);
  EXPECT_EQ(8u, output[4]);
}

TEST(VectorUtilTest, DISABLED_PerfSelect) {
  constexpr u32 num_elems = 128 * 1024u * 1024u;
  constexpr const u32 chunk_size = 4096;

  std::vector<i32> arr(num_elems);

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

  alignas(CACHELINE_SIZE) sel_t out[chunk_size] = {0};

  for (i32 sel : {1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 99}) {
    u32 count = 0;
    double time = 0.0;

    {
      ScopedTimer<std::milli> timer(&time);
      for (u32 offset = 0; offset < num_elems; offset += chunk_size) {
        auto size = std::min(chunk_size, num_elems - offset);
        auto found = VectorUtil::FilterLt(&arr[offset], size, sel, out);
        count += found;
      }
    }

    std::cout << "Sel: " << (static_cast<double>(sel) / 100)
              << ", count: " << count << ", time: " << time << " ms"
              << std::endl;
  }
}

#if 0
TEST_F(VectorUtilTest, DISABLED_PerfInvertSel) {
  LOG_INFO("Sel., Scalar (ms), Vector (ms)");

  for (f64 sel :
       {0.01, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0}) {
    u32 num_selected = std::round(sel * kDefaultVectorSize);

    u16 sel_vec[kDefaultVectorSize] = {0};
    u16 normal_sel_out[kDefaultVectorSize] = {0};
    u16 full_sel_out[kDefaultVectorSize] = {0};

    u32 normal_count = 0, full_count = 0;

    util::FastRand rand;

    // Compute selection vector
    std::bitset<kDefaultVectorSize> set;
    while (set.count() < num_selected) {
      auto idx = rand.Next() % kDefaultVectorSize;
      if (!set[idx]) {
        set[idx] = true;
      }
    }

    // Populate selection vector
    for (u32 i = 0, idx = 0; i < kDefaultVectorSize; i++) {
      if (set[i]) {
        sel_vec[idx++] = i;
      }
    }

    auto normal_ms = Bench(5, [&]() {
      for (u32 i = 0; i < 500; i++) {
        normal_count = util::VectorUtil::DiffSelected_Scalar(
            kDefaultVectorSize, sel_vec, num_selected, normal_sel_out);
      }
    });

    auto full_ms = Bench(5, [&]() {
      for (u32 i = 0; i < 500; i++) {
        full_count = util::VectorUtil::DiffSelected_WithScratchpad(kDefaultVectorSize, sel_vec,
                                                    num_selected, full_sel_out);
      }
    });

    EXPECT_EQ(kDefaultVectorSize - num_selected, full_count);
    for (u32 i = 0; i < kDefaultVectorSize - num_selected; i++) {
      EXPECT_EQ(normal_sel_out[i], full_sel_out[i]);
    }
    LOG_INFO("{:>4.0f},{:>12.2f},{:>12.2f}", sel * 100.0, normal_ms, full_ms);
  }
}
#endif

}  // namespace tpl::util
