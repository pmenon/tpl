#include <random>
#include <vector>

#include "util/bit_vector.h"
#include "util/test_harness.h"
#include "util/vector_util.h"

namespace tpl::util {

class VectorUtilTest : public TplTest {};

TEST_F(VectorUtilTest, ByteToSelectionVector) {
  constexpr uint32_t n = 14;
  uint8_t bytes[n] = {0xFF, 0x00, 0x00, 0xFF, 0x00, 0xFF, 0x00,
                      0xFF, 0xFF, 0x00, 0xFF, 0xFF, 0xFF, 0x00};
  sel_t sel[n];

  uint32_t size = VectorUtil::ByteVectorToSelectionVector(bytes, n, sel);
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

TEST_F(VectorUtilTest, BitToByteVector) {
  // Large enough to run through both vector-loop and scalar tail
  constexpr uint32_t num_bits = 97;

  // The input bit vector output byte vector
  BitVector bv(num_bits);
  uint8_t bytes[num_bits];

  // Set some random bits
  bv.Set(1);
  bv.Set(2);
  bv.Set(7);
  bv.Set(31);
  bv.Set(44);
  bv.Set(73);

  util::VectorUtil::BitVectorToByteVector(bv.GetWords(), bv.GetNumBits(), bytes);

  for (uint32_t i = 0; i < bv.GetNumBits(); i++) {
    EXPECT_EQ(bv[i], bytes[i] == 0xFF);
  }
}

TEST_F(VectorUtilTest, BitToSelectionVector) {
  // 126-bit vector and the output selection vector
  constexpr uint32_t num_bits = 126;
  BitVector bv(num_bits);
  sel_t sel[kDefaultVectorSize];

  // Set even bits
  for (uint32_t i = 0; i < num_bits; i++) {
    bv.Set(i, i % 2 == 0);
  }

  // Transform
  uint32_t size = util::VectorUtil::BitVectorToSelectionVector(bv.GetWords(), num_bits, sel);

  // Only 63 bits are set (remember there are only 126-bits)
  EXPECT_EQ(63u, size);

  // Ensure the indexes that are set are even
  for (uint32_t i = 0; i < size; i++) {
    EXPECT_EQ(0u, sel[i] % 2);
  }
}

TEST_F(VectorUtilTest, BitToSelectionVector_Sparse_vs_Dense) {
  for (uint32_t density : {0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100}) {
    // Create a bit vector with specific density
    BitVector bv(kDefaultVectorSize);
    std::random_device r;
    for (uint32_t i = 0; i < kDefaultVectorSize; i++) {
      if (r() % 100 < density) {
        bv[i] = true;
      }
    }

    sel_t sel_1[kDefaultVectorSize], sel_2[kDefaultVectorSize];

    // Ensure both sparse and dense implementations produce the same output
    const uint32_t size_1 =
        util::VectorUtil::BitVectorToSelectionVector_Sparse(bv.GetWords(), bv.GetNumBits(), sel_1);
    const uint32_t size_2 =
        util::VectorUtil::BitVectorToSelectionVector_Dense(bv.GetWords(), bv.GetNumBits(), sel_2);

    ASSERT_EQ(size_1, size_2);
    ASSERT_TRUE(std::equal(sel_1, sel_1 + size_1, sel_2, sel_2 + size_2));
  }
}

TEST_F(VectorUtilTest, DiffSelected) {
  sel_t input[kDefaultVectorSize] = {0, 2, 3, 5, 7, 9};
  sel_t output[kDefaultVectorSize];
  uint32_t out_count = VectorUtil::DiffSelected_Scalar(10, input, 6, output);
  EXPECT_EQ(4u, out_count);
  EXPECT_EQ(1u, output[0]);
  EXPECT_EQ(4u, output[1]);
  EXPECT_EQ(6u, output[2]);
  EXPECT_EQ(8u, output[3]);
}

TEST_F(VectorUtilTest, DiffSelectedWithScratchPad) {
  sel_t input[kDefaultVectorSize] = {2, 3, 5, 7, 9};
  sel_t output[kDefaultVectorSize];
  uint8_t scratch[kDefaultVectorSize];

  auto count = VectorUtil::DiffSelected_WithScratchPad(10, input, 5, output, scratch);
  EXPECT_EQ(5u, count);
  EXPECT_EQ(0u, output[0]);
  EXPECT_EQ(1u, output[1]);
  EXPECT_EQ(4u, output[2]);
  EXPECT_EQ(6u, output[3]);
  EXPECT_EQ(8u, output[4]);
}

TEST_F(VectorUtilTest, IntersectSelectionVectors) {
  sel_t a[] = {2, 3, 5, 7, 9};
  sel_t b[] = {1, 2, 4, 7, 8, 9, 10, 11};
  sel_t out[kDefaultVectorSize];

  auto out_count =
      VectorUtil::IntersectSelected(a, sizeof(a) / sizeof(a[0]), b, sizeof(b) / sizeof(b[0]), out);
  EXPECT_EQ(3u, out_count);
  EXPECT_EQ(2u, out[0]);
  EXPECT_EQ(7u, out[1]);
  EXPECT_EQ(9u, out[2]);

  // Reverse arguments, should still work
  out_count =
      VectorUtil::IntersectSelected(b, sizeof(b) / sizeof(b[0]), a, sizeof(a) / sizeof(a[0]), out);
  EXPECT_EQ(3u, out_count);
  EXPECT_EQ(2u, out[0]);
  EXPECT_EQ(7u, out[1]);
  EXPECT_EQ(9u, out[2]);

  // Empty arguments should work
  out_count = VectorUtil::IntersectSelected(nullptr, 0, b, sizeof(b) / sizeof(b[0]), out);
  EXPECT_EQ(0u, out_count);

  out_count = VectorUtil::IntersectSelected(b, sizeof(b) / sizeof(b[0]),
                                            static_cast<sel_t *>(nullptr), 0, out);
  EXPECT_EQ(0u, out_count);
}

#if 0
TEST_F(VectorUtilTest, DISABLED_PerfSelect) {
  constexpr uint32_t num_elems = 128 * 1024u * 1024u;
  constexpr const uint32_t chunk_size = 4096;

  std::vector<int32_t> arr(num_elems);

  double load_time = 0.0;
  {
    ScopedTimer<std::milli> timer(&load_time);

    std::mt19937 gen;
    std::uniform_int_distribution<uint32_t> dist(0, 100);
    for (uint32_t i = 0; i < num_elems; i++) {
      arr[i] = dist(gen);
    }
  }

  std::cout << "Load time: " << load_time << " ms" << std::endl;

  alignas(CACHELINE_SIZE) sel_t out[chunk_size] = {0};

  for (int32_t sel : {1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 99}) {
    uint32_t count = 0;
    double time = 0.0;

    {
      ScopedTimer<std::milli> timer(&time);
      for (uint32_t offset = 0; offset < num_elems; offset += chunk_size) {
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

TEST_F(VectorUtilTest, DISABLED_PerfInvertSel) {
  LOG_INFO("Sel., Scalar (ms), Vector (ms)");

  for (double sel :
       {0.01, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0}) {
    uint32_t num_selected = std::round(sel * kDefaultVectorSize);

    uint16_t sel_vec[kDefaultVectorSize] = {0};
    uint16_t normal_sel_out[kDefaultVectorSize] = {0};
    uint16_t full_sel_out[kDefaultVectorSize] = {0};

    uint32_t normal_count = 0, full_count = 0;

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
    for (uint32_t i = 0, idx = 0; i < kDefaultVectorSize; i++) {
      if (set[i]) {
        sel_vec[idx++] = i;
      }
    }

    auto normal_ms = Bench(5, [&]() {
      for (uint32_t i = 0; i < 500; i++) {
        normal_count = util::VectorUtil::DiffSelected_Scalar(
            kDefaultVectorSize, sel_vec, num_selected, normal_sel_out);
      }
    });

    auto full_ms = Bench(5, [&]() {
      for (uint32_t i = 0; i < 500; i++) {
        full_count = util::VectorUtil::DiffSelected_WithScratchpad(kDefaultVectorSize, sel_vec,
                                                    num_selected, full_sel_out);
      }
    });

    EXPECT_EQ(kDefaultVectorSize - num_selected, full_count);
    for (uint32_t i = 0; i < kDefaultVectorSize - num_selected; i++) {
      EXPECT_EQ(normal_sel_out[i], full_sel_out[i]);
    }
    LOG_INFO("{:>4.0f},{:>12.2f},{:>12.2f}", sel * 100.0, normal_ms, full_ms);
  }
}
#endif

}  // namespace tpl::util
