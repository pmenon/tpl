#include <random>
#include <unordered_set>
#include <vector>

#include "tpl_test.h"  // NOLINT

#include "util/bit_vector.h"

namespace tpl::util {

namespace {

// Verify that the callback returns true for all set bit indexes
template <typename BitVectorType, typename F>
::testing::AssertionResult Verify(BitVectorType &bv, F &&f) {
  for (u32 i = 0; i < bv.num_bits(); i++) {
    if (bv[i] && !f(i)) {
      return ::testing::AssertionFailure()
             << "bv[" << i << "]=true, but expected to be false";
    }
  }
  return ::testing::AssertionSuccess();
}

// Verify that only specific bits are set
template <typename BitVectorType>
::testing::AssertionResult Verify(const BitVectorType &bv,
                                  std::initializer_list<u32> idxs) {
  std::unordered_set<u32> positions(idxs);

  u32 num_set = 0;
  auto success = Verify(bv, [&](auto idx) {
    num_set++;
    return positions.count(idx) != 0;
  });

  if (!success) {
    return success;
  }

  if (num_set != positions.size()) {
    return ::testing::AssertionFailure()
           << "Unmatched # set bits. Actual: " << num_set
           << ", Expected: " << positions.size();
  }

  return ::testing::AssertionSuccess();
}

}  // namespace

BitVector Make(std::initializer_list<u32> vals) {
  BitVector bv(vals.size());
  std::for_each(vals.begin(), vals.end(),
                [&, i = 0](auto &bval) mutable { bv.SetTo(i++, bval); });
  return bv;
}

TEST(BitVectorTest, BitVectorSize) {
  // We need at least one word for 1 bit
  EXPECT_EQ(1u, BitVector::NumNeededWords(1));

  // We still need one 64-bit word for 63 and 64 bits
  EXPECT_EQ(1u, BitVector::NumNeededWords(63));
  EXPECT_EQ(1u, BitVector::NumNeededWords(64));

  // For 33 elements, we need two 32-bit words
  EXPECT_EQ(2u, BitVector::NumNeededWords(65));
}

TEST(BitVectorTest, Init) {
  BitVector bv(100);
  EXPECT_EQ(2u, bv.num_words());
  EXPECT_EQ(100u, bv.num_bits());

  for (u32 i = 0; i < bv.num_bits(); i++) {
    EXPECT_FALSE(bv[i]);
  }
}

TEST(BitVectorTest, Set) {
  BitVector bv(10);

  bv.Set(2);
  EXPECT_TRUE(Verify(bv, {2}));

  bv.Set(0);
  EXPECT_TRUE(Verify(bv, {0, 2}));

  bv.Set(7);
  EXPECT_TRUE(Verify(bv, {0, 2, 7}));

  bv.SetAll();
  EXPECT_TRUE(Verify(bv, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
}

TEST(BitVectorTest, SetTo) {
  BitVector bv(10);

  bv.SetTo(2, true);
  EXPECT_TRUE(Verify(bv, {2}));

  // Repeats work
  bv.SetTo(2, true);
  EXPECT_TRUE(Verify(bv, {2}));

  bv.SetTo(2, false);
  EXPECT_TRUE(Verify(bv, {}));

  bv.SetTo(3, true);
  EXPECT_TRUE(Verify(bv, {3}));

  bv.SetTo(2, true);
  EXPECT_TRUE(Verify(bv, {2, 3}));

  bv.SetAll();
  EXPECT_TRUE(Verify(bv, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
}

TEST(BitVectorTest, SetWord) {
  BitVector bv(10);

  // bv[0] = true
  bv.SetWord(0, 0x00000001);

  EXPECT_TRUE(bv.Test(0));
  for (u32 i = 1; i < bv.num_bits(); i++) {
    EXPECT_FALSE(bv.Test(i));
  }

  // bv[9] = true
  bv.SetWord(0, 0x00000200);
  EXPECT_TRUE(bv.Test(9));
  for (u32 i = 0; i < bv.num_bits() - 1; i++) {
    EXPECT_FALSE(bv.Test(i));
  }

  // Try setting out of bound bits
  bv.SetWord(0, 0xfffff800);
  EXPECT_FALSE(bv.Any());

  // Even bits only
  bv.SetWord(0, 0x55555555);
  for (u32 i = 0; i < bv.num_bits(); i++) {
    EXPECT_EQ(i % 2 == 0, bv.Test(i));
  }
}

TEST(BitVectorTest, Unset) {
  BitVector bv(10);

  // Set every 3rd bit
  for (u32 i = 0; i < bv.num_bits(); i++) {
    if (i % 3 == 0) {
      bv.Set(i);
    }
  }
  EXPECT_TRUE(Verify(bv, {0, 3, 6, 9}));

  bv.Unset(3);
  EXPECT_TRUE(Verify(bv, {0, 6, 9}));

  bv.Unset(9);
  EXPECT_TRUE(Verify(bv, {0, 6}));

  bv.UnsetAll();
  EXPECT_TRUE(Verify(bv, {}));
}

TEST(BitVectorTest, TestAndSet) {
  // Simple set
  {
    BitVector bv(100);
    EXPECT_FALSE(bv.Test(20));
    bv.Set(20);
    EXPECT_TRUE(bv.Test(20));
  }

  // Set even bits only, then check
  {
    BitVector bv(100);
    for (u32 i = 0; i < bv.num_bits(); i++) {
      if (i % 2 == 0) {
        bv.Set(i);
      }
    }
    for (u32 i = 0; i < bv.num_bits(); i++) {
      EXPECT_EQ(i % 2 == 0, bv[i]);
    }
  }
}

TEST(BitVectorTest, Flip) {
  BitVector bv(10);

  // Set even bits
  for (u32 i = 0; i < bv.num_bits(); i++) {
    if (i % 2 == 0) {
      bv.Set(i);
    }
  }
  EXPECT_TRUE(Verify(bv, {0, 2, 4, 6, 8}));

  bv.Flip(0);
  EXPECT_TRUE(Verify(bv, {2, 4, 6, 8}));

  bv.Flip(8);
  EXPECT_TRUE(Verify(bv, {2, 4, 6}));

  bv.FlipAll();
  EXPECT_TRUE(Verify(bv, {0, 1, 3, 5, 7, 8, 9}));
}

TEST(BitVectorTest, Any) {
  BitVector bv(100);
  EXPECT_FALSE(bv.Any());

  bv.UnsetAll();
  EXPECT_FALSE(bv.Any());

  bv.Set(64);
  EXPECT_TRUE(bv.Any());
}

TEST(BitVectorTest, All) {
  BitVector bv(100);
  EXPECT_FALSE(bv.All());

  bv.UnsetAll();
  EXPECT_FALSE(bv.All());

  bv.Set(64);
  EXPECT_FALSE(bv.All());

  // Set all but one
  for (u32 i = 0; i < bv.num_bits() - 1; i++) {
    bv.Set(i);
  }
  EXPECT_FALSE(bv.All());

  // Set last manually
  bv.Set(99);
  EXPECT_TRUE(bv.All());

  bv.UnsetAll();
  bv.SetAll();
  EXPECT_TRUE(bv.All());
}

TEST(BitVectorTest, None) {
  BitVector bv(100);
  EXPECT_TRUE(bv.None());

  bv.UnsetAll();
  EXPECT_TRUE(bv.None());

  bv.Set(64);
  EXPECT_FALSE(bv.None());

  bv.Unset(64);
  EXPECT_TRUE(bv.None());

  bv.SetAll();
  EXPECT_FALSE(bv.None());

  bv.UnsetAll();
  EXPECT_TRUE(bv.None());
}

TEST(BitVectorTest, SetFromBytes) {
  // Simple
  {
    BitVector bv(10);

    // Set first last bit only
    bv.SetFromBytes(std::vector<u8>{0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0xff}.data(),
                    10);
    EXPECT_TRUE(Verify(bv, {0, 9}));

    // Set odd bits
    bv.SetFromBytes(
        std::vector<u8>{0, 0xff, 0, 0xff, 0, 0xff, 0, 0xff, 0, 0xff}.data(),
        10);
    EXPECT_TRUE(Verify(bv, {1, 3, 5, 7, 9}));
  }

  // Complex
  {
    // Use a non-multiple of the vector size to force execution of the tail
    // process loop.
    constexpr u32 vec_size = kDefaultVectorSize + 101 /* prime */;
    BitVector bv(vec_size);

    // Set even indexes
    std::random_device r;
    alignas(16) u8 bytes[vec_size] = {0};
    u32 num_set = 0;
    for (auto &byte : bytes) {
      byte = -(r() % 4 == 0);
      num_set += (byte == 0xff);
    }

    // Check only even indexes set
    bv.SetFromBytes(bytes, vec_size);
    EXPECT_TRUE(Verify(bv, [&](u32 idx) { return bytes[idx] == 0xff; }));
    EXPECT_EQ(num_set, bv.CountOnes());
  }
}

TEST(BitVectorTest, NthOne) {
  {
    BitVector bv = Make({0, 0, 1, 0, 0, 1, 0, 1, 1, 0, 1, 0});
    EXPECT_EQ(2u, bv.NthOne(0));
    EXPECT_EQ(5u, bv.NthOne(1));
    EXPECT_EQ(7u, bv.NthOne(2));
    EXPECT_EQ(8u, bv.NthOne(3));
    EXPECT_EQ(10u, bv.NthOne(4));
    EXPECT_EQ(bv.num_bits(), bv.NthOne(10));
  }

  // Multi-word
  {
    BitVector bv(140);
    EXPECT_EQ(bv.num_bits(), bv.NthOne(0));

    bv.Set(7);
    bv.Set(71);
    bv.Set(131);

    EXPECT_EQ(7u, bv.NthOne(0));
    EXPECT_EQ(71u, bv.NthOne(1));
    EXPECT_EQ(131u, bv.NthOne(2));
  }
}

TEST(BitVectorTest, Intersect) {
  BitVector bv1 = Make({0, 0, 1, 0, 0, 1, 0, 1, 1, 0, 1, 0});
  BitVector bv2 = Make({0, 1, 0, 0, 0, 1, 0, 1, 0, 0, 1, 0});
  bv1.Intersect(bv2);
  EXPECT_TRUE(Verify(bv1, {5, 7, 10}));
}

TEST(BitVectorTest, Union) {
  BitVector bv1 = Make({0, 0, 1, 0, 0, 1, 0, 1, 1, 0, 1, 0});
  BitVector bv2 = Make({0, 1, 0, 0, 0, 1, 0, 1, 0, 0, 1, 0});
  bv1.Union(bv2);
  EXPECT_TRUE(Verify(bv1, {1, 2, 5, 7, 8, 10}));
}

TEST(BitVectorTest, Difference) {
  BitVector bv1 = Make({0, 0, 1, 0, 0, 1, 0, 1, 1, 0, 1, 0});
  BitVector bv2 = Make({0, 1, 0, 0, 0, 1, 0, 1, 0, 0, 1, 0});
  bv1.Difference(bv2);
  EXPECT_TRUE(Verify(bv1, {2, 8}));
}

TEST(BitVectorTest, Iterate) {
  // Simple
  {
    BitVector bv(100);
    bv.IterateSetBits([](UNUSED auto idx) {
      FAIL() << "Empty bit vectors shouldn't have any set bits";
    });

    bv.Set(99);
    bv.IterateSetBits([](auto idx) { EXPECT_EQ(99u, idx); });

    bv.Set(64);
    bv.IterateSetBits([](auto idx) { EXPECT_TRUE(idx == 64 || idx == 99); });
  }

  // Complex 1
  {
    BitVector bv(100);
    // Set even bits
    for (u32 i = 0; i < bv.num_bits(); i++) {
      if (i % 2 == 0) {
        bv.Set(i);
      }
    }

    // Check
    bv.IterateSetBits([](auto idx) { EXPECT_EQ(0u, idx % 2); });

    // Flip and check again
    bv.FlipAll();
    bv.IterateSetBits([](auto idx) { EXPECT_NE(0u, idx % 2); });
  }
}

TEST(BitVectorTest, Slice) {
  BitVector bv(100);

  bv.Set(65);
  bv.Set(74);
  bv.Set(99);

  // Simple, bv2 only has position 1 (65 in original) set.
  BitVector bv2 = bv.Slice(64, 10);
  EXPECT_FALSE(bv2[0]);
  EXPECT_TRUE(bv2[1]);
  for (u32 i = 2; i < bv2.num_bits(); i++) {
    EXPECT_FALSE(bv2[i]) << "bv_2[" << i << "]=true, expected false";
  }

  // Oversize
  BitVector bv3 = bv.Slice(64, 100);
  EXPECT_EQ(36u, bv3.num_bits());
  EXPECT_TRUE(bv3[10]);
  EXPECT_TRUE(bv3[35]);

  // Nesting
  BitVector bv4 = bv3.Slice(0, 10);
  bv4.Set(5);
  EXPECT_TRUE(bv4[5]);
  EXPECT_TRUE(bv3[5]);
}

TEST(BitVectorTest, InlinedBitVector) {
  InlinedBitVector<64> bits;

  EXPECT_EQ(64u, bits.num_bits());

  // Initially all false
  for (u32 i = 0; i < bits.num_bits(); i++) {
    EXPECT_FALSE(bits.Test(i));
  }

  // Set even bits
  for (u32 i = 0; i < bits.num_bits(); i++) {
    if (i % 2 == 0) {
      bits.Set(i);
    }
  }

  // Check
  for (u32 i = 0; i < bits.num_bits(); i++) {
    auto set = bits.Test(i);
    if (i % 2 == 0) {
      EXPECT_TRUE(set);
    } else {
      EXPECT_FALSE(set);
    }
  }

  // Clear
  bits.UnsetAll();

  // Final check all 0
  for (u32 i = 0; i < bits.num_bits(); i++) {
    EXPECT_FALSE(bits.Test(i));
  }
}

}  // namespace tpl::util
