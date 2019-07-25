#include <unordered_set>

#include "tpl_test.h"  // NOLINT

#include "util/bit_vector.h"

namespace tpl::util::test {

namespace {

// Verify that only specific bits are set
template <typename BitVectorType>
bool Verify(BitVectorType &bv, std::initializer_list<u32> idxs) {
  std::unordered_set<u32> positions(idxs);
  for (u32 i = 0; i < bv.num_bits(); i++) {
    bool expected_in_bits = positions.count(i);
    if (expected_in_bits != bv[i]) {
      return false;
    }
  }
  return true;
}

}  // namespace

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
  Verify(bv, {2});

  bv.Set(0);
  Verify(bv, {0, 2});

  bv.Set(7);
  Verify(bv, {0, 2, 7});

  bv.SetAll();
  Verify(bv, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
}

TEST(BitVectorTest, Unset) {
  BitVector bv(10);

  // Set every 3rd bit
  for (u32 i = 0; i < bv.num_bits(); i++) {
    if (i % 3 == 0) {
      bv.Set(i);
    }
  }

  bv.Unset(3);
  Verify(bv, {0, 6, 9});

  bv.Unset(9);
  Verify(bv, {0, 9});

  bv.UnsetAll();
  Verify(bv, {});
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
  Verify(bv, {0, 2, 4, 6, 8});

  bv.Flip(0);
  Verify(bv, {2, 4, 6, 8});

  bv.Flip(8);
  Verify(bv, {2, 4, 6});

  bv.FlipAll();
  Verify(bv, {0, 1, 3, 5, 7, 8, 9});
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

TEST(BitVectorTest, FindFirstOne) {
  // Simple
  {
    BitVector bv(100);
    EXPECT_EQ(bv.num_bits(), bv.FirstOne());

    bv.Set(10);
    EXPECT_EQ(10u, bv.FirstOne());
    EXPECT_EQ(bv.num_bits(), bv.FirstOne(11));

    bv.Set(32);
    EXPECT_EQ(32u, bv.FirstOne(11));
    EXPECT_EQ(bv.num_bits(), bv.FirstOne(33));

    bv.Set(64);
    EXPECT_EQ(64u, bv.FirstOne(33));
    EXPECT_EQ(bv.num_bits(), bv.FirstOne(65));
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
    auto position = bv.FirstOne();
    for (; position != bv.num_bits(); position = bv.FirstOne(position + 1)) {
      EXPECT_TRUE(position % 2 == 0);
    }
  }
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

}  // namespace tpl::util::test
