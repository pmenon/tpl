#include "tpl_test.h"  // NOLINT

#include "util/bit_vector.h"

namespace tpl::util::test {

TEST(BitVectorTest, BitVectorSize) {
  // We need at least one word for 1 bit
  EXPECT_EQ(1u, BitVector::NumNeededWords(1));

  // We still need one 64-bit word for 63 and 64 bits
  EXPECT_EQ(1u, BitVector::NumNeededWords(63));
  EXPECT_EQ(1u, BitVector::NumNeededWords(64));

  // For 33 elements, we need two 32-bit words
  EXPECT_EQ(2u, BitVector::NumNeededWords(65));
}

TEST(BitVectorTest, Simple) {
  BitVector bv(100);
  EXPECT_EQ(2u, bv.num_words());
  EXPECT_EQ(100u, bv.num_bits());

  for (u32 i = 0; i < bv.num_bits(); i++) {
    EXPECT_FALSE(bv[i]);
  }
}

TEST(BitVectorTest, SetAndClearAll) {
  BitVector bv(10);

  bv.SetAll();
  for (u32 i = 0; i < bv.num_bits(); i++) {
    EXPECT_TRUE(bv[i]);
  }

  bv.UnsetAll();
  for (u32 i = 0; i < bv.num_bits(); i++) {
    EXPECT_FALSE(bv[i]);
  }
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
  BitVector bv(100);

  // Set even bits
  for (u32 i = 0; i < bv.num_bits(); i++) {
    if (i % 2 == 0) {
      bv.Set(i);
    }
  }

  // Flip every bit, now odd bits are set
  for (u32 i = 0; i < bv.num_bits(); i++) {
    bv.Flip(i);
  }

  // Now, every odd bit position should be set
  for (u32 i = 0; i < bv.num_bits(); i++) {
    if (i % 2 == 0) {
      EXPECT_FALSE(bv[i]);
    } else {
      EXPECT_TRUE(bv[i]);
    }
  }
}

TEST(BitVectorTest, Unset) {
  BitVector bv(100);

  // Set all to 1
  bv.SetAll();

  // Now, manually unset every bit
  for (u32 i = 0; i < bv.num_bits(); i++) {
    bv.Unset(i);
  }

  // Ensure all unset
  for (u32 i = 0; i < bv.num_bits(); i++) {
    EXPECT_FALSE(bv[i]);
  }
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
