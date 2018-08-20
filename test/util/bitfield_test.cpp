#include "gtest/gtest.h"

#include "util/bitfield.h"

namespace tpl::util::test {

TEST(BitfieldTest, SingleElementTest) {
  // Try to encode a single 8-bit character element in a 32-bit integer
  using TestField = BitField<uint32_t, char, 0, sizeof(char) * kBitsPerByte>;

  // First, try a simple test where the character is at position 0
  {
    uint32_t s = TestField::Encode('P');

    EXPECT_EQ('P', TestField::Decode(s));

    s = TestField::Update(s, 'M');

    EXPECT_EQ('M', TestField::Decode(s));
  }

  // Now, try a more complicated scenario where the character is encoded at a
  // non-zero position
  {
    constexpr const char msg[] = "Hello, there!";

    uint32_t s;
    for (uint32_t i = 0; i < sizeof(msg); i++) {
      if (i == 0) {
        s = TestField::Encode(msg[i]);
      } else {
        s = TestField::Update(s, msg[i]);
      }
      EXPECT_EQ(msg[i], TestField::Decode(s));
    }
  }
}

TEST(BitfieldTest, MultiElementTest) {
  // Encode a 16-bit value and an 8-bit value in 32-bit storage
  using U16_BF =
      BitField<uint32_t, uint16_t, 0, sizeof(uint16_t) * kBitsPerByte>;

  using U8_BF = BitField<uint32_t, uint8_t, U16_BF::kNextBit,
                         sizeof(uint8_t) * kBitsPerByte>;

  uint32_t s = U16_BF::Encode(1024);

  EXPECT_EQ(1024, U16_BF::Decode(s));

  s = U8_BF::Update(s, 44);

  EXPECT_EQ(1024, U16_BF::Decode(s));
  EXPECT_EQ(44, U8_BF::Decode(s));
}

}  // namespace tpl::util::test
