#pragma once

#include "util/common.h"
#include "util/math_util.h"

namespace tpl::util {

class BitUtil {
  static constexpr const u32 kBitWordSize = sizeof(i32) * kBitsPerByte;

  // Make sure the number of bits in a word is a power of two to make all these
  // bit operations cheap
  static_assert(util::MathUtil::IsPowerOf2(kBitWordSize));

 public:
  /// Calculate the number of 32-bit words are needed to store a bit vector of
  /// the given size
  /// \param num_bits The size of the bit vector, in bits
  /// \return The number of words needed to store a bit vector of the given size
  static u32 NumWordsFor(u32 num_bits) {
    return MathUtil::DivRoundUp(num_bits, kBitWordSize);
  }

  /// Test if the bit at index \refitem idx is set in the bit vector
  /// \param bits The bit vector
  /// \param idx The index of the bit to check
  /// \return True if set; false otherwise
  static bool Test(const u32 *const bits, const u32 idx) {
    u32 mask = 1u << (idx % kBitWordSize);
    return (bits[idx / kBitWordSize] & mask) != 0;
  }

  /// Set the bit at index \refitem idx to 1 in the bit vector \refitem bits
  /// \param bits The bit vector
  /// \param idx The index of the bit to set to 1
  static void Set(u32 *const bits, const u32 idx) {
    bits[idx / kBitWordSize] |= 1u << (idx % kBitWordSize);
  }

  /// Flip the value of the bit at index \refitem idx in the bit vector
  /// \param bits The bit vector
  /// \param idx The index of the bit to flip
  static void Flip(u32 *const bits, const u32 idx) {
    bits[idx / kBitWordSize] ^= 1u << (idx % kBitWordSize);
  }

  /// Clear all bits in the bit vector
  /// \param bits The bit vector
  /// \param size The number of bits in the bit vector
  static void Clear(u32 *const bits, const u32 size) {
    TPL_MEMSET(bits, 0, size / kBitWordSize);
  }
};

}  // namespace tpl::util