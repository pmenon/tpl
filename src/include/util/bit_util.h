#pragma once

#include <memory>
#include <utility>

#include "util/common.h"
#include "util/macros.h"
#include "util/math_util.h"

namespace tpl::util {

/**
 * Utility class to deal with bit-level operations.
 */
class BitUtil {
 public:
  // The number of bits in one word
  static constexpr const u32 kBitWordSize = sizeof(u32) * kBitsPerByte;

  // Make sure the number of bits in a word is a power of two to make all these
  // bit operations cheap
  static_assert(util::MathUtil::IsPowerOf2(kBitWordSize));

  /**
   * Count the number of zeroes from the most significant bit to the first 1 in
   * the input number @em val
   * @tparam T The data type of the input value
   * @param val The input number
   * @return The number of leading zeros
   */
  template <typename T>
  ALWAYS_INLINE static u64 CountLeadingZeros(T val) {
    return llvm::countLeadingZeros(val);
  }

  /**
   * Calculate the number of 32-bit words are needed to store a bit vector of
   * the given size
   * @param num_bits The size of the bit vector, in bits
   * @return The number of words needed to store a bit vector of the given size
   */
  ALWAYS_INLINE static u64 Num32BitWordsFor(u64 num_bits) {
    return MathUtil::DivRoundUp(num_bits, kBitWordSize);
  }

  /**
   * Test if the bit at index @em idx is set in the bit vector
   * @param bits The bit vector
   * @param idx The index of the bit to check
   * @return True if set; false otherwise
   */
  ALWAYS_INLINE static bool Test(const u32 bits[], const u32 idx) {
    const u32 mask = 1u << (idx % kBitWordSize);
    return bits[idx / kBitWordSize] & mask;
  }

  /**
   * Set the bit at index @em idx to 1 in the bit vector @em bits
   * @param bits The bit vector
   * @param idx The index of the bit to set to 1
   */
  ALWAYS_INLINE static void Set(u32 bits[], const u32 idx) {
    bits[idx / kBitWordSize] |= 1u << (idx % kBitWordSize);
  }

  /**
   * Set the bit at index @em idx to the boolean indicated by @em val
   * @param bits The bit vector
   * @param idx The index of the bit to set or unset
   * @param val The value to set the bit to
   */
  ALWAYS_INLINE static void SetTo(u32 bits[], const u32 idx, const bool val) {
    if (val) {
      Set(bits, idx);
    } else {
      Unset(bits, idx);
    }
  }

  /**
   * Set the bit at index @em idx to 0 in the bit vector @em bits
   * @param bits The bit vector
   * @param idx The index of the bit to unset
   */
  ALWAYS_INLINE static void Unset(u32 bits[], const u32 idx) {
    bits[idx / kBitWordSize] &= ~(1u << (idx % kBitWordSize));
  }

  /**
   * Flip the value of the bit at index @em idx in the bit vector
   * @param bits The bit vector
   * @param idx The index of the bit to flip
   */
  ALWAYS_INLINE static void Flip(u32 bits[], const u32 idx) {
    bits[idx / kBitWordSize] ^= 1u << (idx % kBitWordSize);
  }

  /**
   * Set all bits in the bit vector to the given value.
   * @param bits The bit vector.
   * @param num_bits The number of elements in the bit vector.
   * @param val The value to set all bits to; 1 if true, false otherwise.
   */
  ALWAYS_INLINE static void SetAll(u32 bits[], const u64 num_bits,
                                   const bool val) {
    const auto num_words = Num32BitWordsFor(num_bits);
    const auto num_bytes = num_words * sizeof(u32);
    std::memset(bits, val ? 1 : 0, num_bytes);
  }

  /**
   * Count the number of set bits in the given value
   */
  template <typename T>
  static u32 CountBits(T val) {
    return llvm::countPopulation(val);
  }
};

}  // namespace tpl::util
