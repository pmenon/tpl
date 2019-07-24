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
  constexpr static u64 CountLeadingZeros(T val) {
    return llvm::countLeadingZeros(val);
  }

  /**
   * Count the number of zeroes from the most significant bit to the first 1 in
   * the input number @em val
   * @tparam T The data type of the input value
   * @param val The input number
   * @return The number of leading zeros
   */
  template <typename T>
  constexpr static u64 CountTrailingZeros(T val) {
    return llvm::countTrailingZeros(val);
  }

  /**
   * Count the number of set bits in the given value.
   */
  template <typename T>
  constexpr static u32 CountBits(T val) {
    return llvm::countPopulation(val);
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
   * Flip the value of the bit at index @em idx in the bit vector
   * @param bits The bit vector
   * @param idx The index of the bit to flip
   */
  ALWAYS_INLINE static void Flip(u32 bits[], const u32 idx) {
    bits[idx / kBitWordSize] ^= 1u << (idx % kBitWordSize);
  }
};

}  // namespace tpl::util
