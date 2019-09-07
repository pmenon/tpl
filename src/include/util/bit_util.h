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
  static constexpr const uint32_t kBitWordSize = sizeof(uint32_t) * kBitsPerByte;

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
  constexpr static uint64_t CountLeadingZeros(T val) {
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
  constexpr static uint64_t CountTrailingZeros(T val) {
    return llvm::countTrailingZeros(val);
  }

  /**
   * Count the number of set bits in the given value.
   */
  template <typename T>
  constexpr static uint32_t CountPopulation(T val) {
    return llvm::countPopulation(val);
  }

  /**
   * Calculate the number of 32-bit words are needed to store a bit vector of
   * the given size
   * @param num_bits The size of the bit vector, in bits
   * @return The number of words needed to store a bit vector of the given size
   */
  constexpr static uint64_t Num32BitWordsFor(uint64_t num_bits) {
    return MathUtil::DivRoundUp(num_bits, kBitWordSize);
  }

  /**
   * Test if the bit at index @em idx is set in the bit vector
   * @param bits The bit vector
   * @param idx The index of the bit to check
   * @return True if set; false otherwise
   */
  constexpr static bool Test(const uint32_t bits[], const uint32_t idx) {
    const uint32_t mask = 1u << (idx % kBitWordSize);
    return bits[idx / kBitWordSize] & mask;
  }

  /**
   * Set the bit at index @em idx to 1 in the bit vector @em bits
   * @param bits The bit vector
   * @param idx The index of the bit to set to 1
   */
  constexpr static void Set(uint32_t bits[], const uint32_t idx) {
    bits[idx / kBitWordSize] |= 1u << (idx % kBitWordSize);
  }

  /**
   * Flip the value of the bit at index @em idx in the bit vector
   * @param bits The bit vector
   * @param idx The index of the bit to flip
   */
  constexpr static void Flip(uint32_t bits[], const uint32_t idx) {
    bits[idx / kBitWordSize] ^= 1u << (idx % kBitWordSize);
  }
};

}  // namespace tpl::util
