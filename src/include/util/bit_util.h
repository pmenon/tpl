#pragma once

#include <memory>
#include <utility>

#include "common/common.h"
#include "common/macros.h"
#include "util/math_util.h"

namespace tpl::util {

/**
 * Utility class to deal with bit-level operations.
 */
class BitUtil : public AllStatic {
 public:
  // The number of bits in one word
  static constexpr const uint32_t kBitWordSize = sizeof(uint32_t) * kBitsPerByte;

  // Make sure the number of bits in a word is a power of two to make all these bit operations cheap
  static_assert(util::MathUtil::IsPowerOf2(kBitWordSize));

  /**
   * @return A count of the number of unset (i.e., '0') bits from the most significant bit to the
   *         least, stopping at the first set (i.e., '1') bit in the input integral value @em val.
   */
  template <typename T>
  static constexpr uint64_t CountLeadingZeros(T val) {
    return llvm::countLeadingZeros(val);
  }

  /**
   * @return A count of the number of unset (i.e., '0') bits from the least significant bit to the
   *         most, stopping at the first set (i.e., '1') bit in the input integral value @em val.
   */
  template <typename T>
  static constexpr uint64_t CountTrailingZeros(T val) {
    return llvm::countTrailingZeros(val);
  }

  /**
   * @return A count of the number of set (i.e., '1') bits in the given integral value @em val.
   */
  template <typename T>
  static constexpr uint32_t CountPopulation(T val) {
    return llvm::countPopulation(val);
  }

  /**
   * @return The number of words needed to store a bit vector with @em num_bits bits, rounded up to
   *         the next word size.
   */
  static constexpr uint64_t Num32BitWordsFor(uint64_t num_bits) {
    return MathUtil::DivRoundUp(num_bits, kBitWordSize);
  }

  /**
   * Test if the bit at index @em idx is set in the bit vector.
   * @param bits The bit vector.
   * @param idx The index of the bit to check.
   * @return True if set; false otherwise.
   */
  static constexpr bool Test(const uint32_t bits[], const uint32_t idx) {
    const uint32_t mask = 1u << (idx % kBitWordSize);
    return bits[idx / kBitWordSize] & mask;
  }

  /**
   * Set the bit at index @em idx to 1 in the bit vector @em bits.
   * @param bits The bit vector.
   * @param idx The index of the bit to set to 1.
   */
  static constexpr void Set(uint32_t bits[], const uint32_t idx) {
    bits[idx / kBitWordSize] |= 1u << (idx % kBitWordSize);
  }

  /**
   * Set the bit at index @em idx to 0 in the bit vector @em bits.
   * @param bits The bit vector.
   * @param idx The index of the bit to set to 0.
   */
  static constexpr void Unset(uint32_t bits[], const uint32_t idx) {
    bits[idx / kBitWordSize] &= ~(1u << (idx % kBitWordSize));
  }

  /**
   * Flip the value of the bit at index @em idx in the bit vector.
   * @param bits The bit vector.
   * @param idx The index of the bit to flip.
   */
  static constexpr void Flip(uint32_t bits[], const uint32_t idx) {
    bits[idx / kBitWordSize] ^= 1u << (idx % kBitWordSize);
  }
};

}  // namespace tpl::util
