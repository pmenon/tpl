#pragma once

#include "util/bit_util.h"
#include "util/common.h"
#include "util/math_util.h"

namespace tpl::util {

/**
 * Base class for bit vectors. Uses CRTP to access bits and bit-vector size.
 * Subclasses must implement bits() and num_bits() to provide raw access to the
 * bit vector data and the number of bits, respectively.
 */
template <typename Subclass>
class BitVectorBase {
 protected:
  using WordType = u64;

  // The size of a word (in bytes) used to store a contiguous set of bits. This
  // is the smallest granularity we store bits at.
  static constexpr u32 kWordSizeBytes = sizeof(u64);
  // The size of a word in bits.
  static constexpr u32 kWordSizeBits = kWordSizeBytes * kBitsPerByte;
  // Ensure the size is a power of two so all the division and modulo math we do
  // is optimized into bit shifts.
  static_assert(MathUtil::IsPowerOf2(kWordSizeBits),
                "Word size in bits expected to be a power of two");

 public:
  /**
   * Return the number of words required to store at least @em num_bits number
   * if bits in a bit vector. Note that this may potentially over allocate.
   * @param num_bits The number of bits.
   * @return The number of words required to store the given number of bits.
   */
  constexpr static u32 NumNeededWords(u32 num_bits) {
    return util::MathUtil::DivRoundUp(num_bits, kWordSizeBits);
  }

  /**
   * Test if the bit at the provided index is set.
   * @return True if the bit is set; false otherwise.
   */
  bool Test(const u32 position) const {
    TPL_ASSERT(position < impl()->num_bits(), "Index out of range");
    const WordType *data = impl()->data_array();
    const WordType mask = WordType(1) << (position % kWordSizeBits);
    return data[position / kWordSizeBits] & mask;
  }

  /**
   * Blindly set the bit at the given index to 1.
   * @param position The index of the bit to set.
   */
  void Set(const u32 position) {
    TPL_ASSERT(position < impl()->num_bits(), "Index out of range");
    WordType *data = impl()->data_array();
    data[position / kWordSizeBits] |= WordType(1) << (position % kWordSizeBits);
  }

  /**
   * Set all bits to 1.
   */
  void SetAll() {
    auto *data_array = impl()->data_array();
    const auto num_words = impl()->num_words();
    // Set all bits in all words but the last
    std::memset(data_array, 255, kWordSizeBytes * (num_words - 1));
    // The last word is special
    data_array[num_words - 1] =
        ~static_cast<WordType>(0) >>
        (num_words * kWordSizeBits - impl()->num_bits());
  }

  /**
   * Blindly set the bit at the given index to 0.
   * @param position The index of the bit to set.
   */
  void Unset(const u32 position) {
    TPL_ASSERT(position < impl()->num_bits(), "Index out of range");
    WordType *data = impl()->data_array();
    data[position / kWordSizeBits] &=
        ~(WordType(1) << (position % kWordSizeBits));
  }

  /**
   * Write zeroes to all bits in the bit vector.
   */
  void UnsetAll() {
    const auto num_bytes = impl()->num_words() * kWordSizeBytes;
    std::memset(impl()->data_array(), 0, num_bytes);
  }

  /**
   * Complement the value of the bit at the given index. If it is currently 1,
   * it will be flipped to 0; if it is currently 0, its flipped to 1.
   * @param position The index of the bit to flip.
   */
  void Flip(const u32 position) {
    TPL_ASSERT(position < impl()->num_bits(), "Index out of range");
    WordType *data = impl()->data_array();
    data[position / kWordSizeBits] ^= WordType(1) << (position % kWordSizeBits);
  }

  /**
   * Check if any bit in the vector is non-zero.
   * @return True if any bit in the vector is set to 1; false otherwise.
   */
  bool Any() const {
    const auto *data_array = impl()->data_array();
    for (u32 i = 0; i < impl()->num_words(); i++) {
      if (data_array[i] != static_cast<WordType>(0)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Check if all bits in the vector are non-zero.
   * @return True if all bits are set to 1; false otherwise.
   */
  bool All() const {
    const auto *data_array = impl()->data_array();
    const auto num_words = impl()->num_words();
    for (u32 i = 0; i < num_words - 1; i++) {
      if (data_array[i] != ~static_cast<WordType>(0)) {
        return false;
      }
    }
    const WordType hi_word = data_array[num_words - 1];
    return hi_word == ~static_cast<WordType>(0) >>
                          (num_words * kWordSizeBits - impl()->num_bits());
  }

  /**
   * Check if all bits in the vector are zero.
   * @return True if all bits are set to 0; false otherwise.
   */
  bool None() const {
    const auto *data_array = impl()->data_array();
    for (u32 i = 0; i < impl()->num_words(); i++) {
      if (data_array[i] != static_cast<WordType>(0)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Find the first 1-bit at or after the given position in this bit vector.
   * @param position The starting position to search from.
   * @return The index of the first 1-bit at or after @em position. If there are
   *         no 1-bits, return the size of the bit vector.
   */
  u32 FirstOne(u32 position = 0) const {
    const auto *data_array = impl()->data_array();

    auto word_idx = position / kWordSizeBits;
    auto bit_idx = position % kWordSizeBits;

    if (bool mid_word = bit_idx != 0; mid_word) {
      auto word =
          data_array[word_idx] >> (position - (word_idx * kWordSizeBits));
      if (word != 0) {
        return position + BitUtil::CountTrailingZeros(word);
      }
      word_idx++;
    }

    for (u32 i = word_idx; i < impl()->num_words(); i++) {
      if (data_array[i] != static_cast<WordType>(0)) {
        return (i * kWordSizeBits) + BitUtil::CountTrailingZeros(data_array[i]);
      }
    }

    return impl()->num_bits();
  }

  /**
   * Find the first 0-bit at or after the given position in this bit vector.
   * @param position The starting position to search from.
   * @return The index of the first 1-bit at or after @em position. If there are
   *         no 0-bits, return the size of the bit vector.
   */
  u32 FirstZero(const u32 position = 0) const {
    throw std::runtime_error("Implement me");
  }

  /**
   * Find the last 0-bit strictly before the given position in this bit vector.
   * @param position The starting position to search from.
   * @return The index of the last 0-bit strictly before the given position. If
   *         there are no 0-bits, return 0.
   */
  u32 LastOne(const u32 position = std::numeric_limits<u32>::max()) const {
    throw std::runtime_error("Implement me");
  }

  /**
   * Find the last 0-bit strictly before the given position in this bit vector.
   * @param position The starting position to search from.
   * @return The index of the last 0-bit strictly before the given position. If
   *         there are no 0-bits, return 0.
   */
  u32 LastZero(const u32 position = std::numeric_limits<u32>::max()) const {
    throw std::runtime_error("Implement me");
  }

  /**
   * Count the 1-bits in the bit vector starting at the given position.
   * @param position The starting position to search from.
   * @return The number of 1-bits in the bit vector starting at @em position.
   */
  u32 CountOnes(const u32 position = 0) const {
    u32 count = 0;
    const auto *data_array = impl()->data_array();
    for (u32 i = 0; i < impl()->num_words(); i++) {
      count += util::BitUtil::CountBits(data_array[i]);
    }
    return count;
  }

  /**
   * Access the boolean value of the bit a the given index using an array
   * operator.
   * @param position The index of the bit to read.
   * @return True if the bit is set; false otherwise.
   */
  bool operator[](const u32 position) const { return Test(position); }

 private:
  // Access this instance as an instance of the templated subclass
  Subclass *impl() { return static_cast<Subclass *>(this); }
  // Access this instance as a constant instance of the templated subclass
  const Subclass *impl() const { return static_cast<const Subclass *>(this); }
};

/**
 * A generic bit vector. This class can either allocate and own its bits, or can
 * reference an externally managed (and owned) bit set. Use the appropriate
 * constructor.
 */
class BitVector : public BitVectorBase<BitVector> {
 public:
  /**
   * Create a new bit vector with the specified number of bits.
   * @param num_bits The number of bits in the vector.
   */
  explicit BitVector(const u32 num_bits)
      : num_bits_(num_bits), num_words_(NumNeededWords(num_bits)) {
    TPL_ASSERT(num_bits_ > 0, "Cannot create bit vector with zero bits");
    owned_data_ = std::make_unique<u64[]>(num_words_);
    data_array_ = owned_data_.get();
    UnsetAll();
  }

  /**
   * Create a new bit vector that references the given raw bit vector without
   * taking ownership.
   * @param unowned_data_array The externally managed bit vector.
   * @param num_bits The number of bits in the vector.
   */
  BitVector(u64 *const unowned_data_array, const u32 num_bits)
      : data_array_(unowned_data_array),
        num_bits_(num_bits),
        num_words_(NumNeededWords(num_bits)),
        owned_data_(nullptr) {
    TPL_ASSERT(data_array_ != nullptr,
               "Cannot create bit vector referencing NULL bitmap");
    TPL_ASSERT(num_bits_ > 0, "Cannot create bit vector with zero bits");
  }

  /**
   * Return the number of bits in the bit vector.
   */
  u32 num_bits() const { return num_bits_; }

  /**
   * Return the number of words used by the bit vector.
   */
  u32 num_words() const { return num_words_; }

 private:
  friend class BitVectorBase<BitVector>;

  u64 *data_array() { return data_array_; }
  const u64 *data_array() const { return data_array_; }

 private:
  // The array of bits.
  u64 *data_array_;

  // The number of bits in the bit vector.
  u32 num_bits_;

  // The number of words in the bit vector.
  u32 num_words_;

  // If this vector allocated its bits, this pointer owns it.
  std::unique_ptr<u64[]> owned_data_;
};

/**
 * A bit vector that stores the bit set data inline in the class.
 */
template <u32 NumBits>
class InlinedBitVector : public BitVectorBase<InlinedBitVector<NumBits>> {
  using Base = BitVectorBase<InlinedBitVector<NumBits>>;

  static_assert(NumBits % Base::kWordSizeBits == 0,
                "Inlined bit vectors only support vectors that are a multiple "
                "of the word size (i.e., 64 bits, 128 bits, etc.");

  static constexpr u32 kNumWords = Base::NumNeededWords(NumBits);

 public:
  /**
   * Construct a bit vector with all zeros.
   */
  InlinedBitVector() : data_array_{0} {}

  /**
   * Return the number of bits in the bit vector.
   */
  u32 num_bits() const { return NumBits; }

  /**
   * Return the number of words used by the bit vector.
   */
  u32 num_words() const { return kNumWords; }

 private:
  friend class BitVectorBase<InlinedBitVector<NumBits>>;

  u64 *data_array() { return data_array_; }
  const u64 *data_array() const { return data_array_; }

 private:
  u64 data_array_[kNumWords];
};

}  // namespace tpl::util
