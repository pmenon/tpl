#pragma once

#include <immintrin.h>
#include <algorithm>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include "common/common.h"
#include "util/bit_util.h"
#include "util/math_util.h"
#include "util/vector_util.h"

namespace tpl::util {

/**
 * A BitVector represents a set of bits. It provides access to individual bits through
 * BitVector::operator[], along with a collection of operations commonly performed on bit vectors
 * such as intersection (operator&), union (operator|), and difference (operator-).
 *
 * There are a few important differences between BitVector and std::bitset<>:
 * 1. The size of a BitVector is provided at run-time during construction and is resizable over its
 *    lifetime, whereas the size of a std::bitset<> is a compile-time constant provided through a
 *    template argument.
 * 2. BitVectors allow using wider underlying "storage blocks" as the unit of bit storage. For
 *    sparse bit vectors, this enables faster iteration while not compromising on dense vectors.
 * 3. BitVectors support bulk-update features that can leverage SIMD instructions.
 * 4. BitVectors allow faster conversion to selection index vectors.
 *
 * @tparam WordType An unsigned integral type where the bits of the bit vector are stored.
 */
template <typename WordType = uint64_t, typename Allocator = std::allocator<WordType>>
class BitVector {
  static_assert(std::is_integral_v<WordType> && std::is_unsigned_v<WordType>,
                "Template type 'Word' must be an unsigned integral");

  // The size of a word (in bytes) used to store a contiguous set of bits. This
  // is the smallest granularity we store bits at.
  static constexpr uint32_t kWordSizeBytes = sizeof(WordType);

  // The size of a word in bits.
  static constexpr uint32_t kWordSizeBits = kWordSizeBytes * kBitsPerByte;

  // Word value with all ones.
  static constexpr WordType kAllOnesWord = ~static_cast<WordType>(0);

  // Ensure the size is a power of two so all the division and modulo math we do
  // is optimized into bit shifts.
  static_assert(MathUtil::IsPowerOf2(kWordSizeBits),
                "Word size in bits expected to be a power of two");

 public:
  /**
   * Return the number of words required to store at least @em num_bits number if bits in a bit
   * vector. Note that this may potentially over allocate.
   * @param num_bits The number of bits.
   * @return The number of words required to store the given number of bits.
   */
  static constexpr uint32_t NumNeededWords(uint32_t num_bits) {
    return util::MathUtil::DivRoundUp(num_bits, kWordSizeBits);
  }

  /**
   * Abstracts a reference to one bit in the bit vector.
   */
  class BitReference {
   public:
    /**
     * Test the value of the bit.
     * @return True if the bit is 1; false otherwise.
     */
    operator bool() const noexcept { return ((*word_) & mask_) != 0; }  // NOLINT

    /**
     * Assign the value of the bit to the boolean @em val. If input value is true, the bit is set to
     * 1. If the input value is false, the bit is set to 0.
     * @param val The value to assign the bit.
     * @return This bit.
     */
    BitReference &operator=(bool val) noexcept {
      Assign(val);
      return *this;
    }

    /**
     * Assign the value of the bit the value of another bit in the bit vector.
     * @param other The other bit to read from.
     * @return This bit.
     */
    BitReference &operator=(const BitReference &other) noexcept {
      Assign(other);
      return *this;
    }

   private:
    friend class BitVector<WordType>;

    BitReference(WordType *word, uint32_t bit_pos) : word_(word), mask_(WordType(1) << bit_pos) {}

    // Assign this bit to the given value
    void Assign(bool val) noexcept { (*word_) ^= (-static_cast<WordType>(val) ^ *word_) & mask_; }

   private:
    WordType *word_;
    WordType mask_;
  };

  /**
   * Create an empty bit vector. Users must call @em Resize() before interacting with it.
   *
   * @ref BitVector::Resize()
   */
  BitVector() : num_bits_(0) {}

  /**
   * Create a new bit vector with the specified number of bits. After construction, all bits are
   * unset.
   * @param num_bits The number of bits in the vector.
   */
  explicit BitVector(const uint32_t num_bits)
      : num_bits_(num_bits), words_(NumNeededWords(num_bits), WordType(0)) {
    TPL_ASSERT(num_bits_ > 0, "Cannot create bit vector with zero bits");
  }

  /**
   * Create a copy of the provided bit vector.
   * @param other The bit vector to copy.
   */
  BitVector(const BitVector &other) : num_bits_(other.num_bits_), words_(other.words_) {}

  /**
   * Move constructor.
   * @param other Move the given bit vector into this.
   */
  BitVector(BitVector &&other) noexcept = default;

  /**
   * Copy the provided bit vector into this bit vector.
   * @param other The bit vector to copy.
   * @return This bit vector as a copy of the input vector.
   */
  BitVector &operator=(const BitVector &other) {
    num_bits_ = other.num_bits_;
    words_ = other.words_;
    return *this;
  }

  /**
   * Move assignment.
   * @param other The bit vector we're moving.
   * @return This vector.
   */
  BitVector &operator=(BitVector &&other) noexcept = default;

  /**
   * Test if the bit at the provided index is set.
   * @return True if the bit is set; false otherwise.
   */
  bool Test(const uint32_t position) const {
    TPL_ASSERT(position < num_bits(), "Index out of range");
    const WordType mask = WordType(1) << (position % kWordSizeBits);
    return words_[position / kWordSizeBits] & mask;
  }

  /**
   * Blindly set the bit at the given index to 1.
   * @param position The index of the bit to set.
   * @return This bit vector.
   */
  BitVector &Set(const uint32_t position) {
    TPL_ASSERT(position < num_bits(), "Index out of range");
    words_[position / kWordSizeBits] |= WordType(1) << (position % kWordSizeBits);
    return *this;
  }

  /**
   * Set the bit at the given position to a given value.
   * @param position The index of the bit to set.
   * @param v The value to set the bit to.
   * @return This bit vector.
   */
  BitVector &Set(const uint32_t position, const bool v) {
    TPL_ASSERT(position < num_bits(), "Index out of range");
    WordType mask = static_cast<WordType>(1) << (position % kWordSizeBits);
    words_[position / kWordSizeBits] ^=
        (-static_cast<WordType>(v) ^ words_[position / kWordSizeBits]) & mask;
    return *this;
  }

  /**
   * Efficiently set all bits in the range [start, end).
   *
   * @pre start <= end <= num_bits()
   *
   * @param start The start bit position.
   * @param end The end bit position.
   * @return This bit vector.
   */
  BitVector &SetRange(uint32_t start, uint32_t end) {
    TPL_ASSERT(start <= end, "Cannot set backward range");
    TPL_ASSERT(end <= num_bits(), "End position out of range");

    if (start == end) {
      return *this;
    }

    const auto start_word_idx = start / kWordSizeBits;
    const auto end_word_idx = end / kWordSizeBits;

    if (start_word_idx == end_word_idx) {
      const WordType prefix_mask = kAllOnesWord << (start % kWordSizeBits);
      const WordType postfix_mask = ~(kAllOnesWord << (end % kWordSizeBits));
      words_[start_word_idx] |= (prefix_mask & postfix_mask);
      return *this;
    }

    // Prefix
    words_[start_word_idx] |= kAllOnesWord << (start % kWordSizeBits);

    // Middle
    for (uint32_t i = start_word_idx + 1; i < end_word_idx; i++) {
      words_[i] = kAllOnesWord;
    }

    // Postfix
    words_[end_word_idx] |= ~(kAllOnesWord << (end % kWordSizeBits));

    return *this;
  }

  /**
   * Set all bits to 1.
   * @return This bit vector.
   */
  BitVector &SetAll() {
    for (uint32_t i = 0; i < num_words(); i++) {
      words_[i] = kAllOnesWord;
    }
    ZeroUnusedBits();
    return *this;
  }

  /**
   * Blindly set the bit at the given index to 0.
   * @param position The index of the bit to set.
   * @return This bit vector.
   */
  BitVector &Unset(const uint32_t position) {
    TPL_ASSERT(position < num_bits(), "Index out of range");
    words_[position / kWordSizeBits] &= ~(WordType(1) << (position % kWordSizeBits));
    return *this;
  }

  /**
   * Set all bits in the bit vector to 0.
   * @return This bit vector.
   */
  BitVector &Reset() {
    for (uint32_t i = 0; i < num_words(); i++) {
      words_[i] = WordType(0);
    }
    return *this;
  }

  /**
   * Flip the bit at the given bit position, i.e., change the bit to 1 if it's 0 and to 0 if it's 1.
   * @param position The index of the bit to flip.
   * @return This bit vector.
   */
  BitVector &Flip(const uint32_t position) {
    TPL_ASSERT(position < num_bits(), "Index out of range");
    words_[position / kWordSizeBits] ^= WordType(1) << (position % kWordSizeBits);
    return *this;
  }

  /**
   * Flip all bits in the bit vector.
   * @return This bit vector.
   */
  BitVector &FlipAll() {
    for (uint32_t i = 0; i < num_words(); i++) {
      words_[i] = ~words_[i];
    }
    ZeroUnusedBits();
    return *this;
  }

  /**
   * Get the word at the given word index.
   * @param word_position The index of the word to set.
   * @return Value of the word.
   */
  WordType GetWord(const uint32_t word_position) const {
    TPL_ASSERT(word_position < num_words(), "Index out of range");
    return words_[word_position];
  }

  /**
   * Set the value of the word at the given word index to the provided value. If the size of the bit
   * vector is not a multiple of the word size, the tail bits are masked off.
   * @param word_position The index of the word to set.
   * @param word_val The value to set.
   * @return This bit vector.
   */
  BitVector &SetWord(const uint32_t word_position, const WordType word_val) {
    TPL_ASSERT(word_position < num_words(), "Index out of range");
    words_[word_position] = word_val;
    if (word_position == num_words() - 1) ZeroUnusedBits();
    return *this;
  }

  /**
   * Check if any bit in the vector is non-zero.
   * @return True if any bit in the vector is set to 1; false otherwise.
   */
  bool Any() const {
    for (uint32_t i = 0; i < num_words(); i++) {
      if (words_[i] != static_cast<WordType>(0)) {
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
    const uint32_t extra_bits = GetNumExtraBits();

    if (extra_bits == 0) {
      for (uint32_t i = 0; i < num_words(); i++) {
        if (words_[i] != kAllOnesWord) {
          return false;
        }
      }
      return true;
    } else {
      for (uint32_t i = 0; i < num_words() - 1; i++) {
        if (words_[i] != kAllOnesWord) {
          return false;
        }
      }
      const WordType mask = ~(kAllOnesWord << GetNumExtraBits());
      return words_[num_words() - 1] == mask;
    }
  }

  /**
   * Check if all bits in the vector are zero.
   * @return True if all bits are set to 0; false otherwise.
   */
  bool None() const { return !Any(); }

  /**
   * Count the 1-bits in the bit vector.
   * @return The number of 1-bits in the bit vector.
   */
  uint32_t CountOnes() const {
    uint32_t count = 0;
    for (uint32_t i = 0; i < num_words(); i++) {
      count += util::BitUtil::CountPopulation(words_[i]);
    }
    return count;
  }

  /**
   * Return the index of the n-th 1 in this bit vector.
   * @param n Which 1-bit to look for.
   * @return The index of the n-th 1-bit. If there are fewer than @em n bits, return the size.
   */
  uint32_t NthOne(uint32_t n) const {
    for (uint32_t i = 0; i < num_words(); i++) {
      const WordType word = words_[i];
      const uint32_t count = BitUtil::CountPopulation(word);
      if (n < count) {
        const WordType mask = _pdep_u64(static_cast<WordType>(1) << n, word);
        const uint32_t pos = BitUtil::CountTrailingZeros(mask);
        return std::min(num_bits(), (i * kWordSizeBits) + pos);
      }
      n -= count;
    }
    return num_bits();
  }

  /**
   * Copy the bit vector @em other into this bit vector.
   *
   * @pre The sizes of the bit vectors must be the same.
   *
   * @param other The bit vector to read and copy from.
   * @return This bit vector.
   */
  BitVector &Copy(const BitVector &other) {
    TPL_ASSERT(num_bits() == other.num_bits(), "Mismatched bit vector size");
    for (uint32_t i = 0; i < num_words(); i++) {
      words_[i] = other.words_[i];
    }
    return *this;
  }

  /**
   * Perform the bitwise intersection of this bit vector with the provided @em other bit vector.
   *
   * @pre The sizes of the bit vectors must be the same.
   *
   * @param other The bit vector to intersect with. Lengths must match exactly.
   * @return This bit vector.
   */
  BitVector &Intersect(const BitVector &other) {
    TPL_ASSERT(num_bits() == other.num_bits(), "Mismatched bit vector size");
    for (uint32_t i = 0; i < num_words(); i++) {
      words_[i] &= other.words_[i];
    }
    return *this;
  }

  /**
   * Perform the bitwise union of this bit vector with the provided @em other bit vector.
   *
   * @pre The sizes of the bit vectors must be the same.
   *
   * @param other The bit vector to union with. Lengths must match exactly.
   * @return This bit vector.
   */
  BitVector &Union(const BitVector &other) {
    TPL_ASSERT(num_bits() == other.num_bits(), "Mismatched bit vector size");
    for (uint32_t i = 0; i < num_words(); i++) {
      words_[i] |= other.words_[i];
    }
    return *this;
  }

  /**
   * Clear all bits in this bit vector whose corresponding bit is set in the provided bit vector.
   *
   * @pre The sizes of the bit vectors must be the same.
   *
   * @param other The bit vector to diff with. Lengths must match exactly.
   * @return This bit vector.
   */
  BitVector &Difference(const BitVector &other) {
    TPL_ASSERT(num_bits() == other.num_bits(), "Mismatched bit vector size");
    for (uint32_t i = 0; i < num_words(); i++) {
      words_[i] &= ~other.words_[i];
    }
    return *this;
  }

  /**
   * Reserve enough space in the bit vector to store @em num_bits bits. This does not change the
   * size of the bit vector, but may allocate additional memory.
   * @param num_bits The desired number of bits to reserve for.
   */
  void Reserve(const uint32_t num_bits) { words_.reserve(NumNeededWords(num_bits)); }

  /**
   * Change the number of bits in the bit vector to @em num_bits. If @em num_bits > num_bits() then
   * the bits in the range [0, num_bits()) are unchanged, and the remaining bits are set to zero. If
   * @em num_bits < @em num_bits() then the bits in the range [0, num_bits) are unchanged the
   * remaining bits are discarded.
   *
   * @param num_bits The number of bits to resize this bit vector to.
   */
  void Resize(const uint32_t num_bits) {
    if (num_bits == num_bits_) {
      return;
    }

    uint32_t new_num_words = NumNeededWords(num_bits);

    if (num_words() != new_num_words) {
      words_.resize(new_num_words, WordType(0));
    }

    num_bits_ = num_bits;
    ZeroUnusedBits();
  }

  /**
   * Retain all set bits in the bit vector for which the predicate returns true.
   * @tparam P A predicate functor that accepts an unsigned 32-bit integer and returns a boolean.
   * @param p The predicate to apply to each set bit position.
   */
  template <typename P>
  void UpdateSetBits(P &&p) {
    static_assert(std::is_invocable_r_v<bool, P, uint32_t>,
                  "Predicate must be accept an unsigned 32-bit index and return a bool");

    for (WordType i = 0; i < num_words(); i++) {
      WordType word = words_[i];
      WordType word_result = 0;
      while (word != 0) {
        const auto t = word & -word;
        const auto r = util::BitUtil::CountTrailingZeros(word);
        word_result |= static_cast<WordType>(p(i * kWordSizeBits + r)) << r;
        word ^= t;
      }
      words_[i] &= word_result;
    }
  }

  /**
   * Like BitVector::UpdateSetBits(), this function will also retain all set bits in the bit vector
   * for which the predicate returns true. The important difference is that this function will
   * invoke the predicate function on ALL bit positions, not just those positions that are set to 1.
   * This optimization takes advantage of SIMD to enable up-to 4x faster execution times, but the
   * caller must safely tolerate operating on both set and unset bit positions.
   *
   * @tparam P A predicate functor that accepts an unsigned 32-bit integer and returns a boolean.
   * @param p The predicate to apply to each bit position.
   */
  template <typename P>
  void UpdateFull(P &&p) {
    static_assert(std::is_invocable_r_v<bool, P, uint32_t>,
                  "Predicate must be accept an unsigned 32-bit index and return a bool");
    if (num_bits() == 0) {
      return;
    }

    const uint32_t num_full_words = GetNumExtraBits() == 0 ? num_words() : num_words() - 1;

    // This first loop processes all FULL words in the bit vector. It should be fully vectorized
    // if the predicate function can also vectorized.
    for (WordType i = 0; i < num_full_words; i++) {
      WordType word_result = 0;
      for (WordType j = 0; j < kWordSizeBits; j++) {
        word_result |= static_cast<WordType>(p(i * kWordSizeBits + j)) << j;
      }
      words_[i] &= word_result;
    }

    // If the last word isn't full, process it using a scalar loop.
    for (WordType i = num_full_words * kWordSizeBits; i < num_bits(); i++) {
      if (!p(i)) {
        Unset(i);
      }
    }
  }

  /**
   * Iterate all bits in this vector and invoke the callback with the index of set bits only.
   * @tparam F The type of the callback function. Must accept a single unsigned integer value.
   * @param callback The callback function to invoke with the index of set bits.
   */
  template <typename F>
  void IterateSetBits(F &&callback) const {
    static_assert(std::is_invocable_v<F, uint32_t>,
                  "Callback must be a single-argument functor accepting an unsigned 32-bit index");

    for (WordType i = 0; i < num_words(); i++) {
      WordType word = words_[i];
      while (word != 0) {
        const auto t = word & -word;
        const auto r = BitUtil::CountTrailingZeros(word);
        callback(i * kWordSizeBits + r);
        word ^= t;
      }
    }
  }

  /**
   * Populate this bit vector from the values stored in the given bytes. The byte array is assumed
   * to be a "saturated" match vector, i.e., true values are all 1's
   * (255 = 11111111 = std::numeric_limits<uint8_t>::max()), and false values are all 0.
   * @param bytes The array of saturated bytes to read.
   * @param num_bytes The number of bytes in the input array.
   */
  void SetFromBytes(const uint8_t *const bytes, const uint32_t num_bytes) {
    TPL_ASSERT(bytes != nullptr, "Null input");
    TPL_ASSERT(num_bytes == num_bits(), "Byte vector too small");
    util::VectorUtil::ByteVectorToBitVector(bytes, num_bytes, words_.data());
  }

  /**
   * Return a string representation of this bit vector.
   * @return String representation of this vector.
   */
  std::string ToString() const {
    std::string result = "BitVector(#bits=" + std::to_string(num_bits()) + ")=[";
    bool first = true;
    for (uint32_t i = 0; i < num_bits(); i++) {
      if (!first) result += ",";
      first = false;
      result += Test(i) ? "1" : "0";
    }
    result += "]";
    return result;
  }

  // -------------------------------------------------------
  // Operator overloads -- should be fairly obvious ...
  // -------------------------------------------------------

  bool operator[](const uint32_t position) const { return Test(position); }

  BitReference operator[](const uint32_t position) {
    TPL_ASSERT(position < num_bits(), "Out-of-range access");
    return BitReference(&words_[position / kWordSizeBits], position % kWordSizeBits);
  }

  bool operator==(const BitVector &that) const noexcept {
    return num_bits() == that.num_bits() && words_ == that.words_;
  }

  bool operator!=(const BitVector &that) const noexcept { return !(*this == that); }

  BitVector &operator-=(const BitVector &other) {
    Difference(other);
    return *this;
  }

  BitVector &operator&=(const BitVector &other) {
    Intersect(other);
    return *this;
  }

  BitVector &operator|=(const BitVector &other) {
    Union(other);
    return *this;
  }

  uint32_t num_bits() const { return num_bits_; }

  uint32_t num_words() const { return words_.size(); }

  const WordType *words() const { return words_.data(); }

  WordType *words() { return words_.data(); }

 private:
  // The number of used bits in the last word
  uint32_t GetNumExtraBits() const { return num_bits_ % kWordSizeBits; }

  // Zero unused bits in the last word
  void ZeroUnusedBits() {
    uint32_t extra_bits = GetNumExtraBits();
    if (extra_bits != 0) {
      words_[num_words() - 1] &= ~(kAllOnesWord << extra_bits);
    }
  }

 private:
  // The number of bits in the bit vector
  uint32_t num_bits_;
  // The array of words making up the bit vector
  std::vector<WordType> words_;
};

template <typename T>
inline BitVector<T> operator-(BitVector<T> a, const BitVector<T> &b) {
  a -= b;
  return a;
}

template <typename T>
inline BitVector<T> operator&(BitVector<T> a, const BitVector<T> &b) {
  a &= b;
  return a;
}

template <typename T>
inline BitVector<T> operator|(BitVector<T> a, const BitVector<T> &b) {
  a |= b;
  return a;
}

}  // namespace tpl::util
