#pragma once

#include <immintrin.h>
#include <algorithm>
#include <memory>
#include <string>

#include "common/common.h"
#include "util/bit_util.h"
#include "util/math_util.h"
#include "util/vector_util.h"

namespace tpl::util {

/**
 * Base class for bit vector implementations. Uses the CRTP pattern to lift algorithmic
 * implementation into single class while delegating actual memory layout and storage semantics to
 * base classes. All implementation subclasses must implement num_bits() to return the size of the
 * bit vector in bits, num_words() to return the number of 64-bit words that make up the bit vector,
 * and data_array() to return the array of words constituting the bit vector.
 */
template <typename Subclass>
class BitVectorBase {
 public:
  // Bits are grouped into chunks (also known as words) of 64-bits.
  using WordType = uint64_t;

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

  /**
   * Return the number of words required to store at least @em num_bits number if bits in a bit
   * vector. Note that this may potentially over allocate.
   * @param num_bits The number of bits.
   * @return The number of words required to store the given number of bits.
   */
  constexpr static uint32_t NumNeededWords(uint32_t num_bits) {
    return util::MathUtil::DivRoundUp(num_bits, kWordSizeBits);
  }

  /**
   * Test if the bit at the provided index is set.
   * @return True if the bit is set; false otherwise.
   */
  bool Test(const uint32_t position) const {
    TPL_ASSERT(position < impl()->num_bits(), "Index out of range");
    const WordType *data = impl()->data_array();
    const WordType mask = WordType(1) << (position % kWordSizeBits);
    return data[position / kWordSizeBits] & mask;
  }

  /**
   * Blindly set the bit at the given index to 1.
   * @param position The index of the bit to set.
   */
  void Set(const uint32_t position) {
    TPL_ASSERT(position < impl()->num_bits(), "Index out of range");
    WordType *data = impl()->data_array();
    data[position / kWordSizeBits] |= WordType(1) << (position % kWordSizeBits);
  }

  /**
   * Efficiently set all bits in the range [start, end).
   * @param start The start bit position.
   * @param end The end bit position.
   */
  void SetRange(uint32_t start, uint32_t end) {
    TPL_ASSERT(start <= end, "Cannot set backward range");
    TPL_ASSERT(end <= impl()->num_bits(), "End position out of range");

    if (start == end) {
      return;
    }

    WordType *data = impl()->data_array();

    const auto start_word_idx = start / kWordSizeBits;
    const auto end_word_idx = end / kWordSizeBits;

    if (start_word_idx == end_word_idx) {
      const WordType prefix_mask = kAllOnesWord << (start % kWordSizeBits);
      const WordType postfix_mask = ~(kAllOnesWord << (end % kWordSizeBits));
      data[start_word_idx] |= (prefix_mask & postfix_mask);
      return;
    }

    // Prefix
    data[start_word_idx] |= kAllOnesWord << (start % kWordSizeBits);

    // Middle
    for (uint32_t i = start_word_idx + 1; i < end_word_idx; i++) {
      data[i] = kAllOnesWord;
    }

    // Postfix
    data[end_word_idx] |= ~(kAllOnesWord << (end % kWordSizeBits));
  }

  /**
   * Set the bit at the given position to a given value.
   * @param position The index of the bit to set.
   * @param v The value to set the bit to.
   */
  void SetTo(const uint32_t position, const bool v) {
    TPL_ASSERT(position < impl()->num_bits(), "Index out of range");
    WordType *const data = impl()->data_array();
    WordType mask = static_cast<WordType>(1) << (position % kWordSizeBits);
    data[position / kWordSizeBits] ^=
        (-static_cast<WordType>(v) ^ data[position / kWordSizeBits]) & mask;
  }

  /**
   * Get the word at the given word index.
   * @param word_position The index of the word to set.
   * @return Value of the word.
   */
  WordType GetWord(const uint32_t word_position) const {
    TPL_ASSERT(word_position < impl()->num_words(), "Index out of range");
    return impl()->data_array()[word_position];
  }

  /**
   * Set the value of the word at the given word index to the provided value. If the size of the bit
   * vector is not a multiple of the word size, the tail bits are masked off.
   * @param word_position The index of the word to set.
   * @param word_val The value to set.
   */
  void SetWord(const uint32_t word_position, const WordType word_val) {
    TPL_ASSERT(word_position < impl()->num_words(), "Index out of range");
    WordType *data = impl()->data_array();
    const uint32_t num_words = impl()->num_words();
    data[word_position] = word_val;
    if (word_position == num_words - 1) {
      data[num_words - 1] &= kAllOnesWord >> (num_words * kWordSizeBits - impl()->num_bits());
    }
  }

  /**
   * Set all bits to 1.
   */
  void SetAll() {
    WordType *data = impl()->data_array();
    const auto num_words = impl()->num_words();
    // Set all words but the last
    for (uint64_t i = 0; i < num_words - 1; i++) {
      data[i] = kAllOnesWord;
    }
    // The last word is special
    data[num_words - 1] = kAllOnesWord >> (num_words * kWordSizeBits - impl()->num_bits());
  }

  /**
   * Blindly set the bit at the given index to 0.
   * @param position The index of the bit to set.
   */
  void Unset(const uint32_t position) {
    TPL_ASSERT(position < impl()->num_bits(), "Index out of range");
    WordType *data = impl()->data_array();
    data[position / kWordSizeBits] &= ~(WordType(1) << (position % kWordSizeBits));
  }

  /**
   * Set all bits to 0.
   */
  void UnsetAll() {
    const auto num_bytes = impl()->num_words() * kWordSizeBytes;
    std::memset(impl()->data_array(), 0, num_bytes);
  }

  /**
   * Complement the value of the bit at the given index. If it is currently 1, it will be flipped
   * to 0; if it is currently 0, its flipped to 1.
   * @param position The index of the bit to flip.
   */
  void Flip(const uint32_t position) {
    TPL_ASSERT(position < impl()->num_bits(), "Index out of range");
    WordType *data = impl()->data_array();
    data[position / kWordSizeBits] ^= WordType(1) << (position % kWordSizeBits);
  }

  /**
   * Invert all bits.
   */
  void FlipAll() {
    WordType *data_array = impl()->data_array();
    const auto num_words = impl()->num_words();
    // Invert all words in vector except the last
    for (uint32_t i = 0; i < num_words - 1; i++) {
      data_array[i] = ~data_array[i];
    }
    // The last word is special
    const auto mask = kAllOnesWord >> (num_words * kWordSizeBits - impl()->num_bits());
    data_array[num_words - 1] = (mask & ~data_array[num_words - 1]);
  }

  /**
   * Check if any bit in the vector is non-zero.
   * @return True if any bit in the vector is set to 1; false otherwise.
   */
  bool Any() const {
    const WordType *data_array = impl()->data_array();
    for (uint32_t i = 0; i < impl()->num_words(); i++) {
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
    const WordType *data_array = impl()->data_array();
    const auto num_words = impl()->num_words();
    for (uint32_t i = 0; i < num_words - 1; i++) {
      if (data_array[i] != kAllOnesWord) {
        return false;
      }
    }
    const WordType hi_word = data_array[num_words - 1];
    return hi_word == kAllOnesWord >> (num_words * kWordSizeBits - impl()->num_bits());
  }

  /**
   * Check if all bits in the vector are zero.
   * @return True if all bits are set to 0; false otherwise.
   */
  bool None() const {
    const WordType *data_array = impl()->data_array();
    for (uint32_t i = 0; i < impl()->num_words(); i++) {
      if (data_array[i] != static_cast<WordType>(0)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Count the 1-bits in the bit vector.
   * @return The number of 1-bits in the bit vector.
   */
  uint32_t CountOnes() const {
    uint32_t count = 0;
    const WordType *data_array = impl()->data_array();
    for (uint32_t i = 0; i < impl()->num_words(); i++) {
      count += util::BitUtil::CountPopulation(data_array[i]);
    }
    return count;
  }

  /**
   * Return the index of the n-th 1-bit in this bit vector.
   * @param n Which 1-bit to look for.
   * @return The index of the n-th 1-bit. If there are fewer than @em n bits,
   *         return the size of the bit vector.
   */
  uint32_t NthOne(uint32_t n) const {
    const WordType *data_array = impl()->data_array();

    for (uint32_t i = 0; i < impl()->num_words(); i++) {
      const WordType word = data_array[i];
      const uint32_t count = BitUtil::CountPopulation(word);
      if (n < count) {
        const WordType mask = _pdep_u64(static_cast<WordType>(1) << n, word);
        const uint32_t pos = BitUtil::CountTrailingZeros(mask);
        return std::min(impl()->num_bits(), (i * kWordSizeBits) + pos);
      }
      n -= count;
    }
    return impl()->num_bits();
  }

  /**
   * Perform the bitwise intersection of this bit vector with the provided @em other bit vector.
   * @tparam T The CRTP type of the other bit vector.
   * @param other The bit vector to intersect with. Lengths must match exactly.
   */
  template <typename T>
  void Intersect(const BitVectorBase<T> &other) {
    TPL_ASSERT(impl()->num_bits() == other.impl()->num_bits(), "Mismatched bit vector size");
    WordType *data = impl()->data_array();
    const WordType *other_data = other.impl()->data_array();
    for (uint32_t i = 0; i < impl()->num_words(); i++) {
      data[i] &= other_data[i];
    }
  }

  /**
   * Perform the bitwise union of this bit vector with the provided @em other bit vector.
   * @tparam T The CRTP type of the other bit vector.
   * @param other The bit vector to union with. Lengths must match exactly.
   */
  template <typename T>
  void Union(const BitVectorBase<T> &other) {
    TPL_ASSERT(impl()->num_bits() == other.impl()->num_bits(), "Mismatched bit vector size");
    WordType *data = impl()->data_array();
    const WordType *other_data = other.impl()->data_array();
    for (uint32_t i = 0; i < impl()->num_words(); i++) {
      data[i] |= other_data[i];
    }
  }

  /**
   * Clear all bits in this bit vector whose corresponding bit is set in the provided bit vector.
   * @tparam T The CRTP type of the other bit vector.
   * @param other The bit vector to diff with. Lengths must match exactly.
   */
  template <typename T>
  void Difference(const BitVectorBase<T> &other) {
    TPL_ASSERT(impl()->num_bits() == other.impl()->num_bits(), "Mismatched bit vector size");
    WordType *data = impl()->data_array();
    const WordType *other_data = other.impl()->data_array();
    for (uint32_t i = 0; i < impl()->num_words(); i++) {
      data[i] &= ~other_data[i];
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
                  "Callback must be a single-argument functor accepting an "
                  "unsigned 32-bit index");

    const WordType *data_array = impl()->data_array();

    for (WordType i = 0; i < impl()->num_words(); i++) {
      WordType word = data_array[i];
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
    TPL_ASSERT(num_bytes == impl()->num_bits(), "Byte vector too small");
    util::VectorUtil::ByteVectorToBitVector(bytes, num_bytes, impl()->data_array());
  }

  /**
   * Return a string representation of this bit vector.
   * @return String representation of this vector.
   */
  std::string ToString() const {
    std::string result = "BitVector=[";
    bool first = true;
    for (uint32_t i = 0; i < impl()->num_bits(); i++) {
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

  template <typename T>
  Subclass &operator-=(const BitVectorBase<T> &other) {
    Difference(other);
    return *impl();
  }

  template <typename T>
  Subclass &operator&=(const BitVectorBase<T> &other) {
    Intersect(other);
    return *impl();
  }

  template <typename T>
  Subclass &operator|=(const BitVectorBase<T> &other) {
    Union(other);
    return *impl();
  }

  template <typename T>
  friend Subclass operator-(Subclass a, const BitVectorBase<T> &b) {
    a -= b;
    return a;
  }

  template <typename T>
  friend Subclass operator&(Subclass a, const BitVectorBase<T> &b) {
    a &= b;
    return a;
  }

  template <typename T>
  friend Subclass operator|(Subclass a, const BitVectorBase<T> &b) {
    a |= b;
    return a;
  }

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
  explicit BitVector(const uint32_t num_bits)
      : num_bits_(num_bits), num_words_(NumNeededWords(num_bits)) {
    TPL_ASSERT(num_bits_ > 0, "Cannot create bit vector with zero bits");
    data_array_ = std::make_unique<uint64_t[]>(num_words_);
    UnsetAll();
  }

  /**
   * Create a copy of the provided bit vector.
   * @param other The bit vector to copy.
   */
  BitVector(const BitVector &other) : num_bits_(other.num_bits_), num_words_(other.num_words_) {
    data_array_ = std::make_unique<uint64_t[]>(num_words_);
    for (uint32_t i = 0; i < num_words_; i++) {
      data_array_[i] = other.data_array_[i];
    }
  }

  /**
   * Move constructor.
   * @param other Move the given bit vector into this.
   */
  BitVector(BitVector &&other) = default;

  /**
   * Move assignment.
   * @param other The bit vector we're moving.
   * @return This vector.
   */
  BitVector &operator=(BitVector &&other) = default;

  /**
   * Copy the provided bit vector into this bit vector.
   * @param other The bit vector to copy.
   * @return This bit vector as a copy of the input vector.
   */
  BitVector &operator=(const BitVector &other) {
    if (num_bits() != other.num_bits()) {
      num_bits_ = other.num_bits_;
      num_words_ = other.num_words_;
      data_array_ = std::make_unique<uint64_t[]>(num_words_);
    }
    for (uint32_t i = 0; i < num_words_; i++) {
      data_array_[i] = other.data_array_[i];
    }
    return *this;
  }

  /**
   * Return the number of bits in the bit vector.
   */
  uint32_t num_bits() const { return num_bits_; }

  /**
   * Return the number of words used by the bit vector.
   */
  uint32_t num_words() const { return num_words_; }

  /**
   * Return a constant reference to the underlying word data.
   */
  const uint64_t *data_array() const { return data_array_.get(); }

  /**
   * Return a reference to the underlying word data.
   */
  uint64_t *data_array() { return data_array_.get(); }

 private:
  // The number of bits in the bit vector
  uint32_t num_bits_;
  // The number of words in the bit vector
  uint32_t num_words_;
  // The array of words making up the bit vector
  std::unique_ptr<uint64_t[]> data_array_;
};

/**
 * A view over an existing raw bit vector.
 */
class BitVectorView : public BitVectorBase<BitVectorView> {
 public:
  /**
   * Create a new bit vector that references the given raw bit vector without taking ownership.
   * @param unowned_data_array The externally managed bit vector.
   * @param num_bits The number of bits in the vector.
   */
  BitVectorView(uint64_t *const unowned_data_array, const uint32_t num_bits)
      : data_array_(unowned_data_array), num_bits_(num_bits) {
    TPL_ASSERT(data_array_ != nullptr, "Cannot create bit vector referencing NULL bitmap");
    TPL_ASSERT(num_bits_ > 0, "Cannot create bit vector with zero bits");
  }

  /**
   * Return the number of bits in the bit vector.
   */
  uint32_t num_bits() const { return num_bits_; }

  /**
   * Return the number of words used by the bit vector.
   */
  uint32_t num_words() const { return NumNeededWords(num_bits_); }

  /**
   * Return a constant reference to the underlying word data.
   */
  const uint64_t *data_array() const { return data_array_; }

  /**
   * Return a reference to the underlying word data.
   */
  uint64_t *data_array() { return data_array_; }

 private:
  // The array of words making up the bit vector
  uint64_t *data_array_;
  // The number of bits in the bit vector
  uint32_t num_bits_;
};

/**
 * A bit vector that stores the bit set data inline in the class.
 */
template <uint32_t NumBits>
class InlinedBitVector : public BitVectorBase<InlinedBitVector<NumBits>> {
  using Base = BitVectorBase<InlinedBitVector<NumBits>>;

  static_assert(NumBits % Base::kWordSizeBits == 0,
                "Inlined bit vectors only support vectors that are a multiple "
                "of the word size (i.e., 64 bits, 128 bits, etc.");

  constexpr static uint32_t kNumWords = Base::NumNeededWords(NumBits);

 public:
  /**
   * Construct a bit vector with all zeros.
   */
  InlinedBitVector() : data_array_{0} {}

  /**
   * Return the number of bits in the bit vector.
   */
  uint32_t num_bits() const { return NumBits; }

  /**
   * Return the number of words used by the bit vector.
   */
  uint32_t num_words() const { return kNumWords; }

  /**
   * Return a constant reference to the underlying word data.
   */
  const uint64_t *data_array() const { return data_array_; }

  /**
   * Return a reference to the underlying word data.
   */
  uint64_t *data_array() { return data_array_; }

 private:
  uint64_t data_array_[kNumWords];
};

}  // namespace tpl::util
