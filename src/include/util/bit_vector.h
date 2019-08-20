#pragma once

#include <immintrin.h>
#include <algorithm>
#include <memory>
#include <string>

#include "util/bit_util.h"
#include "util/common.h"
#include "util/math_util.h"
#include "util/vector_util.h"

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
  // Word value with all ones.
  static constexpr WordType kAllOnesWord = ~static_cast<WordType>(0);

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
    const WordType *const data = impl()->data_array();
    const WordType mask = WordType(1) << (position % kWordSizeBits);
    return data[position / kWordSizeBits] & mask;
  }

  /**
   * Blindly set the bit at the given index to 1.
   * @param position The index of the bit to set.
   */
  void Set(const u32 position) {
    TPL_ASSERT(position < impl()->num_bits(), "Index out of range");
    WordType *const data = impl()->data_array();
    data[position / kWordSizeBits] |= WordType(1) << (position % kWordSizeBits);
  }

  /**
   * Set the bit at the given position to a given value.
   * @param position The index of the bit to set.
   * @param v The value to set the bit to.
   */
  void SetTo(const u32 position, const bool v) {
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
  WordType GetWord(const u32 word_position) const {
    TPL_ASSERT(word_position < impl()->num_words(), "Index out of range");
    return impl()->data_array()[word_position];
  }

  /**
   * Set the value of the word at the given word index to the provided value.
   * If the size of the bit vector is not a multiple of the word size, the tail
   * bits are masked off.
   * @param word_position The index of the word to set.
   * @param word_val The value to set.
   */
  void SetWord(const u32 word_position, const WordType word_val) {
    TPL_ASSERT(word_position < impl()->num_words(), "Index out of range");
    WordType *const data = impl()->data_array();
    const u32 num_words = impl()->num_words();
    data[word_position] = word_val;
    if (word_position == num_words - 1) {
      data[num_words - 1] &=
          kAllOnesWord >> (num_words * kWordSizeBits - impl()->num_bits());
    }
  }

  /**
   * Set all bits to 1.
   */
  void SetAll() {
    auto *data = impl()->data_array();
    const auto num_words = impl()->num_words();
    // Set all bits in all words but the last
    std::memset(data, 255, kWordSizeBytes * (num_words - 1));
    // The last word is special
    data[num_words - 1] =
        kAllOnesWord >> (num_words * kWordSizeBits - impl()->num_bits());
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
   * Set all bits to 0.
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
   * Invert all bits.
   */
  void FlipAll() {
    auto *data_array = impl()->data_array();
    const auto num_words = impl()->num_words();
    // Invert all words in vector except the last
    for (u32 i = 0; i < num_words - 1; i++) {
      data_array[i] = ~data_array[i];
    }
    // The last word is special
    const auto mask =
        kAllOnesWord >> (num_words * kWordSizeBits - impl()->num_bits());
    data_array[num_words - 1] = (mask & ~data_array[num_words - 1]);
  }

  /**
   * Check if any bit in the vector is non-zero.
   * @return True if any bit in the vector is set to 1; false otherwise.
   */
  bool Any() const {
    const WordType *data_array = impl()->data_array();
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
    const WordType *data_array = impl()->data_array();
    const auto num_words = impl()->num_words();
    for (u32 i = 0; i < num_words - 1; i++) {
      if (data_array[i] != kAllOnesWord) {
        return false;
      }
    }
    const WordType hi_word = data_array[num_words - 1];
    return hi_word ==
           kAllOnesWord >> (num_words * kWordSizeBits - impl()->num_bits());
  }

  /**
   * Check if all bits in the vector are zero.
   * @return True if all bits are set to 0; false otherwise.
   */
  bool None() const {
    const WordType *data_array = impl()->data_array();
    for (u32 i = 0; i < impl()->num_words(); i++) {
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
  u32 CountOnes() const {
    u32 count = 0;
    const WordType *data_array = impl()->data_array();
    for (u32 i = 0; i < impl()->num_words(); i++) {
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
  u32 NthOne(u32 n) const {
    const WordType *data_array = impl()->data_array();

    for (u32 i = 0; i < impl()->num_words(); i++) {
      const WordType word = data_array[i];
      const u32 count = BitUtil::CountPopulation(word);
      if (n < count) {
        const WordType mask = _pdep_u64(static_cast<WordType>(1) << n, word);
        const u32 pos = BitUtil::CountTrailingZeros(mask);
        return std::min(impl()->num_bits(), (i * kWordSizeBits) + pos);
      }
      n -= count;
    }
    return impl()->num_bits();
  }

  /**
   * Perform the bitwise intersection of this bit vector with the provided
   * @em other bit vector, modifying this bit vector in-place.
   * @tparam T The CRTP type of the other bit vector.
   * @param other The bit vector to intersect with. Lengths must match exactly.
   */
  template <typename T>
  void Intersect(const BitVectorBase<T> &other) {
    TPL_ASSERT(impl()->num_bits() == other.impl()->num_bits(),
               "Mismatched bit vector size");
    auto *data = impl()->data_array();
    auto *other_data = other.impl()->data_array();
    for (u32 i = 0; i < impl()->num_words(); i++) {
      data[i] &= other_data[i];
    }
  }

  /**
   * Perform the bitwise union of this bit vector with the provided @em other
   * bit vector, modifying this bit vector in-place.
   * @tparam T The CRTP type of the other bit vector.
   * @param other The bit vector to union with. Lengths must match exactly.
   */
  template <typename T>
  void Union(const BitVectorBase<T> &other) {
    TPL_ASSERT(impl()->num_bits() == other.impl()->num_bits(),
               "Mismatched bit vector size");
    auto *data = impl()->data_array();
    auto *other_data = other.impl()->data_array();
    for (u32 i = 0; i < impl()->num_words(); i++) {
      data[i] |= other_data[i];
    }
  }

  /**
   * Clear all bits in this bit vector whose corresponding bit is set in the
   * provided bit vector.
   * @tparam T The CRTP type of the other bit vector.
   * @param other The bit vector to diff with. Lengths must match exactly.
   */
  template <typename T>
  void Difference(const BitVectorBase<T> &other) {
    TPL_ASSERT(impl()->num_bits() == other.impl()->num_bits(),
               "Mismatched bit vector size");
    auto *data = impl()->data_array();
    auto *other_data = other.impl()->data_array();
    for (u32 i = 0; i < impl()->num_words(); i++) {
      data[i] &= ~other_data[i];
    }
  }

  /**
   * Iterate all bits in this vector and invoke the callback with the index of
   * set bits only.
   * @tparam F The type of the callback function. Must accept a single unsigned
   *           integer value.
   * @param callback The callback function to invoke with the index of set bits.
   */
  template <typename F>
  void IterateSetBits(F &&callback) const {
    static_assert(std::is_invocable_v<F, u32>,
                  "Callback must be a single-argument functor accepting an "
                  "unsigned 32-bit index");

    const WordType *data_array = impl()->data_array();

    for (u32 i = 0; i < impl()->num_words(); i++) {
      WordType word = data_array[i];
      while (word != 0) {
        const WordType t = word & -word;
        const u32 r = BitUtil::CountTrailingZeros(word);
        callback(i * kWordSizeBits + r);
        word ^= t;
      }
    }
  }

  /**
   * Populate this bit vector from the values stored in the given bytes. The
   * byte array is assumed to be a "saturated" match vector, i.e., true values
   * are all 1's (255 = 11111111 = std::numeric_limits<u8>::max()), and false
   * values are all 0.
   * @param bytes The array of saturated bytes to read.
   * @param num_bytes The number of bytes in the input array.
   */
  void SetFromBytes(const u8 *const bytes, const u32 num_bytes) {
    TPL_ASSERT(bytes != nullptr, "Null input");
    TPL_ASSERT(num_bytes == impl()->num_bits(), "Byte vector too small");
    util::VectorUtil::ByteVectorToBitVector(bytes, num_bytes,
                                            impl()->data_array());
  }

  /**
   * Return a string representation of this bit vector.
   * @return String representation of this vector.
   */
  std::string ToString() const {
    std::string result = "BitVector=[";
    bool first = true;
    for (u32 i = 0; i < impl()->num_bits(); i++) {
      if (!first) result += ",";
      first = false;
      result += Test(i) ? "1" : "0";
    }
    result += "]";
    return result;
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
   * Take a slice from this bit vector starting at bit position @em offset and
   * assuming @em size bits. The bit position must be a multiple of a word,
   * i.e., 64, 128, 256, etc.
   * @param offset The bit offset from the start to begin the slice from.
   * @param size The number of bits to size the slice.
   * @return
   */
  BitVector Slice(const u32 offset, const u32 size) {
    TPL_ASSERT(offset / kWordSizeBits < num_words_, "Out-of-bounds access");
    TPL_ASSERT(offset % kWordSizeBits == 0, "Invalid offset");
    const auto word_idx = offset / kWordSizeBits;
    const auto len = std::min(num_bits_ - (word_idx * kWordSizeBits), size);
    return BitVector(data_array_ + word_idx, len);
  }

  /**
   * Return the number of bits in the bit vector.
   */
  u32 num_bits() const { return num_bits_; }

  /**
   * Return the number of words used by the bit vector.
   */
  u32 num_words() const { return num_words_; }

  /**
   * Return a constant reference to the underlying word data.
   */
  const u64 *data_array() const { return data_array_; }

  /**
   * Return a reference to the underlying word data.
   */
  u64 *data_array() { return data_array_; }

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

  /**
   * Return a constant reference to the underlying word data.
   */
  const u64 *data_array() const { return data_array_; }

  /**
   * Return a reference to the underlying word data.
   */
  u64 *data_array() { return data_array_; }

 private:
  u64 data_array_[kNumWords];
};

}  // namespace tpl::util
