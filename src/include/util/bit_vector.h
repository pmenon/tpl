#pragma once

#include "util/bit_util.h"
#include "util/common.h"

namespace tpl::util {

/**
 * A generic bit vector. This class can either allocate and own its bits, or can
 * reference an externally managed (and owned) bit set. Use the appropriate
 * constructor.
 */
class BitVector {
 public:
  /**
   * Create an uninitialized bit vector. Callers **MUST** use Init() before
   * interacting with the BitVector.
   */
  BitVector() : owned_bits_(nullptr), bits_(nullptr), num_bits_(0) {}

  /**
   * Create a new bit vector with the specified number of bits.
   * @param num_bits The number of bits in the vector.
   */
  explicit BitVector(u32 num_bits, bool clear = true)
      : owned_bits_(
            std::make_unique<u32[]>(BitUtil::Num32BitWordsFor(num_bits))),
        bits_(owned_bits_.get()),
        num_bits_(num_bits) {
    if (clear) {
      ClearAll();
    }
  }

  /**
   * Create a new bit vector that references the given raw bit vector without
   * taking ownership.
   * @param unowned_bits The externally managed bit vector.
   * @param num_bits The number of bits in the vector.
   */
  BitVector(u32 *unowned_bits, u32 num_bits)
      : owned_bits_(nullptr), bits_(unowned_bits), num_bits_(num_bits) {}

  /**
   * Initialize a new bit vector with the given size.
   * @param num_bits The number of bits to size the vector with.
   */
  void Init(u32 num_bits) {
    owned_bits_ = std::make_unique<u32[]>(BitUtil::Num32BitWordsFor(num_bits));
    bits_ = owned_bits_.get();
    num_bits_ = num_bits;
  }

  /**
   * Test if the bit at the provided index is set.
   * @return True if the bit is set; false otherwise.
   */
  bool Test(const u32 idx) const {
    TPL_ASSERT(idx < num_bits_, "Index out of range");
    return BitUtil::Test(bits_, idx);
  }

  /**
   * Blindly set the bit at the given index to 1.
   * @param idx The index of the bit to set.
   */
  void Set(const u32 idx) {
    TPL_ASSERT(idx < num_bits_, "Index out of range");
    return BitUtil::Set(bits_, idx);
  }

  /**
   * Set the bit at the given index to 1 if @em val is true, and 0 otherwise.
   * @param idx The index of the bit to modify.
   * @param val The value to the set bit; 1 if true, 0 if false.
   */
  void SetTo(const u32 idx, const bool val) {
    TPL_ASSERT(idx < num_bits_, "Index out of range");
    return BitUtil::SetTo(bits_, idx, val);
  }

  /**
   * Blindly set the bit at the given index to 0.
   * @param idx The index of the bit to set.
   */
  void Unset(const u32 idx) {
    TPL_ASSERT(idx < num_bits_, "Index out of range");
    return BitUtil::Unset(bits_, idx);
  }

  /**
   * Complement the value of the bit at the given index. If it is currently 1,
   * it will be flipped to 0; if it is currently 0, its flipped to 1.
   * @param idx The index of the bit to flip.
   */
  void Flip(const u32 idx) {
    TPL_ASSERT(idx < num_bits_, "Index out of range");
    return BitUtil::Flip(bits_, idx);
  }

  /**
   * Write zeroes to all bits in the bit vector.
   */
  void ClearAll() { return BitUtil::Clear(bits_, num_bits_); }

  /**
   * Return the number of bits in the bit vector.
   */
  u32 GetNumBits() const noexcept { return num_bits_; }

  /**
   * Access the boolean value of the bit a the given index using an array
   * operator.
   * @param idx The index of the bit to read.
   * @return True if the bit is set; false otherwise.
   */
  bool operator[](const u32 idx) const { return Test(idx); }

 private:
  // If this vector allocated its bits, this pointer owns it
  std::unique_ptr<u32[]> owned_bits_;

  // The array of bits
  u32 *bits_;

  // The number of bits in the bit vector
  u32 num_bits_;
};

/**
 * A bit vector that stores the bit set data inline in the class.
 */
template <u32 NumBits>
class InlinedBitVector : public BitVector {
  static_assert(NumBits % BitUtil::kBitWordSize == 0,
                "Inlined bit vectors only support vectors that are a multiple "
                "of the word size (i.e., 32 bits, 64 bits, 128 bits, etc.");

  static constexpr u32 kNumWords = NumBits / BitUtil::kBitWordSize;

 public:
  InlinedBitVector() : BitVector(bits_, NumBits), bits_{0} {}

 private:
  u32 bits_[kNumWords];
};

}  // namespace tpl::util