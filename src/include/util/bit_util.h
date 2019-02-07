#pragma once

#include "util/common.h"
#include "util/macros.h"
#include "util/math_util.h"

namespace tpl::util {

class BitUtil {
  // The number of bits in one word
  static constexpr const u32 kBitWordSize = sizeof(u32) * kBitsPerByte;

  // Make sure the number of bits in a word is a power of two to make all these
  // bit operations cheap
  static_assert(util::MathUtil::IsPowerOf2(kBitWordSize));

 public:
  /// Calculate the number of 32-bit words are needed to store a bit vector of
  /// the given size
  /// \param num_bits The size of the bit vector, in bits
  /// \return The number of words needed to store a bit vector of the given size
  static u64 Num32BitWordsFor(u64 num_bits) {
    return MathUtil::DivRoundUp(num_bits, kBitWordSize);
  }

  /// Test if the bit at index \p idx is set in the bit vector
  /// \param bits The bit vector
  /// \param idx The index of the bit to check
  /// \return True if set; false otherwise
  static bool Test(const u32 bits[], const u32 idx) {
    u32 mask = 1u << (idx % kBitWordSize);
    return (bits[idx / kBitWordSize] & mask) != 0;
  }

  /// Set the bit at index \p idx to 1 in the bit vector \p bits
  /// \param bits The bit vector
  /// \param idx The index of the bit to set to 1
  static void Set(u32 bits[], const u32 idx) {
    bits[idx / kBitWordSize] |= 1u << (idx % kBitWordSize);
  }

  /// Set the bit at index \p idx to 0 in the bit vector \p bits
  /// \param bits The bit vector
  /// \param idx The index of the bit to unset
  static void Unset(u32 bits[], const u32 idx) {
    bits[idx / kBitWordSize] &= ~(1u << (idx % kBitWordSize));
  }

  /// Flip the value of the bit at index \p idx in the bit vector
  /// \param bits The bit vector
  /// \param idx The index of the bit to flip
  static void Flip(u32 bits[], const u32 idx) {
    bits[idx / kBitWordSize] ^= 1u << (idx % kBitWordSize);
  }

  /// Clear all bits in the bit vector
  /// \param bits The bit vector
  /// \param size The number of elements in the bit vector
  static void Clear(u32 bits[], const u64 size) {
    auto num_bytes = sizeof(u32) * Num32BitWordsFor(size);
    TPL_MEMSET(bits, 0, num_bytes);
  }

  /// Count the number of set bits in the given value
  template <typename T>
  static u64 CountBits(T val) { return llvm::countPopulation(val); }
};

class BitVector {
 public:
  // Create an uninitialized bit vector. Callers **must** use Init() before
  // interacting with the BitVector
  BitVector() : owned_bits_(nullptr), bits_(nullptr), num_bits_(0) {}

  // Create a new BitVector with the specified number of bits
  explicit BitVector(u32 num_bits)
      : owned_bits_(
            std::make_unique<u32[]>(BitUtil::Num32BitWordsFor(num_bits))),
        bits_(owned_bits_.get()),
        num_bits_(num_bits) {
    ClearAll();
  }

  // Take over the given bits
  BitVector(std::unique_ptr<u32[]> bits, u32 num_bits)
      : owned_bits_(std::move(bits)),
        bits_(owned_bits_.get()),
        num_bits_(num_bits) {}

  // Provide bit vector access to the given bits, not taking ownership of them
  BitVector(u32 unowned_bits[], u32 num_bits)
      : owned_bits_(nullptr), bits_(unowned_bits), num_bits_(num_bits) {}

  void Init(u32 num_bits) {
    owned_bits_ = std::make_unique<u32[]>(BitUtil::Num32BitWordsFor(num_bits));
    bits_ = owned_bits_.get();
    num_bits_ = num_bits;
  }

  bool Test(u32 idx) const {
    TPL_ASSERT(idx < num_bits(), "Index out of range");
    return BitUtil::Test(bits(), idx);
  }

  void Set(u32 idx) {
    TPL_ASSERT(idx < num_bits(), "Index out of range");
    return BitUtil::Set(bits(), idx);
  }

  void Unset(u32 idx) {
    TPL_ASSERT(idx < num_bits(), "Index out of range");
    return BitUtil::Unset(bits(), idx);
  }

  void Flip(u32 idx) {
    TPL_ASSERT(idx < num_bits(), "Index out of range");
    return BitUtil::Flip(bits(), idx);
  }

  void ClearAll() { return BitUtil::Clear(bits(), num_bits()); }

  bool operator[](u32 idx) const { return Test(idx); }

  u32 num_bits() const { return num_bits_; }

 private:
  // -------------------------------------------------------
  // Accessors
  // -------------------------------------------------------

  u32 *bits() { return bits_; }

  const u32 *bits() const { return bits_; }

 private:
  std::unique_ptr<u32[]> owned_bits_;

  u32 *bits_;

  u32 num_bits_;
};

}  // namespace tpl::util