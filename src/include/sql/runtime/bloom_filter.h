#pragma once

#include <memory>

#include "util/bit_util.h"
#include "util/common.h"
#include "util/macros.h"

namespace tpl::sql::runtime {

class BloomFilter {
  // The set of salt values we use to produce alternative hash values
  static constexpr const u32 kSalts[8] = {0x47b6137bU, 0x44974d91U, 0x8824ad5bU,
                                          0xa2b7289dU, 0x705495c7U, 0x2df1424bU,
                                          0x9efc4947U, 0x5c6bfb31U};

  static constexpr const u32 kBitsPerElement = 8;

 public:
  // A block in this filter (i.e., the sizes of the bloom filter partitions)
  using Block = u32[8];

 public:
  /// Create an uninitialized bloom filter
  BloomFilter();

  /// Initialize this filter with the given size
  explicit BloomFilter(u32 num_elems);

  /// This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(BloomFilter);

  /// Initialize this BloomFilter with the given size
  void Init(u32 num_elems);

  /// Add an element to the bloom filter
  void Add(hash_t hash);

  /// Check if the given element is contained in the filter
  /// \return True if the hash may be in the filter; false if definitely not
  bool Contains(hash_t hash) const;

  /// Return the size of this Bloom Filter in bytes
  u64 GetSizeInBytes() const { return sizeof(Block) * num_blocks(); }

  /// Return the number of bits set in the Bloom Filter
  u64 GetTotalBitsSet() const;

  /// Get the number of bits this Bloom Filter has
  u64 GetNumBits() const { return GetSizeInBytes() * kBitsPerByte; }

 private:
  // -------------------------------------------------------
  // Accessors
  // -------------------------------------------------------

  u32 num_blocks() const { return block_mask() + 1; }

  u32 block_mask() const { return block_mask_; }

 private:
  std::unique_ptr<Block[]> blocks_;

  u32 block_mask_;
};

inline BloomFilter::BloomFilter() : blocks_(nullptr), block_mask_(0) {}

inline BloomFilter::BloomFilter(u32 num_elems) : BloomFilter() {
  Init(num_elems);
}

inline void BloomFilter::Init(u32 num_elems) {
  u64 num_bits = kBitsPerElement * num_elems;
  if (!util::MathUtil::IsPowerOf2(num_bits)) {
    num_bits = util::MathUtil::NextPowerOf2(num_bits);
  }
  u64 num_blocks =
      util::MathUtil::DivRoundUp(num_bits, sizeof(Block) * kBitsPerByte);
  blocks_ = std::make_unique<Block[]>(num_blocks);
  block_mask_ = static_cast<u32>(num_blocks - 1);
  TPL_MEMSET(blocks_.get(), 0, num_blocks * sizeof(Block));
}

inline void BloomFilter::Add(hash_t hash) {
  u32 block_idx = static_cast<u32>(hash & block_mask());
  Block &block = blocks_[block_idx];
  u32 alt_hash = static_cast<u32>(hash >> 32);
  for (u32 i = 0; i < 8; i++) {
    u32 bit_idx = (alt_hash * kSalts[i]) >> 27;
    util::BitUtil::Set(&block[i], bit_idx);
  }
}

inline bool BloomFilter::Contains(hash_t hash) const {
  u32 alt_hash = static_cast<u32>(hash >> 32);
  u32 block_idx = static_cast<u32>(hash & block_mask());

  Block &block = blocks_[block_idx];
  for (u32 i = 0; i < 8; i++) {
    u32 bit_idx = (alt_hash * kSalts[i]) >> 27;
    if (!util::BitUtil::Test(&block[i], bit_idx)) {
      return false;
    }
  }

  return true;
}

inline u64 BloomFilter::GetTotalBitsSet() const {
  u64 count = 0;
  for (u32 i = 0; i < num_blocks(); i++) {
    const u64 *bits = reinterpret_cast<const u64 *>(blocks_[i]);
    for (u32 j = 0; j < 4; j++) {
      count += util::BitUtil::CountBits(bits[j]);
    }
  }
  return count;
}

}  // namespace tpl::sql::runtime