#include "sql/runtime/bloom_filter.h"

namespace tpl::sql::runtime {

BloomFilter::BloomFilter()
    : region_(nullptr),
      blocks_(nullptr),
      block_mask_(0),
      lazily_added_hashes_(nullptr) {}

BloomFilter::BloomFilter(util::Region *region)
    : region_(region),
      blocks_(nullptr),
      block_mask_(0),
      lazily_added_hashes_(nullptr) {}

BloomFilter::BloomFilter(util::Region *region, u32 num_elems) : BloomFilter() {
  Init(region, num_elems);
}

void BloomFilter::Init(util::Region *region, u32 num_elems) {
  region_ = region;
  lazily_added_hashes_ = util::RegionVector<hash_t>(region);

  u64 num_bits = kBitsPerElement * num_elems;
  if (!util::MathUtil::IsPowerOf2(num_bits)) {
    num_bits = util::MathUtil::NextPowerOf2(num_bits);
  }
  u64 num_blocks =
      util::MathUtil::DivRoundUp(num_bits, sizeof(Block) * kBitsPerByte);
  u64 num_bytes = num_blocks * sizeof(Block);
  blocks_ =
      reinterpret_cast<Block *>(region->Allocate(num_bytes, CACHELINE_SIZE));
  TPL_MEMSET(blocks_, 0, num_bytes);

  block_mask_ = static_cast<u32>(num_blocks - 1);
}

u64 BloomFilter::GetTotalBitsSet() const {
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