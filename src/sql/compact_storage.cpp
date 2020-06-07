#include "sql/compact_storage.h"

#include <algorithm>
#include <numeric>

#include "util/math_util.h"

namespace tpl::sql {

CompactStorage::CompactStorage(const std::vector<TypeId> &schema) {
  // Create a reordered vector of indexes.
  std::vector<uint32_t> reordered;
  reordered.resize(schema.size());
  std::iota(reordered.begin(), reordered.end(), 0u);
  // Order it by decreasing size to minimize padding due to alignment.
  std::sort(reordered.begin(), reordered.end(), [&](auto left_idx, auto right_idx) {
    return GetTypeIdSize(schema[left_idx]) > GetTypeIdSize(schema[right_idx]);
  });

  // Calculate the byte offset of each element using an exclusive prefix sum.
  std::vector<std::size_t> reordered_offsets;
  reordered_offsets.resize(schema.size());
  std::transform_exclusive_scan(reordered.begin(), reordered.end(), reordered_offsets.begin(),
                                std::size_t{0}, std::plus<std::size_t>{},
                                [&](auto idx) { return GetTypeIdSize(schema[idx]); });

  // The preferred alignment for the storage is the preferred alignment of the
  // largest element in the schema. This happens to be the first element in the
  // reordered element vector.
  preferred_alignment_ = GetTypeIdAlignment(schema[reordered[0]]);

  // For each element in the schema, create an element in the offsets vector
  // containing its byte offset from the start it is stored at.
  offsets_.resize(schema.size());
  for (uint32_t i = 0; i < reordered.size(); i++) {
    offsets_[reordered[i]] = reordered_offsets[i];
  }

#ifndef NDEBUG
  // Validate.
  for (uint32_t i = 0; i < schema.size(); i++) {
    TPL_ASSERT(util::MathUtil::IsAligned(offsets_[i], GetTypeIdAlignment(schema[i])),
               "Type is not aligned correctly!");
  }
#endif

  // The NULL bitmap is at the end of the storage.
  null_bitmap_offset_ = reordered_offsets.back() + GetTypeIdSize(schema[reordered.back()]);
}

uint32_t CompactStorage::GetNumElements() const { return offsets_.size(); }

std::size_t CompactStorage::GetPreferredAlignment() const { return preferred_alignment_; }

std::size_t CompactStorage::GetRequiredSize() const {
  const auto num_null_bytes = util::MathUtil::DivRoundUp(GetNumElements(), 8);
  return null_bitmap_offset_ + num_null_bytes;
}

}  // namespace tpl::sql
