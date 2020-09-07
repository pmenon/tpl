#include "sql/compact_storage.h"

#include <numeric>

#include "util/math_util.h"

namespace tpl::sql {

CompactStorage::CompactStorage(const std::vector<TypeId> &schema) {
  std::vector<uint32_t> reordered(schema.size()), reordered_offsets(schema.size());
  std::iota(reordered.begin(), reordered.end(), 0u);

  // Re-order attributes by decreasing size to minimize padding.
  std::sort(reordered.begin(), reordered.end(), [&](auto left_idx, auto right_idx) {
    return GetTypeIdSize(schema[left_idx]) > GetTypeIdSize(schema[right_idx]);
  });

  // Calculate the byte offset of each element in the reordered attribute list.
  for (std::size_t i = 0, offset = 0; i < schema.size(); i++) {
    reordered_offsets[i] = offset;
    offset += GetTypeIdSize(schema[reordered[i]]);
  }

  // The preferred alignment for the storage is the preferred alignment of the
  // largest element in the schema. This happens to be the first element in the
  // reordered element vector.
  preferred_alignment_ = GetTypeIdAlignment(schema[reordered[0]]);

  // For each element in the schema, create an element in the offsets vector
  // containing its byte offset from the start it is stored at.
  offsets_.resize(schema.size());
  for (std::size_t i = 0; i < reordered.size(); i++) {
    offsets_[reordered[i]] = reordered_offsets[i];
  }

#ifndef NDEBUG
  // Validate.
  for (std::size_t i = 0; i < schema.size(); i++) {
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
