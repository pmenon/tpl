#include "sql/concise_hash_table.h"

#include "util/bit_util.h"
#include "util/memory.h"

namespace tpl::sql {

ConciseHashTable::ConciseHashTable(u32 probe_threshold)
    : slot_groups_(nullptr),
      num_groups_(0),
      probe_limit_(probe_threshold),
      num_overflow_(0),
      built_(false) {}

void ConciseHashTable::SetSize(const u32 num_elems) {
  // Ensure we have at least one slot group, meaning the minimum capacity is 64
  u64 capacity = std::max(64ul, util::MathUtil::PowerOf2Ceil(num_elems * 2));
  slot_mask_ = capacity - 1;
  num_groups_ = util::MathUtil::DivRoundUp(capacity, 64);
  slot_groups_ = util::mem::MallocHugeArray<SlotGroup>(num_groups_);
}

ConciseHashTable::~ConciseHashTable() {
  if (slot_groups_ != nullptr) {
    util::mem::FreeHugeArray(slot_groups_, num_groups_);
  }
}

void ConciseHashTable::Build() {
  if (is_built()) {
    return;
  }

  // Compute the prefix counts for each slot group

  slot_groups_[0].count =
      static_cast<u32>(util::BitUtil::CountBits(slot_groups_[0].bits));

  for (u32 i = 1; i < num_groups_; i++) {
    slot_groups_[i].count =
        slot_groups_[i - 1].count +
        static_cast<u32>(util::BitUtil::CountBits(slot_groups_[i].bits));
  }

  built_ = true;
}

}  // namespace tpl::sql
