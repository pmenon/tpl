#include "sql/concise_hash_table.h"

#include <algorithm>

#include "util/bit_util.h"

namespace tpl::sql {

ConciseHashTable::ConciseHashTable(uint32_t probe_threshold)
    : slot_groups_(nullptr),
      num_groups_(0),
      probe_limit_(probe_threshold),
      num_overflow_(0),
      built_(false) {}

ConciseHashTable::~ConciseHashTable() {
  if (slot_groups_ != nullptr) {
    util::FreeHugeArray(slot_groups_, num_groups_);
  }
}

void ConciseHashTable::SetSize(const uint32_t num_elems) {
  if (slot_groups_ != nullptr) {
    util::FreeHugeArray(slot_groups_, num_groups_);
  }

  uint64_t capacity =
      std::max(kMinNumSlots, util::MathUtil::PowerOf2Floor(num_elems * kLoadFactor));
  slot_mask_ = capacity - 1;
  num_groups_ = capacity >> kLogSlotsPerGroup;
  slot_groups_ = util::MallocHugeArray<SlotGroup>(num_groups_);
}

void ConciseHashTable::Build() {
  if (is_built()) {
    return;
  }

  // Compute the prefix counts for each slot group

  slot_groups_[0].count = util::BitUtil::CountPopulation(slot_groups_[0].bits);

  for (uint32_t i = 1; i < num_groups_; i++) {
    slot_groups_[i].count =
        slot_groups_[i - 1].count + util::BitUtil::CountPopulation(slot_groups_[i].bits);
  }

  built_ = true;
}

}  // namespace tpl::sql
