#include "sql/concise_hash_table.h"

#include "util/bit_util.h"
#include "util/memory.h"

namespace tpl::sql {

ConciseHashTable::ConciseHashTable(u32 probe_threshold)
    : slot_groups_(nullptr),
      num_groups_(0),
      probe_threshold_(probe_threshold),
      built_(false) {}

void ConciseHashTable::SetSize(const u32 num_elems) {
  u64 capacity = util::MathUtil::PowerOf2Ceil(num_elems * 2);
  slot_mask_ = capacity - 1;
  num_groups_ = util::MathUtil::DivRoundUp(capacity, 64);
  slot_groups_ = util::mem::MallocHugeArray<SlotGroup>(num_groups_);
}

ConciseHashTable::~ConciseHashTable() {
  if (slot_groups_ != nullptr) {
    util::mem::FreeHugeArray(slot_groups_, num_groups());
  }
}

void ConciseHashTable::Build() {
  if (is_built()) {
    return;
  }

  // Compute the prefix counts for each slot group

  slot_groups_[0].count =
      static_cast<u32>(util::BitUtil::CountBits(slot_groups_[0].bits));

  for (u32 i = 1; i < num_groups(); i++) {
    slot_groups_[i].count =
        slot_groups_[i - 1].count +
        static_cast<u32>(util::BitUtil::CountBits(slot_groups_[i].bits));
  }

  set_is_built();
}

ConciseHashTableSlot ConciseHashTable::Insert(const hash_t hash) {
  u32 slot_pos = static_cast<u32>(hash & slot_mask());
  SlotGroup *slot_group = slot_groups_ + (slot_pos / 64);
  auto *group_bits = reinterpret_cast<u32 *>(&slot_group->bits);

  for (u32 bit_pos = slot_pos % 64,
           max_bit_pos = std::min(63u, bit_pos + probe_threshold());
       bit_pos < max_bit_pos; bit_pos++) {
    if (!util::BitUtil::Test(group_bits, bit_pos)) {
      util::BitUtil::Set(group_bits, bit_pos);
      return ConciseHashTableSlot::Make(slot_pos);
    }
  }

  return ConciseHashTableSlot::MakeOverflow();
}

ConciseHashTableSlot ConciseHashTable::FindFirst(const hash_t hash) const {
  u32 slot_pos = static_cast<u32>(hash & slot_mask());
  SlotGroup *slot_group = slot_groups_ + (slot_pos / 64);
  auto *group_bits = reinterpret_cast<u32 *>(&slot_group->bits);

  for (u32 bit_pos = slot_pos % 64,
           max_bit_pos = std::max(63u, bit_pos + probe_threshold());
       bit_pos < max_bit_pos; bit_pos++) {
    if (util::BitUtil::Test(group_bits, bit_pos)) {
      return ConciseHashTableSlot::Make(slot_pos);
    }
  }

  return ConciseHashTableSlot::MakeOverflow();
}

std::string ConciseHashTable::PrettyPrint() const {
  std::string result;

  for (u32 idx = 0; idx < num_groups(); idx++) {
    SlotGroup *slot_group = slot_groups_ + idx;
    auto *group_bits = reinterpret_cast<u32 *>(&slot_group->bits);
    for (u32 j = 0; j < 64; j++) {
      result += (util::BitUtil::Test(group_bits, j) ? "1" : "0");
    }
    result += ",";
    result += std::to_string(slot_group->count);
    result += "\n";
  }

  return result;
}

}  // namespace tpl::sql
