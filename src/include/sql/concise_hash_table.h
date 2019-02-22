#pragma once

#include <memory>

#include "sql/hash_table_entry.h"
#include "util/bit_util.h"
#include "util/common.h"

namespace tpl::sql {

class ConciseHashTable {
 public:
  // The maximum probe length before falling back into the overflow table
  static constexpr const u32 kProbeThreshold = 2;

  // The number of CHT slots that belong to one group. This value should either
  // be 32 or 64 for (1) making computation simpler by bit-shifting and (2) to
  // ensure at most one cache-line read/write per insert/lookup.
  static constexpr const u32 kLogSlotsPerGroup = 6;
  static constexpr const u32 kSlotsPerGroup = 1u << kLogSlotsPerGroup;
  static constexpr const u32 kGroupBitMask = kSlotsPerGroup - 1;

  /// Create a new uninitialized concise hash table. Callers **must** call
  /// SetSize() before interacting with the table
  explicit ConciseHashTable(u32 probe_threshold = kProbeThreshold);

  /// Destroy
  ~ConciseHashTable();

  /// This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(ConciseHashTable)

  /// Set the size of the hash table to support at least \a num_elems elements
  void SetSize(u32 num_elems);

  /// Insert an element with the given hash into the table and return an encoded
  /// slot position
  ConciseHashTableSlot Insert(hash_t hash);

  /// Finalize and build this concise hash table
  void Build();

  /// Return the number of occupied slots in the table **before** the given slot
  u64 NumFilledSlotsBefore(ConciseHashTableSlot slot) const;

  // -------------------------------------------------------
  // Utility Operations
  // -------------------------------------------------------

  /// Return the number of bytes this hash table has allocated
  u64 GetTotalMemoryUsage() const { return sizeof(SlotGroup) * num_groups_; }

  // -------------------------------------------------------
  // Accessors
  // -------------------------------------------------------

  /// Return the capacity (the maximum number of elements) this table supports
  u64 capacity() const { return slot_mask_ + 1; }

  /// Return the number of overflows entries in this table
  u64 num_overflow() const { return num_overflow_; }

  /// Has the table been built?
  bool is_built() const { return built_; }

 private:
  // The concise hash table is composed of multiple groups of slots. This struct
  // captures this concept
  struct SlotGroup {
    // The bitmap indicating whether the slots are occupied or free
    u64 bits;
    // The prefix population count
    u32 count;

    static_assert(
        sizeof(bits) * kBitsPerByte == kSlotsPerGroup,
        "Number of slots in group and configured constant are out of sync");
  };

 private:
  // The array of groups. This array is managed by this class.
  SlotGroup *slot_groups_;

  // The number of groups (of slots) in the table
  u64 num_groups_;

  // The mask used to find a slot in the hash table
  u64 slot_mask_;

  // The maximum number of slots to probe
  u32 probe_limit_;

  // The number of entries in the overflow table
  u32 num_overflow_;

  // Flag indicating if the hash table has been built and is frozen (read-only)
  bool built_;
};

// ---------------------------------------------------------
// Implementation below
// ---------------------------------------------------------

inline ConciseHashTableSlot ConciseHashTable::Insert(const hash_t hash) {
  const u64 slot_idx = hash & slot_mask_;
  const u64 group_idx = slot_idx >> kLogSlotsPerGroup;
  const u64 num_bits_to_group = group_idx * kSlotsPerGroup;
  u32 *group_bits = reinterpret_cast<u32 *>(&slot_groups_[group_idx].bits);

  u32 bit_idx = static_cast<u32>(slot_idx & kGroupBitMask);
  u32 max_bit_idx = (bit_idx + probe_limit_) & kGroupBitMask;
  do {
    if (!util::BitUtil::Test(group_bits, bit_idx)) {
      util::BitUtil::Set(group_bits, bit_idx);
      return ConciseHashTableSlot(num_bits_to_group + bit_idx);
    }
  } while (bit_idx++ < max_bit_idx);

  num_overflow_++;

  return ConciseHashTableSlot(num_bits_to_group + bit_idx - 1);
}

inline u64 ConciseHashTable::NumFilledSlotsBefore(
    const ConciseHashTableSlot slot) const {
  TPL_ASSERT(is_built(), "Table must be built");

  const u64 slot_idx = slot.GetSlotIndex();
  const u64 group_idx = slot_idx >> kLogSlotsPerGroup;
  const u64 bit_idx = slot_idx & kGroupBitMask;

  const SlotGroup *slot_group = slot_groups_ + group_idx;
  const u64 bits_after_slot = slot_group->bits & (u64(-1) << bit_idx);
  return slot_group->count - util::BitUtil::CountBits(bits_after_slot);
}

}  // namespace tpl::sql
