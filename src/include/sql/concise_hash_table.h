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
  static constexpr const u32 kSlotsPerGroup = 64;

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

  /// Find the first match for the given hash value
  ConciseHashTableSlot FindFirst(hash_t hash) const;

  /// Return the capacity (the maximum number of elements) this table supports
  u64 Capacity() const { return slot_mask() + 1; }

  // -------------------------------------------------------
  // Utility Operations
  // -------------------------------------------------------

  /// Return the number of bytes this hash table has allocated
  u64 GetTotalMemoryUsage() const { return sizeof(SlotGroup) * num_groups(); }

  /// Pretty print the contents of the table
  std::string PrettyPrint() const;

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

  // -------------------------------------------------------
  // Accessors
  // -------------------------------------------------------

  u64 slot_mask() const { return slot_mask_; }

  u64 num_groups() const { return num_groups_; }

  u32 probe_threshold() const { return probe_threshold_; }

  bool is_built() const { return built_; }
  void set_is_built() { built_ = true; }

 private:
  // The array of groups. This array is managed by this class.
  SlotGroup *slot_groups_;

  // The number of groups (of slots) in the table
  u64 num_groups_;

  // The mask used to find a slot in the hash table
  u64 slot_mask_;

  // The maximum number of slots to probe
  u32 probe_threshold_;

  // Flag indicating if the hash table has been built and is frozen (read-only)
  bool built_;
};

// ---------------------------------------------------------
// Implementation below
// ---------------------------------------------------------

inline ConciseHashTableSlot ConciseHashTable::Insert(const hash_t hash) {
  // kSlotsPerGroup is either 32 or 64, so all divide and modulus operations
  // will optimize into simple bit shifts

  u32 slot_idx = static_cast<u32>(hash & slot_mask());
  u32 group_idx = slot_idx / kSlotsPerGroup;
  u32 *group_bits = reinterpret_cast<u32 *>(&slot_groups_[group_idx].bits);

  for (u32 bit_pos = slot_idx % kSlotsPerGroup,
           max_bit_pos = std::min(63u, bit_pos + probe_threshold());
       bit_pos < max_bit_pos; bit_pos++) {
    if (!util::BitUtil::Test(group_bits, bit_pos)) {
      util::BitUtil::Set(group_bits, bit_pos);
      return ConciseHashTableSlot::Make(group_idx * kSlotsPerGroup + bit_pos);
    }
  }

  return ConciseHashTableSlot::MakeOverflow();
}

inline ConciseHashTableSlot ConciseHashTable::FindFirst(
    const hash_t hash) const {
  // kSlotsPerGroup is either 32 or 64, so all divide and modulus operations
  // will optimize into simple bit shifts

  u32 slot_idx = static_cast<u32>(hash & slot_mask());
  u32 group_idx = slot_idx / kSlotsPerGroup;
  u32 *group_bits = reinterpret_cast<u32 *>(&slot_groups_[group_idx].bits);

  for (u32 bit_pos = slot_idx % kSlotsPerGroup,
           max_bit_pos = std::max(63u, bit_pos + probe_threshold());
       bit_pos < max_bit_pos; bit_pos++) {
    if (util::BitUtil::Test(group_bits, bit_pos)) {
      return ConciseHashTableSlot::Make(group_idx * kSlotsPerGroup + bit_pos);
    }
  }

  return ConciseHashTableSlot::MakeOverflow();
}

}  // namespace tpl::sql
