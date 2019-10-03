#pragma once

#include <algorithm>
#include <memory>
#include <utility>

#include "common/common.h"
#include "common/memory.h"
#include "sql/hash_table_entry.h"
#include "util/bit_util.h"

namespace tpl::sql {

/**
 * A concise hash table that uses a bitmap to determine slot occupation.
 */
class ConciseHashTable {
 public:
  // The maximum probe length before falling back into the overflow table
  static constexpr const uint32_t kProbeThreshold = 1;

  // The default load factor
  static constexpr const uint32_t kLoadFactor = 8;

  // A minimum of 4K slots
  static constexpr const uint64_t kMinNumSlots = 1u << 12;

  // The number of CHT slots that belong to one group. This value should either
  // be 32 or 64 for (1) making computation simpler by bit-shifting and (2) to
  // ensure at most one cache-line read/write per insert/lookup.
  static constexpr const uint32_t kLogSlotsPerGroup = 6;
  static constexpr const uint32_t kSlotsPerGroup = 1u << kLogSlotsPerGroup;
  static constexpr const uint32_t kGroupBitMask = kSlotsPerGroup - 1;

  /**
   * Create a new uninitialized concise hash table. Callers **must** call
   * @em SetSize() before interacting with the table
   * @param probe_threshold The maximum probe threshold before falling back to
   *                        a secondary overflow entry table.
   */
  explicit ConciseHashTable(uint32_t probe_threshold = kProbeThreshold);

  /**
   * Destructor
   */
  ~ConciseHashTable();

  /**
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(ConciseHashTable)

  /**
   * Set the size of the hash table to support at least @em num_elems entries. The table will
   * optimize itself in expectation of seeing at most @em num_elems elements without resizing.
   * @param num_elems The expected number of elements
   */
  void SetSize(uint32_t num_elems);

  /**
   * Insert an element with the given hash into the table and return an encoded lot position.
   * Note: while this function takes both the entry and hash value, the hash value inside the entry
   * should match what's provided here.
   *
   * TODO(pmenon): Accept only the entry and use the hash value in the entry
   *
   * @param entry The entry to insert
   * @param hash The hash value of the entry to insert
   */
  void Insert(HashTableEntry *entry, hash_t hash);

  /**
   * Finalize and build this concise hash table. The table is frozen after finalization.
   */
  void Build();

  /**
   * Prefetch the slot group for an entry with the given hash value @em hash
   * @tparam ForRead Is the prefetch for a subsequent read operation
   * @param hash The hash value of the entry to prefetch
   */
  template <bool ForRead>
  void PrefetchSlotGroup(hash_t hash) const;

  /**
   * Return the number of occupied slots in the table **before** the given slot
   * @param slot The slot to compute the prefix count for
   * @return The number of occupied slot before the provided input slot
   */
  uint64_t NumFilledSlotsBefore(ConciseHashTableSlot slot) const;

  /**
   * Given the probe entry's hash value, return a boolean indicating whether it
   * may exist in the table, and if so, the index of the first slot the entry
   * may be.
   * @param hash The hash value of the entry to lookup
   * @return A pair indicating if the entry may exist and the slot to look at
   */
  std::pair<bool, uint64_t> Lookup(hash_t hash) const;

  /**
   * @return The number of bytes this hash table has allocated.
   */
  uint64_t GetTotalMemoryUsage() const { return sizeof(SlotGroup) * num_groups_; }

  /**
   * @return The capacity, i.e., the maximum number of elements this table supports.
   */
  uint64_t GetCapacity() const { return slot_mask_ + 1; }

  /**
   * @return The number of overflows entries in this table.
   */
  uint64_t GetOverflowEntryCount() const { return num_overflow_; }

  /**
   * @return True if the table has been built; false otherwise.
   */
  bool IsBuilt() const { return built_; }

 private:
  /**
   * A slot group represents a group of 64 slots. Each slot is represented as a single bit from the
   * @em bits field. @em count is a count of the number of set bits in all slot groups in the group
   * array up to and including this group. In other worse, @em count is a prefix count of the number
   * of filled slots up to this group.
   */
  struct SlotGroup {
    // The bitmap indicating whether the slots are occupied or free
    uint64_t bits;
    // The prefix population count
    uint32_t count;

    static_assert(sizeof(bits) * kBitsPerByte == kSlotsPerGroup,
                  "Number of slots in group and configured constant are out of sync");
  } PACKED;

 private:
  // The array of groups. This array is managed by this class.
  SlotGroup *slot_groups_;

  // The number of groups (of slots) in the table
  uint64_t num_groups_;

  // The mask used to find a slot in the hash table
  uint64_t slot_mask_;

  // The maximum number of slots to probe
  uint32_t probe_limit_;

  // The number of entries in the overflow table
  uint32_t num_overflow_;

  // Flag indicating if the hash table has been built and is frozen (read-only)
  bool built_;
};

// ---------------------------------------------------------
// Implementation below
// ---------------------------------------------------------

inline void ConciseHashTable::Insert(HashTableEntry *entry, const hash_t hash) {
  const uint64_t slot_idx = hash & slot_mask_;
  const uint64_t group_idx = slot_idx >> kLogSlotsPerGroup;
  const uint64_t num_bits_to_group = group_idx << kLogSlotsPerGroup;
  uint32_t *group_bits = reinterpret_cast<uint32_t *>(&slot_groups_[group_idx].bits);

  uint32_t bit_idx = static_cast<uint32_t>(slot_idx & kGroupBitMask);
  uint32_t max_bit_idx = std::min(63u, bit_idx + probe_limit_);
  do {
    if (!util::BitUtil::Test(group_bits, bit_idx)) {
      util::BitUtil::Set(group_bits, bit_idx);
      entry->cht_slot = ConciseHashTableSlot(num_bits_to_group + bit_idx);
      return;
    }
  } while (++bit_idx <= max_bit_idx);

  num_overflow_++;

  entry->cht_slot = ConciseHashTableSlot(num_bits_to_group + bit_idx - 1);
}

template <bool ForRead>
inline void ConciseHashTable::PrefetchSlotGroup(hash_t hash) const {
  const uint64_t slot_idx = hash & slot_mask_;
  const uint64_t group_idx = slot_idx >> kLogSlotsPerGroup;
  Memory::Prefetch<ForRead, Locality::Low>(slot_groups_ + group_idx);
}

inline uint64_t ConciseHashTable::NumFilledSlotsBefore(const ConciseHashTableSlot slot) const {
  TPL_ASSERT(IsBuilt(), "Table must be built");

  const uint64_t group_idx = slot >> kLogSlotsPerGroup;
  const uint64_t bit_idx = slot & kGroupBitMask;

  const SlotGroup *slot_group = slot_groups_ + group_idx;
  const uint64_t bits_after_slot = slot_group->bits & (uint64_t(-1) << bit_idx);
  return slot_group->count - util::BitUtil::CountPopulation(bits_after_slot);
}

inline std::pair<bool, uint64_t> ConciseHashTable::Lookup(const hash_t hash) const {
  const uint64_t slot_idx = hash & slot_mask_;
  const uint64_t group_idx = slot_idx >> kLogSlotsPerGroup;
  const uint64_t bit_idx = slot_idx & kGroupBitMask;

  const SlotGroup *slot_group = slot_groups_ + group_idx;
  const uint64_t bits_after_slot = slot_group->bits & (uint64_t(-1) << bit_idx);

  const bool exists = slot_group->bits & (1ull << bit_idx);
  const uint64_t pos = slot_group->count - util::BitUtil::CountPopulation(bits_after_slot);

  return std::pair(exists, pos);
}

}  // namespace tpl::sql
