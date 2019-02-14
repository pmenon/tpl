#pragma once

#include <memory>

#include "sql/hash_table_entry.h"
#include "util/common.h"
#include "util/bit_util.h"

namespace tpl::sql {

class ConciseHashTable {
 public:
  static constexpr const u32 kProbeThreshold = 2;

  /// Create a new uninitialized concise hash table. Callers **must** call
  /// Init() before interacting with the table
  explicit ConciseHashTable(u32 probe_threshold = kProbeThreshold);

  ~ConciseHashTable();

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
  // captures this concept.
  struct SlotGroup {
    // The bitmap indicating whether the slots are occupied or free
    u64 bits;
    // The prefix population count
    u32 count;
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

}  // namespace tpl::sql
