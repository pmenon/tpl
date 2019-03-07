#pragma once

#include "sql/hash_table_entry.h"
#include "sql/join_hash_table.h"
#include "sql/vector_projection_iterator.h"
#include "util/common.h"

namespace tpl::sql {

class JoinHashTable;

/// Helper class to perform vectorized lookups into a JoinHashTable
class JoinHashTableVectorLookup {
 public:
  using HashFn = hash_t (*)(VectorProjectionIterator *) noexcept;
  using KeyEqFn = bool (*)(const byte *, VectorProjectionIterator *) noexcept;

  /// Constructor given a hashing function and a key equality function
  explicit JoinHashTableVectorLookup(const JoinHashTable &table) noexcept;

  /// Setup a vectorized lookup using the given input batch \a vpi
  void Prepare(VectorProjectionIterator *vpi, HashFn hash_fn) noexcept;

  /// Return the next match, moving the input iterator if need be
  const HashTableEntry *GetNextOutput(VectorProjectionIterator *vpi,
                                      KeyEqFn key_eq_fn) noexcept;

 private:
  const JoinHashTable &table_;
  u16 match_idx_;
  hash_t hashes_[kDefaultVectorSize];
  const HashTableEntry *entries_[kDefaultVectorSize];
};

// ---------------------------------------------------------
// Implementation below
// ---------------------------------------------------------

inline JoinHashTableVectorLookup::JoinHashTableVectorLookup(
    const JoinHashTable &table) noexcept
    : table_(table), match_idx_(0), hashes_{0}, entries_{nullptr} {}

inline void JoinHashTableVectorLookup::Prepare(
    VectorProjectionIterator *vpi,
    const JoinHashTableVectorLookup::HashFn hash_fn) noexcept {
  TPL_ASSERT(vpi->num_selected() <= kDefaultVectorSize,
             "VectorProjection size must be less than kDefaultVectorSize");
  // Set up
  match_idx_ = 0;

  // Compute the hashes
  {
    u32 idx = 0;
    vpi->ForEach(
        [this, vpi, hash_fn, &idx]() { hashes_[idx++] = hash_fn(vpi); });
  }

  // Perform the initial lookup
  table_.LookupBatch(vpi->num_selected(), hashes_, entries_);
}

inline const HashTableEntry *JoinHashTableVectorLookup::GetNextOutput(
    VectorProjectionIterator *vpi,
    const JoinHashTableVectorLookup::KeyEqFn key_eq_fn) noexcept {
  TPL_ASSERT(vpi != nullptr, "No input VPI!");
  TPL_ASSERT(match_idx_ < vpi->num_selected(), "Continuing past iteration!");

  while (true) {
    // Continue along current chain until we find a match
    while (const auto *entry = entries_[match_idx_]) {
      entries_[match_idx_] = entry->next;
      if (entry->hash == hashes_[match_idx_] &&
          key_eq_fn(entry->payload, vpi)) {
        return entry;
      }
    }

    // No match found, move to the next probe tuple index
    if (++match_idx_ >= vpi->num_selected()) {
      break;
    }

    // Advance probe input
    if (vpi->IsFiltered()) {
      vpi->AdvanceFiltered();
    } else {
      vpi->Advance();
    }
  }

  return nullptr;
}

}  // namespace tpl::sql