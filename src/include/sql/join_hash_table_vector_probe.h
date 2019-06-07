#pragma once

#include "sql/hash_table_entry.h"
#include "sql/vector_projection_iterator.h"
#include "util/common.h"

namespace tpl::sql {

class JoinHashTable;

/**
 * Helper class to perform vectorized lookups into a JoinHashTable
 */
class JoinHashTableVectorProbe {
 public:
  /**
   * Function to hash the tuple the iterator is currently pointing at.
   */
  using HashFn = hash_t (*)(VectorProjectionIterator *);

  /**
   * Function to check if the tuple in the hash table (i.e., the first argument)
   * is equivalent to the tuple the iterator is currently pointing at.
   */
  using KeyEqFn = bool (*)(const void *, VectorProjectionIterator *);

  /**
   * Constructor given a hashing function and a key equality function
   */
  explicit JoinHashTableVectorProbe(const JoinHashTable &table);

  /**
   * Setup a vectorized lookup using the given input batch @em vpi
   * @param vpi The input vector
   * @param hash_fn The hashing function
   */
  void Prepare(VectorProjectionIterator *vpi, HashFn hash_fn);

  /**
   * Return the next match, moving the input iterator if need be
   * @param vpi The input vector projection
   * @param key_eq_fn The function to check key equality
   * @return The next matching entry
   */
  template <typename T = byte>
  const T *GetNextOutput(VectorProjectionIterator *vpi, KeyEqFn key_eq_fn);

 private:
  // The table we're probing
  const JoinHashTable &table_;
  // The current index in the entries output we're iterating over
  u16 match_idx_;
  // The vector of computed hashes
  hash_t hashes_[kDefaultVectorSize];
  // The vector of entries
  const HashTableEntry *entries_[kDefaultVectorSize];
};

// ---------------------------------------------------------
// Implementation below
// ---------------------------------------------------------

// Because this function is a tuple-at-a-time, it's placed in the header to
// reduce function call overhead.
template <typename T>
inline const T *JoinHashTableVectorProbe::GetNextOutput(
    VectorProjectionIterator *const vpi, const KeyEqFn key_eq_fn) {
  TPL_ASSERT(vpi != nullptr, "No input VPI!");
  TPL_ASSERT(match_idx_ < vpi->num_selected(), "Continuing past iteration!");

  while (true) {
    // Continue along current chain until we find a match
    while (const auto *entry = entries_[match_idx_]) {
      entries_[match_idx_] = entry->next;
      if (entry->hash == hashes_[match_idx_] &&
          key_eq_fn(entry->payload, vpi)) {
        return entry->PayloadAs<T>();
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
