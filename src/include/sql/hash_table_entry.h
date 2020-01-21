#pragma once

#include <type_traits>

#include "common/common.h"
#include "sql/sql.h"

namespace tpl::sql {

using ConciseHashTableSlot = uint64_t;

/**
 * A generic structure used to represent an entry in either a generic hash table or a concise hash
 * table. An entry is a variably-sized chunk of memory where the keys, attributes, aggregates are
 * stored in the @em payload field. This structure is used for both joins and aggregations.
 */
struct HashTableEntry {
  union {
    // Next is used to chain together entries falling to the same bucket
    HashTableEntry *next;

    // This slot is used to record the slot this entry occupies in the CHT
    ConciseHashTableSlot cht_slot;

    // Used during reordering over overflow entries when constructing a CHT
    uint64_t overflow_count;
  };

  hash_t hash;
  byte payload[0];

  /**
   * Compute the size of a HashTableEntry element with a payload of the given size.
   * @param payload_size The size of the payload in bytes.
   * @return The total size of a HashTableEntry in bytes.
   */
  static constexpr std::size_t ComputeEntrySize(const std::size_t payload_size) {
    return sizeof(HashTableEntry) + payload_size;
  }

  /**
   * For testing!
   */
  template <typename T>
  const T *PayloadAs() const noexcept {
    return reinterpret_cast<const T *>(payload);
  }
};

/**
 * An iterator over a chain of hash table entries that match a provided initial hash value. This
 * iterator cannot resolve hash collisions, it is the responsibility of the user to do so.
 * Use as follows:
 *
 * @code
 * for (auto iter = jht.Lookup(hash); iter.HasNext(key_eq_fn);) {
 *   auto payload = (Payload*)iter.GetMatchPayload();
 *   // Check key
 *   if (payload->key == my_key) {
 *     // Match
 *   }
 * }
 * @endcode
 *
 * @em NextMatch() must be called per iteration.
 */
struct HashTableEntryIterator {
 public:
  /**
   * Construct an iterator over hash table entry's beginning at the provided initial entry that
   * match the provided hash value. It is the responsibility of the user to resolve hash collisions.
   * This iterator is returned from JoinHashTable::Lookup().
   * @param initial The first matching entry in the chain of entries
   * @param hash The hash value of the probe tuple
   */
  HashTableEntryIterator(const HashTableEntry *initial, hash_t hash)
      : next_(initial), hash_(hash) {}

  /**
   * Advance to the next match and return true if it is found.
   * @return True if there is at least one more potential match.
   */
  bool HasNext() {
    while (next_ != nullptr) {
      if (next_->hash == hash_) {
        return true;
      }
      next_ = next_->next;
    }
    return false;
  }

  /**
   * @return The next match.
   */
  const HashTableEntry *GetMatch() {
    const HashTableEntry *result = next_;
    next_ = next_->next;
    return result;
  }

  /**
   * @return The payload of the next matched entry.
   */
  const byte *GetMatchPayload() { return GetMatch()->PayloadAs<byte>(); }

 private:
  // The next element the iterator produces.
  const HashTableEntry *next_;

  // The hash value we're looking up. Used as a cheap pre-filter in
  // key-equality checks.
  hash_t hash_;
};

}  // namespace tpl::sql
