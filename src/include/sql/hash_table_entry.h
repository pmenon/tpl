#pragma once

#include <type_traits>

#include "common/common.h"
#include "sql/sql.h"

namespace tpl::sql {

using ConciseHashTableSlot = uint64_t;

/**
 * A generic structure used to represent an entry in either a chaining hash table or a concise hash
 * table. An entry is a variably-sized chunk of memory where the keys, attributes, aggregates are
 * stored in the @em payload field. This structure is used for both joins and aggregations.
 */
struct HashTableEntry {
  union {
    // Used to chain together entries falling to the same bucket within a
    // chaining hash table.
    HashTableEntry *next;
    // Used to record the slot this entry occupies in a concise hash table.
    ConciseHashTableSlot cht_slot;
    // Used during overflow entry reordering when building a concise hash table.
    uint64_t overflow_count;
  };

  // The hash table.
  hash_t hash;
  // The payload starting position.
  byte payload[0];

  /**
   * @return The total size in bytes of a hash table entry element that stores a payload of the
   *         given size, also in bytes.
   */
  static constexpr std::size_t ComputeEntrySize(const std::size_t payload_size) {
    return sizeof(HashTableEntry) + payload_size;
  }

  /**
   * @return The byte offset of the payload within a hash table entry element.
   */
  static constexpr std::size_t ComputePayloadOffset() { return sizeof(HashTableEntry); }

  /**
   * @return A typed pointer to the payload content of this hash table entry.
   */
  template <typename T>
  T *PayloadAs() noexcept {
    return reinterpret_cast<T *>(payload);
  }

  /**
   * @return A const-view typed pointer to the payload content of this hash table entry.
   */
  template <typename T>
  const T *PayloadAs() const noexcept {
    return reinterpret_cast<const T *>(payload);
  }
};

}  // namespace tpl::sql
