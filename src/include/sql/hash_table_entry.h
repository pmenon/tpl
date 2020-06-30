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

  hash_t hash;
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

/**
 * An iterator over a chain of hash table entries. Use as follows:
 *
 * @code
 * for (auto iter = jht.Lookup(hash); iter.HasNext(key_eq_fn);) {
 *   auto entry = iter.NextMatch();
 *   // Use entry
 * }
 * @endcode
 *
 * @em NextMatch() must be called per iteration.
 */
struct HashTableEntryIterator {
 public:
  /**
   * Construct an iterator beginning at the entry @em initial of the chain of entries matching the
   * hash value @em hash. This iterator is returned from @em JoinHashTable::Lookup().
   * @param initial The first matching entry in the chain of entries
   * @param hash The hash value of the probe tuple
   */
  HashTableEntryIterator(const HashTableEntry *initial, hash_t hash)
      : next_(initial), hash_(hash) {}

  /**
   * Function used to check equality of hash keys.
   * First argument is an opaque context object provided by the caller.
   * Second argument is the probe/input tuple we're matching on through keys.
   * Last argument is a candidate entry in the chain we'll check keys with.
   */
  using KeyEq = bool (*)(void *, void *, void *);

  /**
   * Advance to the next match and return true if it is found.
   * @tparam T The templated type of the payload within the entry.
   * @tparam F A functor with the interface: bool(const T *). The argument is a pointer to the tuple
   *           in the hash table entry. The return value is a boolean indicating if the entry
   *           matches what the user is probing for.
   * @param key_eq The function used to determine key equality.
   * @return True if there is at least one more match.
   */
  template <typename T, typename F>
  bool HasNext(F &&key_eq) {
    static_assert(std::is_invocable_r_v<bool, F, const T *>,
                  "Key-equality must be invocable as: bool(const T *)");

    while (next_ != nullptr) {
      if (next_->hash == hash_ && key_eq(reinterpret_cast<const T *>(next_->payload))) {
        return true;
      }
      next_ = next_->next;
    }

    return false;
  }

  /**
   * A more cumbersome API to iterate used in code-gen because lambda's don't exist at that level.
   * @param key_eq Function pointer to check the equality of a tuple to one provided.
   * @param ctx An opaque user-provided object. Passed directly into key-equality callback. Not used
   *            in the function.
   * @param probe_tuple The probe tuple we want to find a match for. This is provided to the
   *                    key-equality callback to find the next match.
   * @return True if there is at least one more match.
   */
  bool HasNext(KeyEq key_eq, void *ctx, void *probe_tuple) {
    return HasNext<void *>([&](const void *table_tuple) -> bool {
      return key_eq(ctx, probe_tuple, const_cast<void *>(table_tuple));
    });
  }

  /**
   * Return the next match.
   */
  const HashTableEntry *NextMatch() {
    const HashTableEntry *result = next_;
    next_ = next_->next;
    return result;
  }

 private:
  // The next element the iterator produces
  const HashTableEntry *next_;

  // The hash value we're looking up. Used as a cheap pre-filter in key-equality checks.
  hash_t hash_;
};

}  // namespace tpl::sql
