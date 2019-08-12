#pragma once

#include <type_traits>

#include "util/common.h"

namespace tpl::sql {

using ConciseHashTableSlot = u64;

/**
 * A generic structure used to represent an entry in either a generic hash
 * table or a concise hash table. An entry is a variably-sized chunk of
 * memory where the keys, attributes, aggregates are stored in the \a payload
 * field. This structure is used for both joins and aggregations.
 */
struct HashTableEntry {
  union {
    // Next is used to chain together entries falling to the same bucket
    HashTableEntry *next;

    // This slot is used to record the slot this entry occupies in the CHT
    ConciseHashTableSlot cht_slot;

    // Used during reordering over overflow entries when constructing a CHT
    u64 overflow_count;
  };

  hash_t hash;
  byte payload[0];

  /**
   * For testing!
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
   * Construct an iterator beginning at the entry @em initial of the chain
   * of entries matching the hash value @em hash. This iterator is returned
   * from @em JoinHashTable::Lookup().
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
   * @tparam F A functor with the interface: bool(const T *). The argument is
   *           is a pointer to the tuple in the hash table entry. The return
   *           value is a boolean indicating if the entry matches what the user
   *           is probing for.
   * @param key_eq The function used to determine key equality.
   * @return True if there is at least one more match.
   */
  template <typename T, typename F>
  bool HasNext(F &&key_eq) {
    static_assert(std::is_invocable_r_v<bool, F, const T *>,
                  "Key-equality must be invocable as: bool(const T *)");

    while (next_ != nullptr) {
      if (next_->hash == hash_ &&
          key_eq(reinterpret_cast<const T *>(next_->payload))) {
        return true;
      }
      next_ = next_->next;
    }

    return false;
  }

  /**
   * A more cumbersome API to iterate used in code-gen because lambda's don't
   * exist at that level.
   * @param key_eq
   * @param ctx
   * @param probe_tuple
   * @return
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
  // The hash value we're looking up
  hash_t hash_;
};

}  // namespace tpl::sql
