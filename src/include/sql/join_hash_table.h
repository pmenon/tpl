#pragma once

#include <memory>
#include <vector>

#include "sql/bloom_filter.h"
#include "sql/concise_hash_table.h"
#include "sql/generic_hash_table.h"
#include "util/chunked_vector.h"
#include "util/region.h"
#include "util/spin_latch.h"

namespace libcount {
class HLL;
}  // namespace libcount

namespace tpl::sql::test {
class JoinHashTableTest;
}  // namespace tpl::sql::test

namespace tpl::sql {

class ThreadStateContainer;

/**
 * The main join hash table. Join hash tables are bulk-loaded through calls to
 * @em AllocInputTuple() and frozen after calling @em Build(). Thus, they're
 * write-once read-many (WORM) structures.
 */
class JoinHashTable {
 public:
  static constexpr u32 kDefaultHLLPrecision = 10;

  /**
   * Construct a join hash table. All memory allocations are sourced from the
   * injected @em region, and thus, are ephemeral.
   * @param region The
   * @param tuple_size
   * @param use_concise_ht
   */
  JoinHashTable(util::Region *region, u32 tuple_size,
                bool use_concise_ht = false) noexcept;

  /**
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(JoinHashTable);

  /**
   * Destructor
   */
  ~JoinHashTable();

  /**
   * Allocate storage in the hash table for an input tuple whose hash value is
   * @em hash. This function only performs an allocation from the table's memory
   * pool. No insertion into the table is performed, meaning a subsequent
   * @em Lookup() for the entry will not return the inserted entry.
   * @param hash The hash value of the tuple to insert
   * @return A memory region where the caller can materialize the tuple
   */
  byte *AllocInputTuple(hash_t hash);

  /**
   * Fully construct the join hash table. Nothing is done if the join hash table
   * has already been built. After building, the table becomes read-only.
   */
  void Build();

  /**
   * The tuple-at-a-time iterator interface
   */
  class Iterator;

  /**
   * Lookup a single entry with hash value @em hash returning an iterator
   * @tparam UseCHT Should the lookup use the concise or general table
   * @param hash The hash value of the element to lookup
   * @return An iterator over all elements that match the hash
   */
  template <bool UseCHT>
  Iterator Lookup(hash_t hash) const;

  /**
   * Perform a batch lookup of elements whose hash values are stored in @em
   * hashes, storing the results in @em results
   * @param num_tuples The number of tuples in the batch
   * @param hashes The hash values of the probe elements
   * @param results The heads of the bucket chain of the probed elements
   */
  void LookupBatch(u32 num_tuples, const hash_t hashes[],
                   const HashTableEntry *results[]) const;

  /**
   * Merge all thread-local hash tables stored in the state contained into this
   * table. Perform the merge in parallel.
   * @param thread_state_container The container for all thread-local tables
   * @param hash_table_offset The offset in the state where the hash table is
   */
  void MergeAllParallel(ThreadStateContainer *thread_state_container,
                        u32 hash_table_offset);

  // -------------------------------------------------------
  // Accessors
  // -------------------------------------------------------

  /**
   * Return the amount of memory the buffered tuples occupy
   */
  u64 GetBufferedTupleMemoryUsage() const noexcept {
    return entries_.size() * entries_.element_size();
  }

  /**
   * Get the amount of memory used by the join index only (i.e., excluding space
   * used to store materialized build-side tuples)
   */
  u64 GetJoinIndexMemoryUsage() const noexcept {
    return use_concise_hash_table() ? concise_hash_table_.GetTotalMemoryUsage()
                                    : generic_hash_table_.GetTotalMemoryUsage();
  }

  /**
   * Return the total size of the join hash table in bytes
   */
  u64 GetTotalMemoryUsage() const noexcept {
    return GetBufferedTupleMemoryUsage() + GetJoinIndexMemoryUsage();
  }

  /**
   * Return the total number of inserted elements, including duplicates
   */
  u64 num_elements() const noexcept { return entries_.size(); }

  /**
   * Has the hash table been built?
   */
  bool is_built() const noexcept { return built_; }

  /**
   * Is this join using a concise hash table?
   */
  bool use_concise_hash_table() const noexcept { return use_concise_ht_; }

 public:
  // -------------------------------------------------------
  // Tuple-at-a-time Iterator
  // -------------------------------------------------------

  /**
   * The iterator used for generic lookups. This class is used mostly for
   * tuple-at-a-time lookups from the hash table.
   */
  class Iterator {
   public:
    /**
     * Construct an iterator beginning at the entry @em initial of the chain
     * of entries matching the hash value @em hash. This iterator is returned
     * from @em JoinHashTable::Lookup().
     * @param initial The first matching entry in the chain of entries
     * @param hash The hash value of the probe tuple
     */
    Iterator(const HashTableEntry *initial, hash_t hash);

    /**
     * Function used to check equality of hash keys
     */
    using KeyEq = bool(void *opaque_ctx, void *probe_tuple, void *table_tuple);

    /**
     * Return the next match (of both hash and keys)
     * @param key_eq The function used to determine key equality
     * @param opaque_ctx An opaque context passed into the key equality function
     * @param probe_tuple The probe tuple
     * @return The next matching entry; null otherwise
     */
    const HashTableEntry *NextMatch(KeyEq key_eq, void *opaque_ctx,
                                    void *probe_tuple);

   private:
    // The next element the iterator produces
    const HashTableEntry *next_;
    // The hash value we're looking up
    hash_t hash_;
  };

 private:
  friend class tpl::sql::test::JoinHashTableTest;

  // Access a stored entry by index
  HashTableEntry *EntryAt(const u64 idx) noexcept {
    return reinterpret_cast<HashTableEntry *>(entries_[idx]);
  }

  const HashTableEntry *EntryAt(const u64 idx) const noexcept {
    return reinterpret_cast<const HashTableEntry *>(entries_[idx]);
  }

  // Dispatched from Build() to build either a generic or concise hash table
  void BuildGenericHashTable() noexcept;
  void BuildConciseHashTable();

  // Dispatched from BuildGenericHashTable()
  template <bool Prefetch>
  void BuildGenericHashTableInternal() noexcept;

  // Dispatched from BuildConciseHashTable() to construct the concise hash table
  // and to reorder buffered build tuples in place according to the CHT
  template <bool PrefetchCHT, bool PrefetchEntries>
  void BuildConciseHashTableInternal();
  template <bool Prefetch>
  void InsertIntoConciseHashTable() noexcept;
  template <bool PrefetchCHT, bool PrefetchEntries>
  void ReorderMainEntries() noexcept;
  template <bool Prefetch, bool PrefetchEntries>
  void ReorderOverflowEntries() noexcept;
  void VerifyMainEntryOrder();
  void VerifyOverflowEntryOrder() noexcept;

  // Dispatched from LookupBatch() to lookup from either a generic or concise
  // hash table in batched manner
  void LookupBatchInGenericHashTable(u32 num_tuples, const hash_t hashes[],
                                     const HashTableEntry *results[]) const;
  void LookupBatchInConciseHashTable(u32 num_tuples, const hash_t hashes[],
                                     const HashTableEntry *results[]) const;

  // Dispatched from LookupBatchInGenericHashTable()
  template <bool Prefetch>
  void LookupBatchInGenericHashTableInternal(
      u32 num_tuples, const hash_t hashes[],
      const HashTableEntry *results[]) const;

  // Dispatched from LookupBatchInConciseHashTable()
  template <bool Prefetch>
  void LookupBatchInConciseHashTableInternal(
      u32 num_tuples, const hash_t hashes[],
      const HashTableEntry *results[]) const;

  // Merge the source hash table (which isn't built yet) into this one
  template <bool Prefetch>
  void MergeIncompleteMT(JoinHashTable *source);

 private:
  // The vector where we store the build-side input
  util::ChunkedVector<util::StlRegionAllocator<byte>> entries_;

  // To protect concurrent access to owned_entries
  util::SpinLatch owned_latch_;
  // List of entries this hash table has taken ownership of
  std::vector<util::ChunkedVector<util::StlRegionAllocator<byte>>> owned_;

  // The generic hash table
  GenericHashTable generic_hash_table_;

  // The concise hash table
  ConciseHashTable concise_hash_table_;

  // The bloom filter
  BloomFilter bloom_filter_;

  // Estimator of unique elements
  std::unique_ptr<libcount::HLL> hll_estimator_;

  // Has the hash table been built?
  bool built_;

  // Should we use a concise hash table?
  bool use_concise_ht_;
};

// ---------------------------------------------------------
// JoinHashTable implementation
// ---------------------------------------------------------

template <>
inline JoinHashTable::Iterator JoinHashTable::Lookup<false>(
    const hash_t hash) const {
  HashTableEntry *entry = generic_hash_table_.FindChainHead(hash);
  while (entry != nullptr && entry->hash != hash) {
    entry = entry->next;
  }
  return JoinHashTable::Iterator(entry, hash);
}

template <>
inline JoinHashTable::Iterator JoinHashTable::Lookup<true>(
    const hash_t hash) const {
  const auto [found, idx] = concise_hash_table_.Lookup(hash);
  auto *entry = (found ? EntryAt(idx) : nullptr);
  return JoinHashTable::Iterator(entry, hash);
}

// ---------------------------------------------------------
// JoinHashTable's Iterator implementation
// ---------------------------------------------------------

inline JoinHashTable::Iterator::Iterator(const HashTableEntry *initial,
                                         hash_t hash)
    : next_(initial), hash_(hash) {}

inline const HashTableEntry *JoinHashTable::Iterator::NextMatch(
    JoinHashTable::Iterator::KeyEq key_eq, void *opaque_ctx,
    void *probe_tuple) {
  const HashTableEntry *result = next_;
  while (result != nullptr) {
    next_ = next_->next;
    if (result->hash == hash_ &&
        key_eq(opaque_ctx, probe_tuple,
               reinterpret_cast<void *>(const_cast<byte *>(result->payload)))) {
      break;
    }
    result = next_;
  }
  return result;
}

}  // namespace tpl::sql
