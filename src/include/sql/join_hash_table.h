#pragma once

#include <memory>
#include <numeric>
#include <vector>

#include "sql/bloom_filter.h"
#include "sql/chaining_hash_table.h"
#include "sql/concise_hash_table.h"
#include "sql/memory_pool.h"
#include "util/chunked_vector.h"
#include "util/spin_latch.h"

namespace libcount {
class HLL;
}  // namespace libcount

namespace tpl::sql {

class ThreadStateContainer;

/**
 * The main class used to for hash joins. JoinHashTables are bulk-loaded through calls to
 * JoinHashTable::AllocInputTuple() and lazily built through JoinHashTable::Build(). After a
 * JoinHashTable has been built once, it is frozen and immutable. Thus, they're write-once read-many
 * (WORM) structures.
 *
 * @code
 * JoinHashTable jht = ...
 * for (tuple in table) {
 *   auto tuple = reinterpret_cast<YourTuple *>(jht.AllocInputTuple());
 *   tuple->col_a = ...
 *   ...
 * }
 * // All insertions complete, lazily build the table
 * jht.Build();
 * @endcode
 *
 * In parallel mode, thread-local join hash tables are lazily built and merged in parallel into a
 * global join hash table through a call to JoinHashTable::MergeParallel(). After this call, the
 * global table takes ownership of all thread-local allocated memory and hash index.
 */
class JoinHashTable {
 public:
  // Default precision to use for HLL estimations
  static constexpr uint32_t kDefaultHLLPrecision = 10;

  // Minimum number of expected elements to merge before triggering a parallel merge
  static constexpr uint32_t kDefaultMinSizeForParallelMerge = 1024;

  /**
   * Construct a join hash table. All memory allocations are sourced from the injected @em memory,
   * and thus, are ephemeral.
   * @param memory The memory pool to allocate memory from.
   * @param tuple_size The size of the tuple stored in this join hash table.
   * @param use_concise_ht Whether to use a concise or generic join index.
   */
  explicit JoinHashTable(MemoryPool *memory, uint32_t tuple_size, bool use_concise_ht = false);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(JoinHashTable);

  /**
   * Destructor.
   */
  ~JoinHashTable();

  /**
   * Allocate storage in the hash table for an input tuple whose hash value is @em hash. This
   * function only performs an allocation from the table's memory pool. No insertion into the table
   * is performed, meaning a subsequent JoinHashTable::Lookup() for the entry will not return the
   * inserted entry.
   * @param hash The hash value of the tuple to insert.
   * @return A memory region where the caller can materialize the tuple.
   */
  byte *AllocInputTuple(hash_t hash);

  /**
   * Fully construct the join hash table. Nothing is done if the join hash table has already been
   * built. After building, the table becomes read-only.
   */
  void Build();

  /**
   * Lookup a single entry with hash value @em hash returning an iterator.
   * @tparam UseCHT Should the lookup use the concise or general table.
   * @param hash The hash value of the element to lookup.
   * @return An iterator over all elements that match the hash.
   */
  template <bool UseCHT>
  HashTableEntryIterator Lookup(hash_t hash) const;

  /**
   * Perform a batch lookup of elements whose hash values are stored in @em hashes, storing the
   * results in @em results.
   * @param num_tuples The number of tuples in the batch.
   * @param hashes The hash values of the probe elements.
   * @param results The heads of the bucket chain of the probed elements.
   */
  void LookupBatch(uint32_t num_tuples, const hash_t hashes[],
                   const HashTableEntry *results[]) const;

  /**
   * Merge all thread-local hash tables stored in the state contained into this table. Perform the
   * merge in parallel.
   * @param thread_state_container The container for all thread-local tables.
   * @param jht_offset The offset in the state where the hash table is.
   */
  void MergeParallel(const ThreadStateContainer *thread_state_container, std::size_t jht_offset);

  /**
   * @return The total number of bytes used to materialize tuples. This excludes space required for
   *         the join index.
   */
  uint64_t GetBufferedTupleMemoryUsage() const { return entries_.size() * entries_.element_size(); }

  /**
   * @return The total number of bytes used by the join index only. The join index (also referred to
   *         as the hash table directory), excludes storage for materialized tuple contents.
   */
  uint64_t GetJoinIndexMemoryUsage() const {
    return UsingConciseHashTable() ? concise_hash_table_.GetTotalMemoryUsage()
                                   : generic_hash_table_.GetTotalMemoryUsage();
  }

  /**
   * @return The total number of bytes used by this table. This includes both the raw tuple storage
   *         and the hash table directory storage.
   */
  uint64_t GetTotalMemoryUsage() const {
    return GetBufferedTupleMemoryUsage() + GetJoinIndexMemoryUsage();
  }

  /**
   * @return True if this table uses an early filtering bloom filter; false otherwise.
   */
  bool HasBloomFilter() const { return !bloom_filter_.IsEmpty(); }

  /**
   * @return The total number of elements in the table, including duplicates.
   */
  uint64_t GetTupleCount() const {
    // We don't know if this table was built in parallel. To be sure, we acquire the latch before
    // checking the owned entries vector. This isn't a performance-critical function, so acquiring
    // the latch shouldn't be a problem.

    util::SpinLatch::ScopedSpinLatch latch(&owned_latch_);
    if (!owned_.empty()) {
      return std::accumulate(owned_.begin(), owned_.end(), uint64_t{0},
                             [](auto c, const auto &entries) { return c + entries.size(); });
    }

    // Not built in parallel, check the entry vector.
    return entries_.size();
  }

  /**
   * @return True if the join hash table has been built; false otherwise.
   */
  bool IsBuilt() const { return built_; }

  /**
   * @return True if this join hash table uses a concise table under the hood.
   */
  bool UsingConciseHashTable() const { return use_concise_ht_; }

  /**
   * @return The underlying bloom filter.
   */
  const BloomFilter *GetBloomFilter() const { return &bloom_filter_; }

 private:
  FRIEND_TEST(JoinHashTableTest, LazyInsertionTest);
  FRIEND_TEST(JoinHashTableTest, PerfTest);

  // Access a stored entry by index
  HashTableEntry *EntryAt(const uint64_t idx) {
    return reinterpret_cast<HashTableEntry *>(entries_[idx]);
  }

  const HashTableEntry *EntryAt(const uint64_t idx) const {
    return reinterpret_cast<const HashTableEntry *>(entries_[idx]);
  }

  // Dispatched from Build() to build either a generic or concise hash table
  void BuildGenericHashTable();
  void BuildConciseHashTable();

  // Dispatched from BuildConciseHashTable() to construct the concise hash table
  // and to reorder buffered build tuples in place according to the CHT
  template <bool PrefetchCHT, bool PrefetchEntries>
  void BuildConciseHashTableInternal();
  template <bool PrefetchCHT, bool PrefetchEntries>
  void ReorderMainEntries();
  template <bool Prefetch, bool PrefetchEntries>
  void ReorderOverflowEntries();
  void VerifyMainEntryOrder();
  void VerifyOverflowEntryOrder();

  // Dispatched from LookupBatch() to lookup from either a generic or concise
  // hash table in batched manner
  void LookupBatchInGenericHashTable(uint32_t num_tuples, const hash_t hashes[],
                                     const HashTableEntry *results[]) const;
  void LookupBatchInConciseHashTable(uint32_t num_tuples, const hash_t hashes[],
                                     const HashTableEntry *results[]) const;

  // Dispatched from LookupBatchInConciseHashTable()
  template <bool Prefetch>
  void LookupBatchInConciseHashTableInternal(uint32_t num_tuples, const hash_t hashes[],
                                             const HashTableEntry *results[]) const;

  // Merge the source hash table (which isn't built yet) into this one
  template <bool Concurrent>
  void MergeIncomplete(JoinHashTable *source);

 private:
  // The vector where we store the build-side input
  util::ChunkedVector<MemoryPoolAllocator<byte>> entries_;

  // To protect concurrent access to 'owned_entries_'
  mutable util::SpinLatch owned_latch_;

  // List of entries this hash table has taken ownership of. Protected by 'owned_latch_'.
  MemPoolVector<decltype(entries_)> owned_;

  // The generic hash table
  UntaggedChainingHashTable generic_hash_table_;

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
inline HashTableEntryIterator JoinHashTable::Lookup<false>(const hash_t hash) const {
  HashTableEntry *entry = generic_hash_table_.FindChainHead(hash);
  while (entry != nullptr && entry->hash != hash) {
    entry = entry->next;
  }
  return HashTableEntryIterator(entry, hash);
}

template <>
inline HashTableEntryIterator JoinHashTable::Lookup<true>(const hash_t hash) const {
  const auto [found, idx] = concise_hash_table_.Lookup(hash);
  auto *entry = (found ? EntryAt(idx) : nullptr);
  return HashTableEntryIterator(entry, hash);
}

}  // namespace tpl::sql
