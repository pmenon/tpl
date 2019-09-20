#pragma once

#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include "sql/generic_hash_table.h"
#include "sql/memory_pool.h"
#include "sql/schema.h"
#include "util/chunked_vector.h"
#include "util/fixed_length_buffer.h"

namespace libcount {
class HLL;
}  // namespace libcount

namespace tpl::sql {

class ThreadStateContainer;
class VectorProjection;
class VectorProjectionIterator;

// Forward declare
class AHTIterator;
class AHTVectorIterator;
class AHTOverflowPartitionIterator;

/**
 * The hash table used when performing aggregations.
 */
class AggregationHashTable {
 public:
  // The default load factor we allow the hash table to reach before resizing
  static constexpr float kDefaultLoadFactor = 0.7;

  // The default initial size we set the hash table on construction
  static constexpr uint32_t kDefaultInitialTableSize = 256;

  // The default number of partitions we use in partitioned aggregation mode
  static constexpr uint32_t kDefaultNumPartitions = 512;

  // The default precision we use to configure the HyperLogLog instances. Set to
  // optimize accuracy and space manually.
  static constexpr uint32_t kDefaultHLLPrecision = 10;

  // -------------------------------------------------------
  // Callback functions to customize aggregations
  // -------------------------------------------------------

  /**
   * Function to check the key equality of an input tuple and an existing entry in the hash table.
   * Convention: First argument is the aggregate entry, second argument is the input tuple.
   */
  using KeyEqFn = bool (*)(const void *, const void *);

  /**
   * Function that takes an input element and computes a hash value.
   */
  using HashFn = hash_t (*)(void *);

  /**
   * Function to initialize a new aggregate.
   * Convention: First argument is the aggregate to initialize, second argument is the input tuple
   *             to initialize the aggregate with.
   */
  using InitAggFn = void (*)(void *, void *);

  /**
   * Function to advance an existing aggregate with a new input value.
   * Convention: First argument is the existing aggregate to update, second argument is the input
   *             tuple to update the aggregate with.
   */
  using AdvanceAggFn = void (*)(void *, void *);

  /**
   * Function to merge a set of overflow partitions into the given aggregation hash table.
   * Convention: First argument is an opaque state object that the user provides. The second
   *             argument is the aggregation to be built. The third argument is the list of overflow
   *             partitions pointers, and the fourth and fifth argument are the range of overflow
   *             partitions to merge into the input aggregation hash table.
   */
  using MergePartitionFn = void (*)(void *, AggregationHashTable *, AHTOverflowPartitionIterator *);

  /**
   * Function to scan an aggregation hash table.
   * Convention: First argument is query state, second argument is thread-local state, last argument
   *             is the aggregation hash table to scan.
   */
  using ScanPartitionFn = void (*)(void *, void *, const AggregationHashTable *);

  /**
   * Small class to capture various usage stats
   */
  struct Stats {
    uint64_t num_growths = 0;
    uint64_t num_flushes = 0;
  };

  // -------------------------------------------------------
  // Main API
  // -------------------------------------------------------

  /**
   * Construct an aggregation hash table using the provided memory pool, and configured to store
   * aggregates of size @em payload_size in bytes.
   * @param memory The memory pool to allocate memory from.
   * @param payload_size The size of the elements in the hash table.
   */
  AggregationHashTable(MemoryPool *memory, std::size_t payload_size);

  /**
   * Construct an aggregation hash table using the provided memory pool, configured to store
   * aggregates of size @em payload_size in bytes, and whose initial size allows for
   * @em initial_size aggregates.
   * @param memory The memory pool to allocate memory from.
   * @param payload_size The size of the elements in the hash table.
   * @param initial_size The initial number of aggregates to support.
   */
  AggregationHashTable(MemoryPool *memory, std::size_t payload_size, uint32_t initial_size);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(AggregationHashTable);

  /**
   * Destructor.
   */
  ~AggregationHashTable();

  /**
   * Insert a new element with hash value @em hash into the aggregation table.
   * @param hash The hash value of the element to insert.
   * @return A pointer to a memory area where the element can be written to.
   */
  byte *Insert(hash_t hash);

  /**
   * Insert a new element with hash value @em hash into this partitioned aggregation hash table.
   * @param hash The hash value of the element to insert.
   * @return A pointer to a memory area where the input element can be written.
   */
  byte *InsertPartitioned(hash_t hash);

  /**
   * Lookup and return an entry in the aggregation table that matches a given hash and key. The hash
   * value is provided here, keys are checked using the provided callback function.
   * @param hash The hash value to use for early filtering.
   * @param key_eq_fn The key-equality function to resolve hash collisions.
   * @param probe_tuple The probe tuple.
   * @return A pointer to the matching entry payload; null if no entry is found.
   */
  byte *Lookup(hash_t hash, KeyEqFn key_eq_fn, const void *probe_tuple);

  /**
   * Injest and process a batch of input into the aggregation.
   * @param vpi The vector projection to process.
   * @param hash_fn Function to compute a hash of an input element.
   * @param key_eq_fn Function to check key equality of an input element and an existing aggregate.
   * @param init_agg_fn Function to initialize a new aggregate.
   * @param advance_agg_fn Function to advance an existing aggregate.
   * @param partitioned Whether to perform insertions in partitioned mode.
   */
  void ProcessBatch(VectorProjectionIterator *vpi, HashFn hash_fn, KeyEqFn key_eq_fn,
                    InitAggFn init_agg_fn, AdvanceAggFn advance_agg_fn, bool partitioned);

  /**
   * Transfer all entries and overflow partitions stored in each thread-local aggregation hash table
   * (in the thread state container) into this table.
   *
   * This function only moves memory around, no aggregation hash tables are built. It is used at the
   * end of the build-portion of a parallel aggregation before the thread state container is reset
   * for the next pipeline's thread-local state.
   *
   * @param thread_states Container for all thread-local tables.
   * @param agg_ht_offset The offset in the container to find the table.
   */
  void TransferMemoryAndPartitions(ThreadStateContainer *thread_states, std::size_t agg_ht_offset,
                                   MergePartitionFn merge_partition_fn);

  /**
   * Execute a parallel scan over this partitioned hash table. It is assumed that this aggregation
   * table was constructed in a partitioned manner. This function will build a new aggregation hash
   * table for any non-empty overflow partition, merge the contents of the partition (using the
   * merging function provided to the call to @em TransferMemoryAndPartitions()), and invoke the
   * scan callback function to scan the newly built table. The building and scanning of the table
   * will be performed entirely in parallel; hence, the callback function should be thread-safe.
   *
   * The thread states container is assumed to already have been configured prior to this scan call.
   *
   * The callback scan function accepts two opaque state objects: an query state and a thread state.
   * The query state is provided as a function argument. The thread state will be pulled from the
   * provided ThreadStateContainer object.
   *
   * @param query_state The (opaque) query state.
   * @param thread_states The container holding all thread states.
   * @param scan_fn The callback scan function that will scan one partition of the partitioned
   *                aggregation hash table.
   */
  void ExecuteParallelPartitionedScan(void *query_state, ThreadStateContainer *thread_states,
                                      ScanPartitionFn scan_fn);

  /**
   * How many aggregates are in this table?
   */
  uint64_t NumElements() const { return hash_table_.num_elements(); }

  /**
   * Read-only access to hash table stats.
   */
  const Stats *stats() const { return &stats_; }

 private:
  friend class AHTIterator;
  friend class AHTVectorIterator;

  // Does the hash table need to grow?
  bool NeedsToGrow() const { return hash_table_.num_elements() >= max_fill_; }

  // Grow the hash table
  void Grow();

  // Lookup a hash table entry internally
  HashTableEntry *LookupEntryInternal(hash_t hash, KeyEqFn key_eq_fn,
                                      const void *probe_tuple) const;

  // Flush all entries currently stored in the hash table into the overflow partitions
  void FlushToOverflowPartitions();

  // Allocate all overflow partition information if unallocated
  void AllocateOverflowPartitions();

  // Compute the hash value and perform the table lookup for all elements in the input vector
  // projections.
  template <bool VPIIsFiltered>
  void ProcessBatchImpl(VectorProjectionIterator *vpi, HashFn hash_fn, KeyEqFn key_eq_fn,
                        InitAggFn init_agg_fn, AdvanceAggFn advance_agg_fn, bool partitioned);

  // Dispatched from ProcessBatchImpl() to compute the hash values for all input tuples. Hashes are
  // stored in the 'hashes' array in the batch state.
  template <bool VPIIsFiltered>
  void ComputeHash(VectorProjectionIterator *vpi, HashFn hash_fn);

  // Called from ProcessBatchImpl() to find matching groups for all tuples in the input vector
  // projection. This function returns the number of groups that were found.
  template <bool VPIIsFiltered>
  uint32_t FindGroups(VectorProjectionIterator *vpi, KeyEqFn key_eq_fn);

  // Called from FindGroups() to lookup initial entries from the hash table for all input tuples.
  uint32_t LookupInitial(uint32_t num_elems);

  // Specialization of LookupInitial() to control prefetching.
  template <bool Prefetch>
  uint32_t LookupInitialImpl(uint32_t num_elems);

  // Called from FindGroups() to check the equality of keys.
  template <bool VPIIsFiltered>
  uint32_t CheckKeyEquality(VectorProjectionIterator *vpi, uint32_t num_elems, KeyEqFn key_eq_fn);

  // Called from FindGroups() to follow the entry chain of candidate groups.
  uint32_t FollowNext();

  // Called from ProcessBatch() to create missing groups
  template <bool VPIIsFiltered, bool Partitioned>
  void CreateMissingGroups(VectorProjectionIterator *vpi, KeyEqFn key_eq_fn, InitAggFn init_agg_fn);

  // Called from ProcessBatch() to update only the valid entries in the input vector
  template <bool VPIIsFiltered>
  void AdvanceGroups(VectorProjectionIterator *vpi, uint32_t num_groups,
                     AdvanceAggFn advance_agg_fn);

  // Called during partitioned scan to build an aggregation hash table over a single partition.
  AggregationHashTable *BuildTableOverPartition(void *query_state, uint32_t partition_idx);

 private:
  // Memory allocator.
  MemoryPool *memory_;

  // The size of the aggregates in bytes.
  std::size_t payload_size_;

  // Where the aggregates are stored.
  util::ChunkedVector<MemoryPoolAllocator<byte>> entries_;

  // Entries taken from other tables.
  MemPoolVector<decltype(entries_)> owned_entries_;

  // The hash index.
  GenericHashTable hash_table_;

  // A struct we use to track various metadata during batch processing
  struct BatchProcessState {
    // Unique hash estimator
    std::unique_ptr<libcount::HLL> hll_estimator;
    // The array of computed hash values
    hash_t hashes[kDefaultVectorSize];
    // Buffer containing entry pointers after lookup and key equality checks
    HashTableEntry *entries[kDefaultVectorSize];
    // Buffer containing indexes of tuples that found a matching group
    sel_t groups_found[kDefaultVectorSize];
    // Buffer containing indexes of tuples that did not find a matching group
    util::FixedLengthBuffer<sel_t, kDefaultVectorSize> groups_not_found;
    // Buffer containing indexes of tuples that didn't match keys
    util::FixedLengthBuffer<sel_t, kDefaultVectorSize> key_not_eq;

    // Constructor
    explicit BatchProcessState(std::unique_ptr<libcount::HLL> estimator);

    // Destructor
    ~BatchProcessState();
  };

  // State required when processing batches of input. Allocated on first use.
  MemPoolPtr<BatchProcessState> batch_state_;

  // -------------------------------------------------------
  // Overflow partitions
  // -------------------------------------------------------

  // The function to merge a set of overflow partitions into one table.
  MergePartitionFn merge_partition_fn_;
  // The head and tail arrays over the overflow partition. These arrays and each element in them are
  // allocated from the pool, so they don't need to be explicitly deleted.
  HashTableEntry **partition_heads_;
  HashTableEntry **partition_tails_;
  // The HyperLogLog++ estimated for each overflow partition. The array is allocated from the pool,
  // but each element is allocated from libcount. Thus, we need to delete manually before freeing
  // the array.
  libcount::HLL **partition_estimates_;
  // The aggregation hash table over each partition. The array and each element is allocated from
  // the pool, so they don't need to be explicitly deleted.
  AggregationHashTable **partition_tables_;
  // The number of elements that can be inserted into the main hash table before we flush into the
  // overflow partitions. We size this so that the entries are roughly L2-sized.
  uint64_t flush_threshold_;
  // The number of bits to shift the hash value to determine its overflow partition.
  uint64_t partition_shift_bits_;

  // Runtime stats.
  Stats stats_;

  // The maximum number of elements in the table before a resize.
  uint64_t max_fill_;
};

// ---------------------------------------------------------
// Implementation below
// ---------------------------------------------------------

inline HashTableEntry *AggregationHashTable::LookupEntryInternal(
    hash_t hash, AggregationHashTable::KeyEqFn key_eq_fn, const void *probe_tuple) const {
  HashTableEntry *entry = hash_table_.FindChainHead(hash);
  while (entry != nullptr) {
    if (entry->hash == hash && key_eq_fn(entry->payload, probe_tuple)) {
      return entry;
    }
    entry = entry->next;
  }
  return nullptr;
}

inline byte *AggregationHashTable::Lookup(hash_t hash, AggregationHashTable::KeyEqFn key_eq_fn,
                                          const void *probe_tuple) {
  auto *entry = LookupEntryInternal(hash, key_eq_fn, probe_tuple);
  return (entry == nullptr ? nullptr : entry->payload);
}

// ---------------------------------------------------------
// Aggregation Hash Table Iterator
// ---------------------------------------------------------

/**
 * A tuple-at-a-time iterator over the contents of an aggregation hash table.
 */
class AHTIterator {
 public:
  /**
   * Construct an iterator over the given aggregation hash table.
   * @param agg_table The table to iterate.
   */
  explicit AHTIterator(const AggregationHashTable &agg_table) : iter_(agg_table.hash_table_) {}

  /**
   * Does this iterator have more data?
   * @return True if the iterator has more data; false otherwise
   */
  bool HasNext() const { return iter_.HasNext(); }

  /**
   * Advance the iterator one tuple.
   */
  void Next() { iter_.Next(); }

  /**
   * Return a pointer to the current row. It's assumed the called has checked the iterator is valid.
   */
  const byte *GetCurrentAggregateRow() const {
    auto *ht_entry = iter_.GetCurrentEntry();
    return ht_entry->payload;
  }

 private:
  // The iterator over the aggregation hash table
  GenericHashTableIterator<false> iter_;
};

// ---------------------------------------------------------
// Aggregation Hash Table Vector Iterator
// ---------------------------------------------------------

/**
 * A vectorized iterator over the contents of an aggregation hash table. This exists so that users
 * can generate vector projections from aggregation hash tables, which might be useful when feeding
 * into other operations or performing filters post aggregation.
 *
 * To facilitate this, users must provide a function that converts row-oriented aggregate data into
 * column-oriented vector projections, i.e., a transpose function.
 */
class AHTVectorIterator {
 public:
  using TransposeFn = void (*)(const HashTableEntry *[], uint64_t, VectorProjectionIterator *);

  /**
   * Construct a vector iterator over the given aggregation table.
   */
  AHTVectorIterator(const AggregationHashTable &agg_hash_table,
                    const std::vector<const Schema::ColumnInfo *> &column_info,
                    TransposeFn transpose_fn);

  /**
   * Construct a vector iterator over the given aggregation table.
   */
  AHTVectorIterator(const AggregationHashTable &agg_hash_table,
                    const Schema::ColumnInfo *column_info, uint32_t num_cols,
                    TransposeFn transpose_fn);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(AHTVectorIterator);

  /**
   * Does this iterator have more data?
   */
  bool HasNext() const { return iter_.HasNext(); }

  /**
   * Advance the iterator by, at most, one vector's worth of data.
   */
  void Next(TransposeFn transpose_fn);

  /**
   * Return the next vector output.
   */
  VectorProjectionIterator *GetVectorProjectionIterator() {
    return vector_projection_iterator_.get();
  }

 private:
  void BuildVectorProjection(TransposeFn transpose_fn);

 private:
  // Memory allocator
  MemoryPool *memory_;

  // The vectorized iterate over the hash table
  GenericHashTableVectorIterator<false> iter_;

  // The vector projection containing aggregate data stored column-wise
  std::unique_ptr<VectorProjection> vector_projection_;

  // An iterator over the produced vector projection
  std::unique_ptr<VectorProjectionIterator> vector_projection_iterator_;
};

// ---------------------------------------------------------
// Aggregation Hash Table Overflow Partitions Iterator
// ---------------------------------------------------------

/**
 * An iterator over a range of overflow partition entries in an aggregation hash table. The range is
 * provided through the constructor. Each overflow entry's hash value is accessible through
 * @em GetHash(), along with the opaque payload through @em GetPayload().
 */
class AHTOverflowPartitionIterator {
 public:
  /**
   * Construct an iterator over the given partition range.
   * @param partitions_begin The beginning of the range.
   * @param partitions_end The end of the range.
   */
  AHTOverflowPartitionIterator(HashTableEntry **partitions_begin, HashTableEntry **partitions_end)
      : partitions_iter_(partitions_begin), partitions_end_(partitions_end), curr_(nullptr) {
    Next();
  }

  /**
   * Are there more overflow entries?
   * @return True if the iterator has more data; false otherwise
   */
  bool HasNext() const { return curr_ != nullptr; }

  /**
   * Move to the next overflow entry.
   */
  void Next() {
    // Try to move along current partition
    if (curr_ != nullptr) {
      curr_ = curr_->next;
      if (curr_ != nullptr) {
        return;
      }
    }

    // Find next non-empty partition
    while (curr_ == nullptr && partitions_iter_ != partitions_end_) {
      curr_ = *partitions_iter_++;
    }
  }

  /**
   * Get the hash value of the overflow entry the iterator is currently pointing to. It is assumed
   * the caller has checked there is data in the iterator.
   * @return The hash value of the current overflow entry.
   */
  hash_t GetHash() const { return curr_->hash; }

  /**
   * Get the payload of the overflow entry the iterator is currently pointing to. It is assumed the
   * caller has checked there is data in the iterator.
   * @return The opaque payload associated with the current overflow entry.
   */
  const byte *GetPayload() const { return curr_->payload; }

  /**
   * Get the payload of the overflow entry the iterator is currently pointing to, but interpret it
   * as the given template type @em T. It is assumed the caller has checked there is data in the
   * iterator.
   * @tparam T The type of the payload in the current overflow entry.
   * @return The opaque payload associated with the current overflow entry.
   */
  template <typename T>
  const T *GetPayloadAs() const {
    return curr_->PayloadAs<T>();
  }

 private:
  // The current position in the partitions array
  HashTableEntry **partitions_iter_;

  // The ending position in the partitions array
  HashTableEntry **partitions_end_;

  // The current overflow entry
  HashTableEntry *curr_;
};

}  // namespace tpl::sql
