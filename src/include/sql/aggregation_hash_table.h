#pragma once

#include "sql/generic_hash_table.h"
#include "sql/memory_pool.h"
#include "util/chunked_vector.h"

namespace tpl::sql {

class VectorProjectionIterator;

/**
 * The hash table used when performing aggregations
 */
class AggregationHashTable {
 public:
  static constexpr const float kDefaultLoadFactor = 0.7;
  static constexpr const u32 kDefaultInitialTableSize = 256;
  static constexpr const u32 kDefaultNumPartitions = 512;

  // -------------------------------------------------------
  // Callback functions to customize aggregations
  // -------------------------------------------------------

  /**
   * Function to check the key equality of an input tuple and an existing entry
   * in the aggregation hash table.
   * Convention: First argument is the aggregate entry, second argument is the
   *             input/probe tuple.
   */
  using KeyEqFn = bool (*)(const void *, const void *);

  /**
   * Function that takes an input element and computes a hash value.
   */
  using HashFn = hash_t (*)(void *);

  /**
   * Function to initialize a new aggregate.
   * Convention: First argument is the aggregate to initialize, second argument
   *             is the input/probe tuple to initialize the aggregate with.
   */
  using InitAggFn = void (*)(void *, void *);

  /**
   * Function to advance an existing aggregate with a new input value.
   * Convention: First argument is the existing aggregate to update, second
   *             argument is the input/probe tuple to update the aggregate with.
   */
  using AdvanceAggFn = void (*)(void *, void *);

  /**
   * Small class to capture various usage stats
   */
  struct Stats {
    u64 num_growths = 0;
    u64 num_flushes = 0;
  };

  // -------------------------------------------------------
  // Main API
  // -------------------------------------------------------

  /**
   * Construct an aggregation hash table using the provided memory pool, and
   * configured to store aggregates of size @em payload_size in bytes
   * @param memory The memory pool to allocate memory from
   * @param payload_size The size of the elements in the hash table
   */
  AggregationHashTable(MemoryPool *memory, u32 payload_size);

  /**
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(AggregationHashTable);

  /**
   * Destructor
   */
  ~AggregationHashTable();

  /**
   * Insert a new element with hash value @em hash into the aggregation table.
   * @param hash The hash value of the element to insert
   * @return A pointer to a memory area where the element can be written to
   */
  byte *Insert(hash_t hash);

  /**
   * Insert a new element with hash value @em hash into this partitioned
   * aggregation hash table.
   * @param hash The hash value of the element to insert
   * @return A pointer to a memory area where the input element can be written
   */
  byte *InsertPartitioned(hash_t hash);

  /**
   * Lookup and return an entry in the aggregation table that matches a given
   * hash and key. The hash value is provided here, keys are checked using the
   * provided callback function.
   * @param hash The hash value to use for early filtering
   * @param key_eq_fn The key-equality function to resolve hash collisions
   * @param probe_tuple The probe tuple
   * @return A pointer to the matching entry payload; null if no entry is found.
   */
  byte *Lookup(hash_t hash, KeyEqFn key_eq_fn, const void *probe_tuple);

  /**
   * Process an entire vector if input.
   * @param iters The input vectors
   * @param hash_fn Function to compute a hash of an input element
   * @param key_eq_fn Function to determine key equality of an input element and
   *                  an existing aggregate
   * @param init_agg_fn Function to initialize a new aggregate
   * @param advance_agg_fn Function to advance an existing aggregate
   */
  void ProcessBatch(VectorProjectionIterator *iters[], HashFn hash_fn,
                    KeyEqFn key_eq_fn, InitAggFn init_agg_fn,
                    AdvanceAggFn advance_agg_fn);

  /**
   * Read-only access to hash table stats
   */
  const Stats *stats() const { return &stats_; }

 private:
  // Does the hash table need to grow?
  bool NeedsToGrow() const { return hash_table_.num_elements() >= max_fill_; }

  // Grow the hash table
  void Grow();

  // Lookup a hash table entry internally
  HashTableEntry *LookupEntryInternal(hash_t hash, KeyEqFn key_eq_fn,
                                      const void *probe_tuple) const;

  // Flush all entries currently stored in the hash table into the overflow
  // partitions
  void FlushToOverflowPartitions();

  // Compute the hash value and perform the table lookup for all elements in the
  // input vector projections.
  template <bool VPIIsFiltered>
  void ProcessBatchImpl(VectorProjectionIterator *iters[], u32 num_elems,
                        hash_t hashes[], HashTableEntry *entries[],
                        HashFn hash_fn, KeyEqFn key_eq_fn,
                        InitAggFn init_agg_fn, AdvanceAggFn advance_agg_fn);

  // Called from ProcessBatch() to lookup a batch of entries. When the function
  // returns, the hashes vector will contain the hash values of all elements in
  // the input vector, and entries will contain a pointer to the associated
  // element's group aggregate, or null if no group exists.
  template <bool VPIIsFiltered>
  void LookupBatch(VectorProjectionIterator *iters[], u32 num_elems,
                   hash_t hashes[], HashTableEntry *entries[], HashFn hash_fn,
                   KeyEqFn key_eq_fn) const;

  // Called from LookupBatch() to compute and fill the hashes input vector with
  // the hash values of all input tuples, and to load the initial set of
  // candidate groups into the entries vector. When this function returns, the
  // hashes vector will be full, and the entries vector will contain either a
  // pointer to the first (of potentially many) elements in the hash table that
  // match the input hash value.
  template <bool VPIIsFiltered>
  void ComputeHashAndLoadInitial(VectorProjectionIterator *iters[],
                                 u32 num_elems, hash_t hashes[],
                                 HashTableEntry *entries[],
                                 HashFn hash_fn) const;
  template <bool VPIIsFiltered, bool Prefetch>
  void ComputeHashAndLoadInitialImpl(VectorProjectionIterator *iters[],
                                     u32 num_elems, hash_t hashes[],
                                     HashTableEntry *entries[],
                                     HashFn hash_fn) const;

  // Called from LookupBatch() to follow the entry chain of candidate group
  // entries filtered through group_sel. Follows the chain and uses the key
  // equality function to resolve hash collisions.
  template <bool VPIIsFiltered>
  void FollowNextLoop(VectorProjectionIterator *iters[], u32 num_elems,
                      u32 group_sel[], const hash_t hashes[],
                      HashTableEntry *entries[], KeyEqFn key_eq_fn) const;

  // Called from ProcessBatch() to create missing groups
  template <bool VPIIsFiltered>
  void CreateMissingGroups(VectorProjectionIterator *iters[], u32 num_elems,
                           const hash_t hashes[], HashTableEntry *entries[],
                           KeyEqFn key_eq_fn, InitAggFn init_agg_fn);

  // Called from ProcessBatch() to update only the valid entries in the input
  // vector
  template <bool VPIIsFiltered>
  void AdvanceGroups(VectorProjectionIterator *iters[], u32 num_elems,
                     HashTableEntry *entries[], AdvanceAggFn advance_agg_fn);

 private:
  // Allocator
  MemoryPool *memory_;

  // Where the aggregates are stored
  util::ChunkedVector<MemoryPoolAllocator<byte>> entries_;

  // The hash index
  GenericHashTable hash_table_;

  // Overflow partitions
  HashTableEntry **partition_heads_;
  HashTableEntry **partition_tails_;
  AggregationHashTable **partition_tables_;
  u64 flush_threshold_;
  u64 part_shift_bits_;

  // Runtime stats
  Stats stats_;

  // The maximum number of elements in the table before a resize
  u64 max_fill_;
};

// ---------------------------------------------------------
// Implementation below
// ---------------------------------------------------------

inline HashTableEntry *AggregationHashTable::LookupEntryInternal(
    hash_t hash, AggregationHashTable::KeyEqFn key_eq_fn,
    const void *probe_tuple) const {
  HashTableEntry *entry = hash_table_.FindChainHead(hash);
  while (entry != nullptr) {
    if (entry->hash == hash && key_eq_fn(entry->payload, probe_tuple)) {
      return entry;
    }
    entry = entry->next;
  }
  return nullptr;
}

inline byte *AggregationHashTable::Lookup(
    hash_t hash, AggregationHashTable::KeyEqFn key_eq_fn,
    const void *probe_tuple) {
  auto *entry = LookupEntryInternal(hash, key_eq_fn, probe_tuple);
  return (entry == nullptr ? nullptr : entry->payload);
}

}  // namespace tpl::sql
