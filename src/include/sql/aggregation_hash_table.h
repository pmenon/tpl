#pragma once

#include "sql/generic_hash_table.h"
#include "util/chunked_vector.h"

namespace tpl::sql {

class VectorProjectionIterator;

/**
 * The hash table used when performing aggregations
 */
class AggregationHashTable {
 public:
  static constexpr const u32 kDefaultInitialTableSize = 256;

  /**
   * Construct an aggregation hash table using the provided memory region, and
   * configured to store aggregates of size @em payload_size in bytes
   * @param region The memory region to allocate from
   * @param payload_size The size of the elements in the hash table
   */
  AggregationHashTable(util::Region *region, u32 payload_size) noexcept;

  /**
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(AggregationHashTable);

  /**
   * Insert a new element with hash value @em hash into the aggregation table.
   * @param hash The hash value of the element to insert
   * @return A pointer to a memory area where the element can be written to
   */
  byte *Insert(hash_t hash) noexcept;

  /**
   * The callback function to check the equality of an input tuple and an
   * existing entry in the aggregation hash table. This is the function used to
   * resolve hash collisions.
   * Convention: The first argument is the probe tuple, the second is an
   *             existing entry in the hash table to check.
   */
  using KeyEqFn = bool (*)(const void *, const void *);

  /**
   * Lookup and return an entry in the aggregation table that matches a given
   * hash and key. The hash value is provided here, keys are checked using the
   * provided callback function.
   * @param hash The hash value to use for early filtering
   * @param key_eq_fn The key-equality function to resolve hash collisions
   * @param arg An opaque argument
   * @return A pointer to the matching entry payload; null if no entry is found.
   */
  byte *Lookup(hash_t hash, KeyEqFn key_eq_fn, const void *arg) noexcept;

  /**
   * Function that takes an array of vector projections pointing to a row and
   * computes its hash value.
   */
  using HashFn = hash_t (*)(VectorProjectionIterator *[]);

  /**
   * Function that takes an uninitialized aggregate and an array of
   * vector projections and initializes the aggregate value.
   */
  using InitAggFn = void (*)(byte *, VectorProjectionIterator *[]);

  /**
   * Function that updates an existing aggregate using values from the input
   * vector projections.
   */
  using MergeAggFn = void (*)(byte *, VectorProjectionIterator *[]);

  /**
   * Process an entire vector if input.
   * @param iters The input vectors
   * @param hash_fn Function to compute a hash
   * @param init_agg_fn Function to initialize/create a new aggregate
   * @param merge_agg_fn Function to merge/update an existing aggregate
   */
  void ProcessBatch(VectorProjectionIterator *iters[], HashFn hash_fn,
                    InitAggFn init_agg_fn, MergeAggFn merge_agg_fn);

 private:
  // Does the hash table need to grow?
  bool NeedsToGrow() const { return hash_table_.num_elements() == max_fill_; }

  // Grow the hash table
  void Grow();

  // Compute the hash value and perform the table lookup for all elements in the
  // input vector projections.
  template <bool Prefetch>
  void ComputeHashAndLookup(VectorProjectionIterator *iters[], hash_t hashes[],
                            HashTableEntry *entries[], HashFn hash_fn);

 private:
  // Where the aggregates are stored
  util::ChunkedVector entries_;

  // The hash table where the aggregates are stored
  GenericHashTable hash_table_;

  // The maximum number of elements in the table before a resize
  u64 max_fill_;
};

}  // namespace tpl::sql
