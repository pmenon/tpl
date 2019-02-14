#pragma once

#include "sql/bloom_filter.h"
#include "sql/concise_hash_table.h"
#include "sql/generic_hash_table.h"
#include "util/region.h"

namespace tpl::sql::test {
class JoinHashTableTest;
}  // namespace tpl::sql::test

namespace tpl::sql {

class VectorProjectionIterator;

class JoinHashTable {
 public:
  /// Construct a hash-table used for join processing using \a region as the
  /// main memory allocator
  JoinHashTable(util::Region *region, u32 tuple_size,
                bool use_concise_ht = false);

  /// This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(JoinHashTable);

  /// Allocate storage in the hash table for an input tuple whose hash value is
  /// \a hash and whose size (in bytes) is \a tuple_size. Remember that this
  /// only performs an allocation from the table's memory pool. No insertion
  /// into the table is performed.
  byte *AllocInputTuple(hash_t hash);

  /// Fully construct the join hash table. If the join hash table has already
  /// been built, do nothing.
  void Build();

  /// Lookup all entries in this hash table with the given hash value, returning
  /// an iterator.
  class Iterator;
  Iterator Lookup(hash_t hash) const;

  class VectorLookup;
  void LookupBatch(VectorLookup *lookup) const;

  /// Return the total number of inserted elements, including duplicates
  u32 num_elems() const { return num_elems_; }

  /// Has the join hash table been built?
  bool is_table_built() const { return built_; }

  /// Is this join using a concise hash table?
  bool use_concise_hash_table() const { return use_concise_ht_; }

 public:
  // -------------------------------------------------------
  // Tuple-at-a-time Iterator
  // -------------------------------------------------------

  /// The iterator used for generic lookups. This class is used mostly for
  /// tuple-at-a-time lookups from the hash table.
  class Iterator {
   public:
    Iterator(const JoinHashTable &table, hash_t hash);

    using KeyEq = bool(void *opaque_ctx, void *probe_tuple, void *table_tuple);
    HashTableEntry *NextMatch(KeyEq key_eq, void *opaque_ctx,
                              void *probe_tuple);

   private:
    HashTableEntry *next() const { return next_; }

    hash_t hash() const { return hash_; }

   private:
    // The next element the iterator produces
    HashTableEntry *next_;
    // The hash value we're looking up
    hash_t hash_;
  };

  // -------------------------------------------------------
  // VectorLookup
  // -------------------------------------------------------

  /// Helper class to perform vectorized lookups
  class VectorLookup {
   public:
    VectorLookup(const JoinHashTable &table, VectorProjectionIterator *vpi);

    u32 NumTuples();

   private:
    friend class JoinHashTable;

    const JoinHashTable &table() const { return table_; }

    VectorProjectionIterator *vpi() const { return vpi_; }

    hash_t *hashes() { return hashes_; }

    HashTableEntry **entries() { return entries_; }

   private:
    const JoinHashTable &table_;
    VectorProjectionIterator *vpi_;
    hash_t hashes_[kDefaultVectorSize];
    HashTableEntry *entries_[kDefaultVectorSize];
  };

 private:
  friend class tpl::sql::test::JoinHashTableTest;

  // Dispatched from Build() to build either a generic or concise hash table
  void BuildGenericHashTable();
  void BuildConciseHashTable();

  // Dispatched from BuildConciseHashTable() to reorder elements based on
  // ordering from the concise hash table
  void ReorderEntries();

  // -------------------------------------------------------
  // Accessors
  // -------------------------------------------------------

  util::Region *region() const { return region_; }

  u32 tuple_size() const { return tuple_size_; }

  GenericHashTable *generic_hash_table() { return &generic_table_; }
  const GenericHashTable *generic_hash_table() const { return &generic_table_; }

  ConciseHashTable *concise_hash_table() { return &concise_table_; }

  BloomFilter *bloom_filter() { return &filter_; }

  HashTableEntry *head() { return &head_; }

 private:
  // The memory allocator we use for all entries stored in the hash table
  util::Region *region_;

  // The size of the build-side tuples that are inserted into the table
  u32 tuple_size_;

  // The generic hash table
  GenericHashTable generic_table_;

  // The concise hash table
  ConciseHashTable concise_table_;

  // The bloom filter
  BloomFilter filter_;

  // The head of the lazy insertion list
  HashTableEntry head_;

  // The number of elements inserted
  u32 num_elems_;

  // Has the hash table been built?
  bool built_;

  // Should we use a concise hash table?
  bool use_concise_ht_;
};

// ---------------------------------------------------------
// JoinHashTable implementation
// ---------------------------------------------------------

inline byte *JoinHashTable::AllocInputTuple(hash_t hash) {
  auto *entry = static_cast<HashTableEntry *>(
      region()->Allocate(sizeof(HashTableEntry) + tuple_size()));
  entry->hash = hash;
  entry->next = head()->next;
  head()->next = entry;

  num_elems_++;

  return entry->payload;
}

inline JoinHashTable::Iterator JoinHashTable::Lookup(hash_t hash) const {
  return JoinHashTable::Iterator(*this, hash);
}

// ---------------------------------------------------------
// JoinHashTable's Iterator implementation
// ---------------------------------------------------------

inline JoinHashTable::Iterator::Iterator(const JoinHashTable &table,
                                         hash_t hash)
    : next_(nullptr), hash_(hash) {
  const auto *generic_table = table.generic_hash_table();
  auto *entry = generic_table->FindChainHead(hash);
  while (entry != nullptr && entry->hash != hash) {
    entry = entry->next;
  }
  next_ = entry;
}

inline HashTableEntry *JoinHashTable::Iterator::NextMatch(
    JoinHashTable::Iterator::KeyEq key_eq, void *opaque_ctx,
    void *probe_tuple) {
  HashTableEntry *result = next();
  while (result != nullptr) {
    next_ = next()->next;
    if (result->hash == hash() &&
        key_eq(opaque_ctx, probe_tuple,
               reinterpret_cast<void *>(result->payload))) {
      break;
    }
    result = next_;
  }
  return result;
}

}  // namespace tpl::sql
