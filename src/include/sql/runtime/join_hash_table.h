#pragma once

#include "sql/runtime/bloom_filter.h"
#include "sql/runtime/generic_hash_table.h"
#include "util/region.h"

namespace tpl::sql::test {
class JoinHashTableTest;
}  // namespace tpl::sql::test

namespace tpl::sql::runtime {

class JoinHashTable {
 public:
  // An entry in the join hash table. An Entry is a variably-sized chunk of
  // memory where the join keys and join attributes are stored. One of these
  // is allocated for every tuple from the build side of a join, through each
  // call to AllocInputTuple(). The first two fields of all entries are the
  // links to the next entry in the chain and the hash value of the entry. These
  // values are populated by this class. The payload (i.e., the keys and values)
  // are populated by the client and are opaque to this class.
  struct Entry : public GenericHashTable::EntryHeader {
    byte payload[0];
  };

  /// Construct a hash-table used for join processing using \p region as the
  /// main memory allocator
  explicit JoinHashTable(util::Region *region);

  /// This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(JoinHashTable);

  /// Allocate storage in the hash table for an input tuple whose hash value is
  /// \p hash and whose size (in bytes) is \p tuple_size. Remember that this
  /// only performs an allocation from the table's memory pool. No insertion
  /// into the table is performed.
  byte *AllocInputTuple(hash_t hash, u32 tuple_size);

  /// Fully construct the join hash table. If the join hash table has already
  /// been built, do nothing.
  void Build();

  /// The iterator used for lookups
  class Iterator {
   public:
    Iterator(const JoinHashTable &table, hash_t hash);

    using KeyEq = bool(void *opaque_ctx, void *probe_tuple, void *table_tuple);
    Entry *NextMatch(KeyEq key_eq, void *opaque_ctx, void *probe_tuple);

   private:
    Entry *next() const { return next_; }

    hash_t hash() const { return hash_; }

   private:
    // The next element the iterator produces
    Entry *next_;
    // The hash value we're looking up
    hash_t hash_;
  };

  /// Lookup all entries in this hash table with the given hash value, returning
  /// an iterator.
  Iterator Lookup(hash_t hash) const;

  /// Return the total number of inserted elements, including duplicates
  u32 num_elems() const { return num_elems_; }

  /// Has the join hash table been built?
  bool is_table_built() const { return built_; }

 private:
  friend class tpl::sql::test::JoinHashTableTest;

  // -------------------------------------------------------
  // Accessors
  // -------------------------------------------------------

  util::Region *region() const { return region_; }

  GenericHashTable *generic_hash_table() { return &generic_table_; }
  const GenericHashTable *generic_hash_table() const { return &generic_table_; }

  BloomFilter *bloom_filter() { return &filter_; }

  Entry *head() { return &head_; }

 private:
  // The memory allocator we use for all entries stored in the hash table
  util::Region *region_;

  // The generic hash table
  GenericHashTable generic_table_;

  // The bloom filter
  BloomFilter filter_;

  // The head of the lazy insertion list
  Entry head_;

  // The number of elements inserted
  u32 num_elems_;

  // Has the hash table been built?
  bool built_;
};

// ---------------------------------------------------------
// JoinHashTable implementation
// ---------------------------------------------------------

inline byte *JoinHashTable::AllocInputTuple(hash_t hash, u32 tuple_size) {
  auto *entry = new (region()->Allocate(sizeof(Entry) + tuple_size)) Entry();
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
  next_ = reinterpret_cast<Entry *>(entry);
}

inline JoinHashTable::Entry *JoinHashTable::Iterator::NextMatch(
    JoinHashTable::Iterator::KeyEq key_eq, void *opaque_ctx,
    void *probe_tuple) {
  Entry *result = next();
  while (result != nullptr) {
    next_ = reinterpret_cast<Entry *>(next()->next);
    if (result->hash == hash() &&
        key_eq(opaque_ctx, probe_tuple,
               reinterpret_cast<void *>(result->payload))) {
      break;
    }
    result = next_;
  }
  return result;
}

}  // namespace tpl::sql::runtime