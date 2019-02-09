#pragma once

#include "sql/runtime/bloom_filter.h"
#include "sql/runtime/generic_hash_table.h"
#include "util/region.h"

namespace tpl::sql::test {
class JoinHashTableTest;
}  // namespace tpl::sql::test

namespace tpl::sql::runtime {

class JoinHashTable {
 private:
  struct Entry : public GenericHashTable::EntryHeader {
    byte payload[0];
  };

 public:
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

  /// Fully construct the join hash table
  void Build();

  /// Return the number of inserted elements, including duplicates
  u32 num_elems() const { return num_elems_; }

  /// Has the join hash table been built?
  bool is_table_built() const { return built_; }

 private:
  friend class tpl::sql::test::JoinHashTableTest;

  // -------------------------------------------------------
  // Accessors
  // -------------------------------------------------------

  util::Region *region() const { return region_; }

  GenericHashTable *hash_table() { return &table_; }

  BloomFilter *bloom_filter() { return &filter_; }

  Entry *head() { return &head_; }

 private:
  // The memory allocator we use for all entries stored in the hash table
  util::Region *region_;

  // The hash table
  GenericHashTable table_;

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
// Implementation below
// ---------------------------------------------------------

inline byte *JoinHashTable::AllocInputTuple(hash_t hash, u32 tuple_size) {
  auto *entry = new (region()->Allocate(sizeof(Entry) + tuple_size)) Entry();
  entry->hash = hash;
  entry->next = head()->next;
  head()->next = entry;

  num_elems_++;

  return entry->payload;
}

}  // namespace tpl::sql::runtime