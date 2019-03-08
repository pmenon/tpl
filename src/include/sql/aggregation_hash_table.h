#pragma once

#include "sql/generic_hash_table.h"
#include "util/chunked_vector.h"

namespace tpl::sql {

class AggregationHashTable {
 public:
  static constexpr const u32 kDefaultInitialTableSize = 256;

  /// Constructor
  AggregationHashTable(util::Region *region, u32 tuple_size) noexcept;

  /// This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(AggregationHashTable);

  /// Insert a new element into the table
  byte *Insert(hash_t hash) noexcept;

 private:
  // Does the hash table need to grow?
  bool NeedsToGrow() const { return hash_table_.num_elements() == max_fill_; }

  // Grow the hash table
  void Grow();

 private:
  // Where the aggregates are stored
  util::ChunkedVector entries_;

  // The hash table where the aggregates are stored
  GenericHashTable hash_table_;

  // The maximum number of elements in the table before a resize
  u64 max_fill_;
};

}  // namespace tpl::sql