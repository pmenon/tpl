#include "sql/join_hash_table_vector_probe.h"

#include "sql/join_hash_table.h"

namespace tpl::sql {

JoinHashTableVectorProbe::JoinHashTableVectorProbe(const JoinHashTable &table)
    : join_hash_table_(table), match_idx_(0), hashes_{0}, entries_{nullptr} {}

void JoinHashTableVectorProbe::Prepare(VectorProjectionIterator *vpi, const HashFn hash_fn) {
  TPL_ASSERT(vpi->GetTupleCount() <= kDefaultVectorSize,
             "VectorProjection size must be less than kDefaultVectorSize");
  // Set up
  match_idx_ = 0;

  // Compute the hashes
  if (vpi->IsFiltered()) {
    for (u32 idx = 0; vpi->HasNextFiltered(); vpi->AdvanceFiltered()) {
      hashes_[idx++] = hash_fn(vpi);
    }
  } else {
    for (u32 idx = 0; vpi->HasNext(); vpi->Advance()) {
      hashes_[idx++] = hash_fn(vpi);
    }
  }

  // Reset the iterator since we just exhausted it from the previous hash
  // computation loop.
  vpi->Reset();

  // If the join hash table uses a bloom filter, use it to early filter.
  if (join_hash_table_.HasBloomFilter()) {
    const BloomFilter *const bloom_filter = join_hash_table_.bloom_filter();
    if (vpi->IsFiltered()) {
      for (u32 idx = 0; vpi->HasNextFiltered(); vpi->AdvanceFiltered()) {
        vpi->Match(bloom_filter->Contains(hashes_[idx++]));
      }
    } else {
      for (u32 idx = 0; vpi->HasNext(); vpi->Advance()) {
        vpi->Match(bloom_filter->Contains(hashes_[idx++]));
      }
    }

    // Reset
    vpi->ResetFiltered();
  }

  // Perform the initial lookup
  join_hash_table_.LookupBatch(vpi->GetTupleCount(), hashes_, entries_);
}

}  // namespace tpl::sql
