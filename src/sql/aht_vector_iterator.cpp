#include "sql/aggregation_hash_table.h"

#include <memory>
#include <utility>
#include <vector>

#include "sql/vector_projection.h"
#include "sql/vector_projection_iterator.h"

namespace tpl::sql {

AHTVectorIterator::AHTVectorIterator(
    const AggregationHashTable &agg_hash_table,
    const std::vector<const Schema::ColumnInfo *> &column_info,
    const AHTVectorIterator::TransposeFn transpose_fn)
    : memory_(agg_hash_table.memory_),
      iter_(agg_hash_table.hash_table_, memory_),
      vector_projection_(std::make_unique<VectorProjection>()),
      vector_projection_iterator_(std::make_unique<VectorProjectionIterator>()),
      temp_aggregates_vec_(memory_->AllocateArray<const byte *>(
          kDefaultVectorSize, CACHELINE_SIZE, false)) {
  // First, initialize the vector projection.
  vector_projection_->Initialize(column_info);

  // Next, since we have a batch of hash table input, build the projection using
  // data from the hash table iterator batch.
  BuildVectorProjection(transpose_fn);
}

AHTVectorIterator::AHTVectorIterator(
    const AggregationHashTable &agg_hash_table,
    const Schema::ColumnInfo *column_info, const u32 num_cols,
    const AHTVectorIterator::TransposeFn transpose_fn)
    : AHTVectorIterator(agg_hash_table, {column_info, column_info + num_cols},
                        transpose_fn) {}

AHTVectorIterator::~AHTVectorIterator() {
  memory_->DeallocateArray(temp_aggregates_vec_, kDefaultVectorSize);
}

void AHTVectorIterator::BuildVectorProjection(
    const AHTVectorIterator::TransposeFn transpose_fn) {
  // Pull out payload pointers from hash table entries into our temporary array.
  auto [size, entries] = iter_.GetCurrentBatch();
  for (u32 i = 0; i < size; i++) {
    temp_aggregates_vec_[i] = entries[i]->payload;
  }

  // Update the vector projection with the new batch size.
  vector_projection_->SetTupleCount(size);
  vector_projection_iterator_->SetVectorProjection(vector_projection_.get());

  // Invoke the transposition function which does the heavy, query-specific,
  // lifting of converting rows to columns.
  transpose_fn(temp_aggregates_vec_, size, vector_projection_iterator_.get());

  // The vector projection is now filled with aggregate data. Reset the VPI
  // so that it's ready for iteration.
  TPL_ASSERT(!vector_projection_iterator_->IsFiltered(),
             "VPI shouldn't be filtered during a transpose");
  vector_projection_iterator_->Reset();

  // Sanity check
  vector_projection_->CheckIntegrity();
}

void AHTVectorIterator::Next(AHTVectorIterator::TransposeFn transpose_fn) {
  TPL_ASSERT(HasNext(), "Iterator does not have more data");
  iter_.Next();
  BuildVectorProjection(transpose_fn);
}

}  // namespace tpl::sql
