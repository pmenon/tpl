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
  vector_projection_->InitializeEmpty(column_info);

  // Next, allocate an array for each component of the aggregation.
  const auto num_elems = kDefaultVectorSize;
  for (const auto *col_info : column_info) {
    auto size = col_info->GetStorageSize() * num_elems;
    auto null_bitmap_size =
        sizeof(u32) * util::BitUtil::Num32BitWordsFor(num_elems);
    auto *data = static_cast<byte *>(memory_->Allocate(size, true));
    auto *nulls = static_cast<u32 *>(memory_->Allocate(null_bitmap_size, true));
    projection_data_.emplace_back(std::make_pair(data, nulls));
  }

  // Build the projection using data from the hash table iterator batch.
  BuildVectorProjection(transpose_fn);
}

AHTVectorIterator::AHTVectorIterator(
    const AggregationHashTable &aht, const Schema::ColumnInfo *col_infos,
    const u32 num_cols, const AHTVectorIterator::TransposeFn transpose_fn)
    : AHTVectorIterator(aht, {col_infos, col_infos + num_cols}, transpose_fn) {}

AHTVectorIterator::~AHTVectorIterator() {
  //
  // Deallocate vector data
  //

  for (u32 i = 0; i < projection_data_.size(); i++) {
    const auto col_info = vector_projection_->GetColumnInfo(i);
    auto size = col_info->GetStorageSize() * kDefaultVectorSize;
    auto null_bitmap_size =
        sizeof(u32) * util::BitUtil::Num32BitWordsFor(kDefaultVectorSize);
    memory_->Deallocate(projection_data_[i].first, size);
    memory_->Deallocate(projection_data_[i].second, null_bitmap_size);
  }

  //
  // Release the temporary aggregate vector
  //

  memory_->DeallocateArray(temp_aggregates_vec_, kDefaultVectorSize);
}

void AHTVectorIterator::BuildVectorProjection(
    const AHTVectorIterator::TransposeFn transpose_fn) {
  //
  // We have an input batch of hash table entries. For each such entry, we pull
  // out the payload pointer into a temporary array in a vectorized fashion.
  // This vector is fed into the transposition function.
  //

  auto [size, entries] = iter_.GetCurrentBatch();

  for (u32 i = 0; i < size; i++) {
    temp_aggregates_vec_[i] = entries[i]->payload;
  }

  //
  // Update the vector projection with the new batch size.
  //

  u32 idx = 0;
  for (auto &[col_data, col_null_bitmap] : projection_data_) {
    vector_projection_->ResetColumn(col_data, col_null_bitmap, idx++, size);
  }

  vector_projection_iterator_->SetVectorProjection(vector_projection_.get());

  //
  // Invoke the transposition function which does the heavy, query-specific,
  // lifting of converting rows to columns.
  //

  transpose_fn(temp_aggregates_vec_, size, vector_projection_iterator_.get());

  //
  // The vector projection is now filled with aggregate data. Reset the VPI
  // so that it's ready for iteration.
  //

  TPL_ASSERT(!vector_projection_iterator_->IsFiltered(),
             "VPI shouldn't be filtered during a transpose");
  vector_projection_iterator_->Reset();
}

}  // namespace tpl::sql
