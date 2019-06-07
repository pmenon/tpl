#include "sql/aggregation_hash_table.h"

#include "sql/vector_projection.h"
#include "sql/vector_projection_iterator.h"

namespace tpl::sql {

AHTVectorIterator::AHTVectorIterator(
    const AggregationHashTable &agg_hash_table,
    const std::vector<const Schema::ColumnInfo *> &col_infos,
    const AHTVectorIterator::TransposeFn transpose_fn)
    : memory_(agg_hash_table.memory_),
      iter_(agg_hash_table.hash_table_, memory_),
      vector_projection_(
          std::make_unique<VectorProjection>(col_infos, kDefaultVectorSize)),
      vector_projection_iterator_(std::make_unique<VectorProjectionIterator>()),
      temp_aggregates_vec_(memory_->AllocateArray<const byte *>(
          kDefaultVectorSize, CACHELINE_SIZE, false)) {
  // Allocate arrays for the transposed aggregate data
  const auto num_elems = kDefaultVectorSize;
  for (const auto *col_info : col_infos) {
    auto size = col_info->StorageSize() * num_elems;
    auto null_bitmap_size =
        sizeof(u32) * util::BitUtil::Num32BitWordsFor(num_elems);
    auto *data = static_cast<byte *>(memory_->Allocate(size, true));
    auto *nulls = static_cast<u32 *>(memory_->Allocate(null_bitmap_size, true));

    // Track
    projection_data_.emplace_back(std::make_pair(data, nulls));
  }

  // The hash table iterator has been setup
  BuildVectorProjection(transpose_fn);
}

AHTVectorIterator::AHTVectorIterator(
    const AggregationHashTable &aht, const Schema::ColumnInfo *col_infos,
    const u32 num_cols, const AHTVectorIterator::TransposeFn transpose_fn)
    : AHTVectorIterator(aht, {col_infos, col_infos + num_cols}, transpose_fn) {}

AHTVectorIterator::~AHTVectorIterator() {
  // Deallocate vector data
  for (u32 i = 0; i < projection_data_.size(); i++) {
    const auto col_info = vector_projection_->GetColumnInfo(i);
    auto size = col_info->StorageSize() * kDefaultVectorSize;
    auto null_bitmap_size =
        sizeof(u32) * util::BitUtil::Num32BitWordsFor(kDefaultVectorSize);
    memory_->Deallocate(projection_data_[i].first, size);
    memory_->Deallocate(projection_data_[i].second, null_bitmap_size);
  }

  // Release the temporary aggregate vector
  memory_->DeallocateArray(temp_aggregates_vec_, kDefaultVectorSize);
}

void AHTVectorIterator::BuildVectorProjection(
    const AHTVectorIterator::TransposeFn transpose_fn) {
  // Collect the aggregate pointers
  auto [size, entries] = iter_.GetCurrentBatch();

  // Pull out the payloads
  for (u32 i = 0; i < size; i++) {
    temp_aggregates_vec_[i] = entries[i]->payload;
  }

  // Setup the projection
  u32 idx = 0;
  for (auto &[col_data, col_null_bitmap] : projection_data_) {
    vector_projection_->ResetFromRaw(col_data, col_null_bitmap, idx++, size);
  }

  // Setup the projection iterator
  vector_projection_iterator_->SetVectorProjection(vector_projection_.get());

  // Perform rows-to-columns transpose
  transpose_fn(temp_aggregates_vec_, size, vector_projection_iterator_.get());
}

}  // namespace tpl::sql
